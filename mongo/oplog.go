package mongo

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"op-log-parser/parser"
	"op-log-parser/postgres"
)

type Client struct {
	client *mongo.Client
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetDirect(true))
	if err != nil {
		return nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}
	log.Println("connected to mongo")
	return &Client{client: client}, nil
}

func (c *Client) Close(ctx context.Context) error {
	return c.client.Disconnect(ctx)
}

func (c *Client) Database(name string) *mongo.Database {
	return c.client.Database(name)
}

func StreamOplogsToFile(ctx context.Context, client *Client, p parser.Parser, outputFile string) error {
	file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("opening output file: %w", err)
	}
	defer file.Close()

	return streamOplogs(ctx, client, p, func(stmt string) error {
		sqlLine := stmt + "\n"
		if _, err := file.WriteString(sqlLine); err != nil {
			return fmt.Errorf("writing to file: %w", err)
		}
		return file.Sync()
	})
}

// StreamOplogsToPostgres streams MongoDB oplogs to PostgreSQL
func StreamOplogsToPostgres(ctx context.Context, client *Client, p parser.Parser, pg *postgres.Executor) error {
	return streamOplogs(ctx, client, p, func(stmt string) error {
		return pg.Execute(ctx, stmt)
	})
}

// processOplogEntry processes a single oplog entry and returns any error encountered
func processOplogEntry(ctx context.Context, raw bson.M, p parser.Parser, processStmt func(string) error) error {
	// Skip system operations
	if ns, ok := raw["ns"].(string); ok && ns == "local.oplog.rs" {
		return nil
	}

	opLog, err := convertToOpLog(raw)
	if err != nil {
		return fmt.Errorf("converting oplog: %w", err)
	}

	log.Printf("Processing oplog: %s on namespace %s", opLog.Operation, opLog.Namespace)
	statements, err := p.ProcessOpLog(opLog)
	if err != nil {
		return fmt.Errorf("processing oplog: %w", err)
	}

	for _, stmt := range statements {
		if err := processStmt(stmt); err != nil {
			return fmt.Errorf("processing statement: %w", err)
		}
		log.Printf("Successfully executed statement: %s", stmt)
	}
	return nil
}

// streamOplogs handles the core oplog streaming logic
var streamOplogs = func(ctx context.Context, client *Client, p parser.Parser, processStmt func(string) error) error {
	collection := client.client.Database("local").Collection("oplog.rs")
	log.Println("Starting oplog streaming...")

	// Get the latest oplog timestamp
	var lastTimestamp bson.M
	err := collection.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.M{"$natural": -1})).Decode(&lastTimestamp)
	if err != nil {
		return fmt.Errorf("getting latest oplog timestamp: %w", err)
	}

	// Process existing oplog entries
	if err := processExistingOplogs(ctx, collection, p, processStmt); err != nil {
		return fmt.Errorf("processing existing oplogs: %w", err)
	}

	// Stream new oplog entries starting from the last processed timestamp
	return streamNewOplogs(ctx, collection, p, processStmt, lastTimestamp["ts"])
}

// processExistingOplogs processes all existing oplog entries
func processExistingOplogs(ctx context.Context, collection *mongo.Collection, p parser.Parser, processStmt func(string) error) error {
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().
		SetSort(bson.M{"$natural": 1}))
	if err != nil {
		return fmt.Errorf("creating initial cursor: %w", err)
	}
	defer cursor.Close(ctx)
	log.Println("Initial cursor created, processing existing oplog entries...")

	for cursor.Next(ctx) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			var raw bson.M
			if err := cursor.Decode(&raw); err != nil {
				log.Printf("Error decoding oplog: %v", err)
				continue
			}

			if err := processOplogEntry(ctx, raw, p, processStmt); err != nil {
				log.Printf("Error processing oplog entry: %v", err)
				continue
			}
		}
	}

	return cursor.Err()
}

func streamNewOplogs(ctx context.Context, collection *mongo.Collection, p parser.Parser, processStmt func(string) error, lastTimestamp interface{}) error {
	log.Println("Creating tailable cursor for new oplog entries...")

	// Create a filter to only get oplog entries after the last processed timestamp
	filter := bson.M{
		"ts": bson.M{"$gt": lastTimestamp},
	}

	tailableCursor, err := collection.Find(ctx, filter, options.Find().
		SetCursorType(options.TailableAwait).
		SetMaxAwaitTime(time.Second).
		SetNoCursorTimeout(true).
		SetSort(bson.M{"$natural": 1}))
	if err != nil {
		return fmt.Errorf("creating tailable cursor: %w", err)
	}
	defer tailableCursor.Close(ctx)
	log.Println("Tailable cursor created, waiting for new oplog entries...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if !tailableCursor.Next(ctx) {
				if tailableCursor.Err() != nil {
					return fmt.Errorf("tailable cursor error: %w", tailableCursor.Err())
				}
				continue
			}

			var raw bson.M
			if err := tailableCursor.Decode(&raw); err != nil {
				log.Printf("Error decoding oplog: %v", err)
				continue
			}

			if err := processOplogEntry(ctx, raw, p, processStmt); err != nil {
				log.Printf("Error processing oplog entry: %v", err)
				continue
			}
		}
	}
}

func convertToOpLog(raw bson.M) (parser.OpLog, error) {
	op, ok := raw["op"].(string)
	if !ok {
		return parser.OpLog{}, fmt.Errorf("invalid op field")
	}
	ns, ok := raw["ns"].(string)
	if !ok {
		return parser.OpLog{}, fmt.Errorf("invalid ns field")
	}

	dataRaw, ok := raw["o"]
	if !ok {
		return parser.OpLog{}, fmt.Errorf("invalid o field")
	}
	dataBytes, err := bson.Marshal(dataRaw)
	if err != nil {
		return parser.OpLog{}, fmt.Errorf("marshaling o field: %w", err)
	}
	var data map[string]any
	if err := bson.Unmarshal(dataBytes, &data); err != nil {
		return parser.OpLog{}, fmt.Errorf("unmarshaling o field: %w", err)
	}

	// Only process o2 field for update operations
	var o2Data *parser.O2Field
	if op == "u" {
		rawO2, ok := raw["o2"]
		if !ok {
			return parser.OpLog{}, fmt.Errorf("invalid o2 field for update operation")
		}
		o2Bytes, err := bson.Marshal(rawO2)
		if err != nil {
			return parser.OpLog{}, fmt.Errorf("marshaling o2 field: %w", err)
		}
		var o2 parser.O2Field
		if err := bson.Unmarshal(o2Bytes, &o2); err != nil {
			return parser.OpLog{}, fmt.Errorf("unmarshaling o2 field: %w", err)
		}
		o2Data = &o2
	}

	return parser.OpLog{
		Operation: op,
		Namespace: ns,
		Data:      data,
		O2:        o2Data,
	}, nil
}
