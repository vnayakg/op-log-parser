package mongo

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"op-log-parser/application/ports"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoReader struct {
	client *mongo.Client
	config ports.ReaderConfig
}

func NewReader(config ports.ReaderConfig) (ports.Reader, error) {
	if config.MongoURI == "" {
		return nil, fmt.Errorf("MongoURI is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.MongoURI).SetDirect(true))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MongoDB: %v", err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ping MongoDB: %v", err)
	}

	log.Println("Connected to MongoDB")
	return &MongoReader{
		client: client,
		config: config,
	}, nil
}

func (r *MongoReader) Read(ctx context.Context) (<-chan string, <-chan error) {
	oplogChan := make(chan string)
	errChan := make(chan error, 1)

	go func() {
		defer close(oplogChan)
		defer close(errChan)

		collection := r.client.Database("local").Collection("oplog.rs")
		log.Println("Starting oplog streaming...")

		var lastTimestamp bson.M
		err := collection.FindOne(ctx, bson.M{}, options.FindOne().SetSort(bson.M{"$natural": -1})).Decode(&lastTimestamp)
		if err != nil {
			log.Printf("Error getting latest oplog timestamp: %v", err)
			errChan <- fmt.Errorf("getting latest oplog timestamp: %v", err)
			return
		}
		log.Printf("Latest oplog timestamp: %v", lastTimestamp)

		if err := r.processExistingOplogs(ctx, collection, oplogChan); err != nil {
			log.Printf("Error processing existing oplogs: %v", err)
			errChan <- fmt.Errorf("processing existing oplogs: %v", err)
			return
		}

		if err := r.streamNewOplogs(ctx, collection, oplogChan, lastTimestamp["ts"]); err != nil {
			log.Printf("Error streaming new oplogs: %v", err)
			errChan <- fmt.Errorf("streaming new oplogs: %v", err)
			return
		}
	}()

	return oplogChan, errChan
}

func (r *MongoReader) processExistingOplogs(ctx context.Context, collection *mongo.Collection, oplogChan chan<- string) error {
	cursor, err := collection.Find(ctx, bson.M{}, options.Find().
		SetSort(bson.M{"$natural": 1}))
	if err != nil {
		return fmt.Errorf("creating initial cursor: %v", err)
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

			if ns, ok := raw["ns"].(string); ok && ns == "local.oplog.rs" {
				continue
			}

			// Wrap the oplog entry in an array
			oplogArray := []bson.M{raw}
			dataBytes, _ := json.Marshal(oplogArray)
			select {
			case oplogChan <- string(dataBytes):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return cursor.Err()
}

func (r *MongoReader) streamNewOplogs(ctx context.Context, collection *mongo.Collection, oplogChan chan<- string, lastTimestamp interface{}) error {
	log.Println("Creating tailable cursor for new oplog entries...")

	filter := bson.M{
		"ts": bson.M{"$gt": lastTimestamp},
	}

	tailableCursor, err := collection.Find(ctx, filter, options.Find().
		SetCursorType(options.TailableAwait).
		SetMaxAwaitTime(time.Second).
		SetNoCursorTimeout(true).
		SetSort(bson.M{"$natural": 1}))
	if err != nil {
		return fmt.Errorf("creating tailable cursor: %v", err)
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
					return fmt.Errorf("tailable cursor error: %v", tailableCursor.Err())
				}
				continue
			}

			var raw bson.M
			if err := tailableCursor.Decode(&raw); err != nil {
				log.Printf("Error decoding oplog: %v", err)
				continue
			}

			if ns, ok := raw["ns"].(string); ok && ns == "local.oplog.rs" {
				continue
			}

			// Wrap the oplog entry in an array
			oplogArray := []bson.M{raw}
			dataBytes, _ := json.Marshal(oplogArray)
			select {
			case oplogChan <- string(dataBytes):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (r *MongoReader) Close() error {
	if r.client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return r.client.Disconnect(ctx)
}
