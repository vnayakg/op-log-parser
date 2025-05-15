package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"op-log-parser/file"
	"op-log-parser/mongo"
	"op-log-parser/parser"
	"op-log-parser/postgres"
	"os"

	"github.com/google/uuid"
)

func main() {
	inputType := flag.String("input-type", "file", "Input source: file or mongo")
	inputFile := flag.String("input-file", "example-input.json", "Input JSON file containing oplogs")
	outputFile := flag.String("output-file", "output.sql", "Output SQL file")
	mongoURI := flag.String("mongo-uri", "mongodb://localhost:27017", "MongoDB URI (for mongo input)")
	outputType := flag.String("output-type", "file", "Output destination: file or postgres")
	postgresURI := flag.String("postgres-uri", "postgres://user:pass@localhost:5432", "PostgreSQL URI (for postgres output)")
	flag.Parse()

	p := parser.CreateParser(func() string { return uuid.New().String() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var err error
	switch *inputType {
	case "file":
		switch *outputType {
		case "file":
			err = file.ParseFromFile(p, *inputFile, *outputFile)
		case "postgres":
			pg, pgErr := postgres.NewExecutor(*postgresURI)
			if pgErr != nil {
				fmt.Printf("PostgreSQL connection failed: %v\n", pgErr)
				os.Exit(1)
			}
			defer pg.Close()
			err = file.ParseFromFileToPostgres(ctx, p, *inputFile, pg)
		default:
			fmt.Printf("Invalid output-type: %s\n", *outputType)
			os.Exit(1)
		}
	case "mongo":
		log.Println("connecting to mongo")
		mongoClient, mongoErr := mongo.NewClient(ctx, *mongoURI)
		if mongoErr != nil {
			fmt.Printf("MongoDB connection failed: %v\n", mongoErr)
			os.Exit(1)
		}
		defer mongoClient.Close(ctx)
		switch *outputType {
		case "file":
			err = mongo.StreamOplogsToFile(ctx, mongoClient, p, *outputFile)
		case "postgres":
			pg, pgErr := postgres.NewExecutor(*postgresURI)
			if pgErr != nil {
				fmt.Printf("PostgreSQL connection failed: %v\n", pgErr)
				os.Exit(1)
			}
			defer pg.Close()
			err = mongo.StreamOplogsToPostgres(ctx, mongoClient, p, pg)
		default:
			fmt.Printf("Invalid output-type: %s\n", *outputType)
			os.Exit(1)
		}
	default:
		fmt.Printf("Invalid input-type: %s\n", *inputType)
		os.Exit(1)
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
