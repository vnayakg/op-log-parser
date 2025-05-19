package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"op-log-parser/application/parsers"
	"op-log-parser/application/persistence/file"
	"op-log-parser/application/persistence/mongo"
	"op-log-parser/application/persistence/postgres"
	"op-log-parser/application/ports"
	"op-log-parser/application/services"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Create parser
	parser := parsers.NewParser(func() string { return uuid.New().String() })

	// Create reader
	var reader ports.Reader
	var err error
	switch *inputType {
	case "file":
		reader, err = file.NewReader(ports.ReaderConfig{FilePath: *inputFile})
	case "mongo":
		reader, err = mongo.NewReader(ports.ReaderConfig{MongoURI: *mongoURI})
	default:
		fmt.Printf("Invalid input-type: %s\n", *inputType)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("Failed to create reader: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Create writer
	var writer ports.Writer
	switch *outputType {
	case "file":
		writer, err = file.NewWriter(ports.WriterConfig{FilePath: *outputFile})
	case "postgres":
		writer, err = postgres.NewWriter(ports.WriterConfig{PostgresURI: *postgresURI})
	default:
		fmt.Printf("Invalid output-type: %s\n", *outputType)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("Failed to create writer: %v\n", err)
		os.Exit(1)
	}
	defer writer.Close()

	// Create and run processor
	processor := services.NewOpLogProcessor(reader, writer, parser)
	if err := processor.Process(ctx); err != nil {
		log.Printf("Processing error: %v\n", err)
		os.Exit(1)
	}

	log.Println("Processing completed successfully")
	time.Sleep(5 * time.Second)
}
