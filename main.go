package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"op-log-parser/parser"
	"op-log-parser/reader"
	"op-log-parser/writer"
	"os"
	"os/signal"
	"syscall"

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal")
		cancel()
	}()

	p := parser.CreateParser(func() string { return uuid.New().String() })

	// get reader
	var r reader.Reader
	var err error
	switch *inputType {
	case "file":
		r, err = reader.NewReader(reader.Config{FilePath: *inputFile})
	case "mongo":
		r, err = reader.NewReader(reader.Config{MongoURI: *mongoURI})
	default:
		fmt.Printf("Invalid input-type: %s\n", *inputType)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("Failed to create reader: %v\n", err)
		os.Exit(1)
	}
	defer r.Close()

	// get writer
	var w writer.Writer
	switch *outputType {
	case "file":
		w, err = writer.NewWriter(writer.Config{FilePath: *outputFile})
	case "postgres":
		w, err = writer.NewWriter(writer.Config{PostgresURI: *postgresURI})
	default:
		fmt.Printf("Invalid output-type: %s\n", *outputType)
		os.Exit(1)
	}
	if err != nil {
		fmt.Printf("Failed to create writer: %v\n", err)
		os.Exit(1)
	}
	defer w.Close()

	oplogChan, errChan := r.Read(ctx)
	fmt.Println("FROM FILE: ", oplogChan)

	processedChan := make(chan []string)
	go func() {
		defer close(processedChan)
		for oplog := range oplogChan {
			select {
			case <-ctx.Done():
				return
			default:
				processed, err := p.ProcessOpLog(oplog)
				if err != nil {
					log.Printf("Error processing oplog: %v\n", err)
					continue
				}
				processedChan <- processed
			}
		}
	}()

	writeErrChan := w.Write(ctx, processedChan)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case err, ok := <-errChan:
				if !ok {
					errChan = nil
				} else if err != nil {
					log.Printf("Reader error: %v\n", err)
				}
			case err, ok := <-writeErrChan:
				if !ok {
					writeErrChan = nil
				} else if err != nil {
					log.Printf("Writer error: %v\n", err)
				}
			case <-ctx.Done():
				return
			}

			if errChan == nil && writeErrChan == nil {
				return
			}
		}
	}()

	select {
	case <-done:
		log.Println("Processing completed successfully")
	case <-ctx.Done():
		log.Println("Shutting down gracefully...")
	}
}
