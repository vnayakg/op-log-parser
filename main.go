package main

import (
	"flag"
	"fmt"
	"op-log-parser/file"
	"op-log-parser/parser"
	"os"

	"github.com/google/uuid"
)

func main() {
	inputFile := flag.String("input", "example-input.json", "Input JSON file containing oplogs")
	outputFile := flag.String("output", "output.sql", "Output SQL file")
	flag.Parse()

	p := parser.CreateParser(func() string {return uuid.New().String()})

	if err := file.ParseFromFile(p, *inputFile, *outputFile); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
