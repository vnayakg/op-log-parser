package file

import (
	"context"
	"encoding/json"
	"fmt"
	"op-log-parser/parser"
	"op-log-parser/postgres"
	"os"
	"strings"
)

func ParseFromFile(p parser.Parser, inputFile, outputFile string) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("reading JSON array start: %w", err)
	}

	var sqlOutput strings.Builder
	for decoder.More() {

		var opLog parser.OpLog
		if err := decoder.Decode(&opLog); err != nil {
			return fmt.Errorf("decoding oplog: %w", err)
		}

		statements, err := p.ProcessOpLog(opLog)
		if err != nil {
			return fmt.Errorf("processing oplog: %w", err)
		}

		for _, stmt := range statements {
			sqlOutput.WriteString(stmt + "\n")
		}
	}

	if err := os.WriteFile(outputFile, []byte(sqlOutput.String()), 0644); err != nil {
		return fmt.Errorf("writing output file: %w", err)
	}
	return nil
}

func ParseFromFileToPostgres(ctx context.Context, p parser.Parser, inputFile string, pg *postgres.Executor) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("opening input file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("reading JSON array start: %w", err)
	}

	for decoder.More() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var opLog parser.OpLog
		if err := decoder.Decode(&opLog); err != nil {
			return fmt.Errorf("decoding oplog: %w", err)
		}

		statements, err := p.ProcessOpLog(opLog)
		if err != nil {
			return fmt.Errorf("processing oplog: %w", err)
		}

		for _, stmt := range statements {
			if err := pg.Execute(ctx, stmt); err != nil {
				return fmt.Errorf("executing SQL: %w", err)
			}
		}
	}

	return nil
}
