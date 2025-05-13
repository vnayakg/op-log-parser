package file

import (
	"encoding/json"
	"fmt"
	"op-log-parser/parser"
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
