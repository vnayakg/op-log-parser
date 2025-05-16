package reader

import (
	"context"
	"encoding/json"
	"log"
	"op-log-parser/parser"
	"os"
)

type FileReader struct {
	file   *os.File
	config Config
}

func NewFileReader(config Config) (Reader, error) {
	file, err := os.Open(config.FilePath)
	if err != nil {
		return nil, err
	}
	return &FileReader{
		file:   file,
		config: config,
	}, nil
}

func (r *FileReader) Read(ctx context.Context) (<-chan parser.OpLog, <-chan error) {
	oplogChan := make(chan parser.OpLog)
	errChan := make(chan error, 1)

	go func() {
		defer close(oplogChan)
		defer close(errChan)
		defer r.file.Close()

		decoder := json.NewDecoder(r.file)
		if _, err := decoder.Token(); err != nil {
			errChan <- err
		}

		for decoder.More() {
			var opLog parser.OpLog
			if err := decoder.Decode(&opLog); err != nil {
				errChan <- err
			}
			log.Println("opLOG", opLog)
			oplogChan <- opLog
		}
	}()

	return oplogChan, errChan
}

func (r *FileReader) Close() error {
	return r.file.Close()
}
