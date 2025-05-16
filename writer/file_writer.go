package writer

import (
	"context"
	"os"
	"strings"
)

type FileWriter struct {
	file   *os.File
	config Config
}

func NewFileWriter(config Config) (Writer, error) {
	file, err := os.Create(config.FilePath)
	if err != nil {
		return nil, err
	}
	return &FileWriter{
		file:   file,
		config: config,
	}, nil
}

func (w *FileWriter) Write(ctx context.Context, oplogs <-chan []string) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)
		defer w.file.Close()

		for oplog := range oplogs {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				if _, err := w.file.Write([]byte(strings.Join(oplog, "\n"))); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	return errChan
}

func (w *FileWriter) Close() error {
	return w.file.Close()
}
