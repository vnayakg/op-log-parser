package file

import (
	"context"
	"os"

	"op-log-parser/application/ports"
)

type fileWriter struct {
	file   *os.File
	config ports.WriterConfig
}

func NewWriter(config ports.WriterConfig) (ports.Writer, error) {
	file, err := os.Create(config.FilePath)
	if err != nil {
		return nil, err
	}
	return &fileWriter{file: file, config: config}, nil
}

func (w *fileWriter) Write(ctx context.Context, statements <-chan []string) <-chan error {
	errChan := make(chan error)

	go func() {
		defer close(errChan)
		defer w.file.Close()

		for statements := range statements {
			select {
			case <-ctx.Done():
				return
			default:
				for _, stmt := range statements {
					if _, err := w.file.WriteString(stmt + ";\n"); err != nil {
						errChan <- err
						return
					}
				}
			}
		}
	}()

	return errChan
}

func (w *fileWriter) Close() error {
	return w.file.Close()
}
