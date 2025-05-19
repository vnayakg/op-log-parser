package file

import (
	"bufio"
	"context"
	"os"

	"op-log-parser/application/ports"
)

type fileReader struct {
	file   *os.File
	config ports.ReaderConfig
}

func NewReader(config ports.ReaderConfig) (ports.Reader, error) {
	file, err := os.Open(config.FilePath)
	if err != nil {
		return nil, err
	}
	return &fileReader{file: file, config: config}, nil
}

func (r *fileReader) Read(ctx context.Context) (<-chan string, <-chan error) {
	oplogChan := make(chan string)
	errChan := make(chan error)

	go func() {
		defer close(oplogChan)
		defer close(errChan)
		defer r.file.Close()

		scanner := bufio.NewScanner(r.file)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
				oplogChan <- scanner.Text()
			}
		}

		if err := scanner.Err(); err != nil {
			errChan <- err
		}
	}()

	return oplogChan, errChan
}

func (r *fileReader) Close() error {
	return r.file.Close()
}
