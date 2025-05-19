package ports

import (
	"context"
)

type Writer interface {
	Write(ctx context.Context, statements <-chan []string) <-chan error

	Close() error
}

type WriterConfig struct {
	FilePath    string
	PostgresURI string
}
