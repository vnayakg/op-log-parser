package ports

import (
	"context"
)

type Reader interface {
	Read(ctx context.Context) (<-chan string, <-chan error)

	Close() error
}

type ReaderConfig struct {
	FilePath string
	MongoURI string
}
