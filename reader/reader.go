package reader

import (
	"context"
	"io"
	"op-log-parser/parser"
)

type Reader interface {
	Read(ctx context.Context) (<-chan parser.OpLog, <-chan error)
	Close() error
}

type Config struct {
	FilePath string
	MongoURI string
}

func NewReader(config Config) (Reader, error) {
	if config.FilePath != "" {
		return NewFileReader(config)
	}
	if config.MongoURI != "" {
		return NewMongoReader(config)
	}
	return nil, io.EOF
}
