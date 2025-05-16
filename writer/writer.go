package writer

import (
	"context"
)

type Writer interface {
	Write(ctx context.Context, oplogs <-chan []string) <-chan error
	Close() error
}

type Config struct {
	FilePath    string
	PostgresURI string
}

func NewWriter(config Config) (Writer, error) {
	if config.FilePath != "" {
		return NewFileWriter(config)
	}
	if config.PostgresURI != "" {
		return NewPostgresWriter(config)
	}
	return nil, nil
}
