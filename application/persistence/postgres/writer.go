package postgres

import (
	"context"
	"database/sql"
	"op-log-parser/application/ports"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

type PostgresWriter struct {
	db     *sql.DB
	config ports.WriterConfig
}

func NewWriter(config ports.WriterConfig) (ports.Writer, error) {
	db, err := sql.Open("postgres", config.PostgresURI)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return &PostgresWriter{
		db:     db,
		config: config,
	}, nil
}

func (w *PostgresWriter) Write(ctx context.Context, oplogs <-chan []string) <-chan error {
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		for oplog := range oplogs {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				_, err := w.db.Exec(strings.Join(oplog, ""))
				if err != nil {
					errChan <- err
					return
				}
			}
		}
	}()
	return errChan
}

func (w *PostgresWriter) Close() error {
	return w.db.Close()
}
