package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Executor struct {
	conn *pgx.Conn
}

func NewExecutor(uri string) (*Executor, error) {
	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		return nil, fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	return &Executor{conn: conn}, nil
}	

func (e *Executor) Close() error {
	return e.conn.Close(context.Background())
}

func (e *Executor) Execute(ctx context.Context, stmt string) error {
	_, err := e.conn.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("executing query %q: %w", stmt, err)
	}
	return nil
}
