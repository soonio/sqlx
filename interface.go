package sqlx

import (
	"context"
	"database/sql"
)

type rowsScanner interface {
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(v ...any) error
}

type Session interface {
	QueryRow(ctx context.Context, dest any, query string, args ...any) error
	QueryRows(ctx context.Context, dest any, query string, args ...any) error
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
}
