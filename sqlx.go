package sqlx

import (
	"context"
	"database/sql"
)

type DB struct {
	conn *sql.DB
}

func New(driver, source string) (*DB, error) {
	db, err := sql.Open(driver, source)
	if err != nil {
		return nil, err
	}
	return &DB{
		conn: db,
	}, nil
}

func MustNew(driver, source string) *DB {
	db, err := New(driver, source)
	if err != nil {
		panic(err)
	}
	return db
}

func NewUseDb(db *sql.DB) *DB {
	return &DB{db}
}

func (db *DB) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)
	err = unmarshalRow(dest, rows, false)
	if err != nil {
		return err
	}
	return rows.Err()
}

func (db *DB) QueryRows(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)

	err = unmarshalRows(dest, rows, false)
	if err != nil {
		return err
	}
	return rows.Err()
}

func (db *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	//db.conn.ExecContext()
	return db.conn.ExecContext(ctx, query, args...)
}

func (db *DB) Trans(ctx context.Context, fn func(context.Context, Session) error) error {
	tx, err := db.conn.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	return fn(ctx, &Tx{conn: tx})
}

func (db *DB) Close() error {
	return db.conn.Close()
}

type Tx struct {
	conn *sql.Tx
}

func (db *Tx) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)
	err = unmarshalRow(dest, rows, false)
	if err != nil {
		return err
	}
	return rows.Err()
}

func (db *Tx) QueryRows(ctx context.Context, dest any, query string, args ...any) error {
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) { _ = rows.Close() }(rows)

	err = unmarshalRows(dest, rows, false)
	if err != nil {
		return err
	}
	return rows.Err()
}

func (db *Tx) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.conn.ExecContext(ctx, query, args...)
}
