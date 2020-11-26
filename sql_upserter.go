package sql

import (
	"context"
	"database/sql"
)

type SqlUpserter struct {
	db        *sql.DB
	tableName string
}

func NewSqlUpserter(database *sql.DB, tableName string) *SqlUpserter {
	return &SqlUpserter{database, tableName}
}

func (w *SqlUpserter) Write(ctx context.Context, models interface{}) error {
	_, err := Upsert(w.db, w.tableName, models)
	return err
}
