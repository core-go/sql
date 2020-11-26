package sql

import (
	"context"
	"database/sql"
)

type SqlInserter struct {
	db        *sql.DB
	tableName string
}

func NewSqlInserter(database *sql.DB, tableName string) *SqlInserter {
	return &SqlInserter{database, tableName}
}

func (w *SqlInserter) Write(ctx context.Context, models interface{}) error {
	_, err := Insert(w.db, w.tableName, models)
	return err
}
