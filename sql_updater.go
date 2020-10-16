package sql

import (
	"context"
	"database/sql"
)

type SqlUpdater struct {
	db        *sql.DB
	tableName string
}

func NewSqlUpdater(database *sql.DB, tableName string) *SqlUpdater {
	return &SqlUpdater{database, tableName}
}

func (w *SqlUpdater) Write(ctx context.Context, models interface{}) error {
	_, err := UpdateOne(w.db, w.tableName, models)
	return err
}
