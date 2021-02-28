package sql

import (
	"context"
	"database/sql"
)

type SqlWriter struct {
	db        *sql.DB
	tableName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSqlWriter(db *sql.DB, tableName string, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *SqlWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &SqlWriter{db, tableName, mp}
}

func (w *SqlWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := Upsert(w.db, w.tableName, m2)
		return err
	}
	_, err := Upsert(w.db, w.tableName, model)
	return err
}
