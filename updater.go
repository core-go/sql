package sql

import (
	"context"
	"database/sql"
)

type Updater struct {
	db        *sql.DB
	tableName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpdater(db *sql.DB, tableName string, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &Updater{db: db, tableName: tableName, Map: mp}
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := Update(w.db, w.tableName, m2)
		return err
	}
	_, err := Update(w.db, w.tableName, model)
	return err
}
