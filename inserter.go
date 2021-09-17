package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type Inserter struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
func NewInserter(db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter(db, tableName, mp, toArray)
}
func NewSqlInserter(db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Inserter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return &Inserter{db: db, tableName: tableName, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}

		queryInsert, values := BuildToInsert(w.tableName, m2, w.BuildParam, w.ToArray)
		_, err := w.db.ExecContext(ctx, queryInsert, values...)
		return err
	}
	queryInsert, values := BuildToInsert(w.tableName, model, w.BuildParam, w.ToArray)
	_, err := w.db.ExecContext(ctx, queryInsert, values...)
	return err
}
