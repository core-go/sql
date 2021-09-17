package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type Updater struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewUpdater(db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater(db, tableName, mp, toArray)
}
func NewSqlUpdater(db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Updater {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return &Updater{db: db, tableName: tableName, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := Update(ctx, w.db, w.tableName, m2, w.ToArray, w.BuildParam)
		return err
	}
	_, err := Update(ctx, w.db, w.tableName, model, w.ToArray, w.BuildParam)
	return err
}
