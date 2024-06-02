package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type Updater[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	schema       *q.Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewUpdater[T any](db *sql.DB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *Updater[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater[T](db, tableName, mp, nil)
}
func NewUpdaterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Updater[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlUpdater[T](db, tableName, mp, toArray)
}
func NewSqlUpdater[T any](db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Updater[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	boolSupport := driver == q.DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := q.CreateSchema(modelType)
	return &Updater[T]{db: db, tableName: tableName, VersionIndex: -1, BoolSupport: boolSupport, schema: schema, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Updater[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		query0, values0 := q.BuildToUpdateWithVersion(w.tableName, m2, w.VersionIndex, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
		_, er1 := w.db.ExecContext(ctx, query0, values0...)
		return er1
	}
	query, values := q.BuildToUpdateWithVersion(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, w.ToArray, w.schema)
	_, er2 := w.db.ExecContext(ctx, query, values...)
	return er2
}
