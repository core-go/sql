package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type Inserter[T any] struct {
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

func NewInserter[T any](db *sql.DB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter[T](db, tableName, mp, nil)
}
func NewInserterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter[T](db, tableName, mp, toArray)
}
func NewSqlInserter[T any](db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *Inserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	boolSupport := driver == q.DriverPostgres
	schema := q.CreateSchema(modelType)
	return &Inserter[T]{db: db, BoolSupport: boolSupport, VersionIndex: -1, schema: schema, tableName: tableName, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *Inserter[T]) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}

		queryInsert, values := q.BuildToInsertWithSchema(w.tableName, m2, w.VersionIndex, w.BuildParam, w.BoolSupport, false, w.ToArray, w.schema)
		_, err := w.db.ExecContext(ctx, queryInsert, values...)
		return err
	}
	queryInsert, values := q.BuildToInsertWithSchema(w.tableName, model, w.VersionIndex, w.BuildParam, w.BoolSupport, false, w.ToArray, w.schema)
	_, err := w.db.ExecContext(ctx, queryInsert, values...)
	return err
}
