package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type SqlWriter[T any] struct {
	db          *sql.DB
	tableName   string
	BuildParam  func(i int) string
	Map         func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport bool
	schema      *q.Schema
	ToArray     func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewSqlWriterWithMap[T any](db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *SqlWriter[T] {
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
	return &SqlWriter[T]{db: db, tableName: tableName, BuildParam: buildParam, Map: mp, BoolSupport: boolSupport, schema: schema, ToArray: toArray}
}

func NewSqlWriter[T any](db *sql.DB, tableName string, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *SqlWriter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlWriterWithMap[T](db, tableName, mp, nil)
}

func (w *SqlWriter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := q.SaveWithArray(ctx, w.db, w.tableName, m2, w.ToArray, w.schema)
		return err
	}
	_, err := q.SaveWithArray(ctx, w.db, w.tableName, model, w.ToArray, w.schema)
	return err
}
