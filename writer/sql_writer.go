package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type SqlWriter struct {
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

func NewSqlWriterWithMap(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *SqlWriter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	boolSupport := driver == q.DriverPostgres
	schema := q.CreateSchema(modelType)
	return &SqlWriter{db: db, tableName: tableName, BuildParam: buildParam, Map: mp, BoolSupport: boolSupport, schema: schema, ToArray: toArray}
}

func NewSqlWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *SqlWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlWriterWithMap(db, tableName, modelType, mp, nil)
}

func (w *SqlWriter) Write(ctx context.Context, model interface{}) error {
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
