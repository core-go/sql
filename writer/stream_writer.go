package writer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type StreamWriter[T any] struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	// BoolSupport  bool
	// VersionIndex int
	schema    *q.Schema
	batchSize int
	batch     []interface{}
	Driver    string
	ToArray   func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewStreamWriter[T any](db *sql.DB, tableName string, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewSqlStreamWriter[T](db, tableName, batchSize, mp, nil)
}
func NewStreamWriterWithArray[T any](db *sql.DB, tableName string, batchSize int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter[T] {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlStreamWriter[T](db, tableName, batchSize, mp, toArray)
}
func NewSqlStreamWriter[T any](db *sql.DB, tableName string, batchSize int,
	mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}, options ...func(i int) string) *StreamWriter[T] {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	// boolSupport := driver == DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := q.CreateSchema(modelType)
	return &StreamWriter[T]{db: db, Driver: driver, schema: schema, tableName: tableName, batchSize: batchSize, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *StreamWriter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2)
	} else {
		w.batch = append(w.batch, model)
	}
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamWriter[T]) Flush(ctx context.Context) error {
	var queryArgsArray []q.Statement
	for _, v := range w.batch {
		query, args, err := q.BuildToSaveWithArray(w.tableName, v, w.Driver, w.ToArray, w.schema)
		if err != nil {
			return err
		}
		queryArgs := q.Statement{
			Query:  query,
			Params: args,
		}
		queryArgsArray = append(queryArgsArray, queryArgs)
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer func() {
		w.batch = make([]interface{}, 0)
	}()

	for _, v := range queryArgsArray {
		_, err = tx.Exec(v.Query, v.Params...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}

	return nil
}
