package batch

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type BatchInserter[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(*T)
	Driver       string
	BoolSupport  bool
	VersionIndex int
	Schema       *q.Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewBatchInserter[T any](db *sql.DB, tableName string, options ...func(*T)) *BatchInserter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter[T](db, tableName, mp, nil)
}
func NewBatchInserterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchInserter[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter[T](db, tableName, mp, toArray)
}
func NewSqlBatchInserter[T any](db *sql.DB, tableName string, mp func(*T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchInserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
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
	return &BatchInserter[T]{db: db, tableName: tableName, BuildParam: buildParam, BoolSupport: boolSupport, Schema: schema, Driver: driver, Map: mp, ToArray: toArray}
}

func (w *BatchInserter[T]) Write(ctx context.Context, models []T) error {
	l := len(models)
	if l == 0 {
		return nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	query, args, err := q.BuildToInsertBatchWithSchema(w.tableName, models, w.Driver, w.ToArray, w.BuildParam, w.Schema)
	if err != nil {
		return err
	}
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	return tx.Commit()
}
