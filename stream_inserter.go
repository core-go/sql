package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type StreamInserter struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	schema       *Schema
	batchSize    int
	batch        []interface{}
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewStreamInserter(db *sql.DB, tableName string, modelType reflect.Type, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}

	return NewSqlStreamInserter(db, tableName, modelType, batchSize, mp, nil)
}
func NewStreamInserterWithArray(db *sql.DB, tableName string, modelType reflect.Type, batchSize int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *StreamInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlStreamInserter(db, tableName, modelType, batchSize, mp, toArray)
}
func NewSqlStreamInserter(db *sql.DB, tableName string, modelType reflect.Type, batchSize int,
	mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *StreamInserter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	return &StreamInserter{db: db, BoolSupport: boolSupport, VersionIndex: -1, schema: schema, tableName: tableName, batchSize: batchSize, BuildParam: buildParam, Map: mp, ToArray: toArray}
}

func (w *StreamInserter) Write(ctx context.Context, model interface{}) error {
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}

func (w *StreamInserter) Flush(ctx context.Context) error {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Defer a rollback in case anything fails.
	defer func() {
		w.batch = make([]interface{}, 0)
	}()
	defer tx.Rollback()
	driver := GetDriver(w.db)
	query, args, er1 := BuildToInsertBatchWithSchema(w.tableName, w.batch, driver, w.ToArray, w.BuildParam, w.schema)
	if er1 != nil {
		return er1
	}
	_, err2 := tx.ExecContext(ctx, query, args...)
	if err2 != nil {
		return err2
	}
	tx.Commit()
	return nil
}
