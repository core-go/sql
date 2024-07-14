package batch

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"

	q "github.com/core-go/sql"
)

type BatchUpdater[T any] struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(*T)
	BoolSupport  bool
	VersionIndex int
	Schema       *q.Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewBatchUpdater[T any](db *sql.DB, tableName string, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, -1, mp, nil)
}
func NewBatchUpdaterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, -1, mp, toArray)
}
func NewBatchUpdaterWithVersion[T any](db *sql.DB, tableName string, versionIndex int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(*T)) *BatchUpdater[T] {
	var mp func(*T)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchUpdater[T](db, tableName, versionIndex, mp, toArray)
}
func NewSqlBatchUpdater[T any](db *sql.DB, tableName string, versionIndex int, mp func(*T), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchUpdater[T] {
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
	if len(schema.Keys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	return &BatchUpdater[T]{db: db, tableName: tableName, Schema: schema, BoolSupport: boolSupport, VersionIndex: versionIndex, Map: mp, BuildParam: buildParam, ToArray: toArray}
}
func (w *BatchUpdater[T]) Write(ctx context.Context, models []T) error {
	l := len(models)
	if l == 0 {
		return nil
	}
	if w.Map != nil {
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	var queryArgsArray []q.Statement
	for _, v := range models {
		query, args := q.BuildToUpdateWithArray(w.tableName, v, w.BuildParam, w.BoolSupport, w.ToArray, w.Schema)
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
