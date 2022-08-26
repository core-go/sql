package batch

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type BatchInserter struct {
	db           *sql.DB
	tableName    string
	BuildParam   func(i int) string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
	BoolSupport  bool
	VersionIndex int
	Schema       *q.Schema
	ToArray      func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
func NewBatchInserter(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter(db, tableName, modelType, mp, nil)
}
func NewBatchInserterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return NewSqlBatchInserter(db, tableName, modelType, mp, toArray)
}
func NewSqlBatchInserter(db *sql.DB, tableName string, modelType reflect.Type, mp func(context.Context, interface{}) (interface{}, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) *BatchInserter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	boolSupport := driver == q.DriverPostgres
	schema := q.CreateSchema(modelType)
	return &BatchInserter{db: db, tableName: tableName, BuildParam: buildParam, BoolSupport: boolSupport, Schema: schema, Map: mp, ToArray: toArray}
}

func (w *BatchInserter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	var models2 interface{}
	var er0 error
	if w.Map != nil {
		models2, er0 = q.MapModels(ctx, models, w.Map)
		if er0 != nil {
			s0 := reflect.ValueOf(models2)
			_, er0b := q.InterfaceSlice(models2)
			failIndices = q.ToArrayIndex(s0, failIndices)
			return successIndices, failIndices, er0b
		}
	} else {
		models2 = models
	}
	s := reflect.ValueOf(models2)
	_, er2 := q.InsertBatchWithSchema(ctx, w.db, w.tableName, models2, w.ToArray, w.BuildParam, w.Schema)

	if er2 == nil {
		// Return full success
		successIndices = q.ToArrayIndex(s, successIndices)
		return successIndices, failIndices, er2
	} else {
		// Return full fail
		failIndices = q.ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, er2
}
