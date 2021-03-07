package sql

import (
	"context"
	"database/sql"
	"reflect"
)

type BatchInserter struct {
	db        *sql.DB
	tableName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchInserter(db *sql.DB, tableName string, options...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &BatchInserter{db: db, tableName: tableName, Map: mp}
}

func (w *BatchInserter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	var models2 interface{}
	var er0 error
	if w.Map != nil {
		models2, er0 = MapModels(ctx, models, w.Map)
		if er0 != nil {
			s0 := reflect.ValueOf(models2)
			_, er0b := InterfaceSlice(models2)
			failIndices = ToArrayIndex(s0, failIndices)
			return successIndices, failIndices, er0b
		}
	} else {
		models2 = models
	}
	s := reflect.ValueOf(models2)
	_models, er1 := InterfaceSlice(models2)
	if er1 != nil {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
		return successIndices, failIndices, er1
	}
	_, er2 := InsertMany(w.db, w.tableName, _models, 0)

	if er2 == nil {
		// Return full success
		successIndices = ToArrayIndex(s, successIndices)
		return successIndices, failIndices, er2
	} else {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, er2
}

func ToArrayIndex(value reflect.Value, indices []int) []int {
	for i := 0; i < value.Len(); i++ {
		indices = append(indices, i)
	}
	return indices
}