package sql

import (
	"context"
	"database/sql"
	"reflect"
)

type BatchUpdater struct {
	db        *sql.DB
	tableName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchUpdater(db *sql.DB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &BatchUpdater{db: db, tableName: tableName, Map: mp}
}

func (w *BatchUpdater) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
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
	s := reflect.ValueOf(models)
	_models, err1 := InterfaceSlice(models)
	if err1 != nil {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
		return successIndices, failIndices, err1
	}
	_, err := UpdateMany(w.db, w.tableName, _models)

	if err == nil {
		// Return full success
		successIndices = ToArrayIndex(s, successIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = ToArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, err
}
