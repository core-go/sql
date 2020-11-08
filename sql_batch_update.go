package orm

import (
	"context"
	"gorm.io/gorm"
	"reflect"
)

type SqlBatchUpdate struct {
	db        *gorm.DB
	tableName string
}

func NewSqlBatchUpdate(database *gorm.DB, tableName string) *SqlBatchUpdate {
	return &SqlBatchUpdate{database, tableName}
}

func (w *SqlBatchUpdate) WriteBatch(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	_models, err1 := InterfaceSlice(models)
	if err1 != nil {
		// Return full fail
		failIndices = toArrayIndex(s, failIndices)
		return successIndices, failIndices, err1
	}
	_, err := UpdateMany(w.db, w.tableName, _models)

	if err == nil {
		// Return full success
		successIndices = toArrayIndex(s, successIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = toArrayIndex(s, failIndices)
	}
	return successIndices, failIndices, err
}
