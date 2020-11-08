package orm

import (
	"context"
	"gorm.io/gorm"
	"reflect"
)

type SqlBatchPatch struct {
	db        *gorm.DB
	tableName string
}

func NewSqlBatchPatch(database *gorm.DB, tableName string) *SqlBatchPatch {
	return &SqlBatchPatch{database, tableName}
}

func (w *SqlBatchPatch) WriteBatch(ctx context.Context, models []map[string]interface{}) ([]int, []int, error) {
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
