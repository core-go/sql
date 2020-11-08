package orm

import (
	"context"
	"gorm.io/gorm"
	"reflect"
)

type SqlBatchInsert struct {
	db        *gorm.DB
	tableName string
}

func NewSqlBatchInsert(database *gorm.DB, tableName string) *SqlBatchInsert {
	return &SqlBatchInsert{database, tableName}
}

func (w *SqlBatchInsert) WriteBatch(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	_models, err1 := InterfaceSlice(models)
	if err1 != nil {
		// Return full fail
		failIndices = toArrayIndex(s, failIndices)
		return successIndices, failIndices, err1
	}
	_, err := InsertMany(w.db, w.tableName, _models, 0)

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

func toArrayIndex(value reflect.Value, indices []int) []int {
	for i := 0; i < value.Len(); i++ {
		indices = append(indices, i)
	}
	return indices
}
