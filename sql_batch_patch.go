package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

type SqlBatchPatch struct {
	db          *sql.DB
	tableName   string
	IdNames     []string
	IdJsonName  []string
	modelsType  reflect.Type
	modelsTypes reflect.Type
}

func NewSqlBatchPatchWithIdName(database *sql.DB, tableName string, modelType reflect.Type, fieldName []string) *SqlBatchPatch {
	modelsTypes := reflect.Zero(reflect.SliceOf(modelType)).Type()
	idJsonName := make([]string, 0)
	if len(fieldName) == 0 {
		fieldName, idJsonName = FindNames(modelType)
	}
	return &SqlBatchPatch{database, tableName, fieldName, idJsonName, modelType, modelsTypes}
}

func (w *SqlBatchPatch) WriteBatch(ctx context.Context, models []map[string]interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	_, err := PatchMaps(w.db, w.tableName, models, w.IdNames, w.IdJsonName)

	if err == nil {
		// Return full success
		successIndices = toArrayMapIndex(models, failIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = toArrayMapIndex(models, failIndices)
	}
	return successIndices, failIndices, err
}

func toArrayMapIndex(models []map[string]interface{}, indices []int) []int {
	for i, _ := range models {
		indices = append(indices, i)
	}
	return indices
}

func FindNames(modelType reflect.Type) ([]string, []string) {
	numField := modelType.NumField()
	var idColumnFields []string
	var idJsons []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idColumnFields = append(idColumnFields, k)
					tag1, ok1 := field.Tag.Lookup("json")
					tagJsons := strings.Split(tag1, ",")
					if ok1 && len(tagJsons) > 0 {
						idJsons = append(idJsons, tagJsons[0])
					}
				}
			}
		}
	}
	return idColumnFields, idJsons
}

func FindJsonName(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	 mapJsonColumn  := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if ok {
			tag1, ok1 := field.Tag.Lookup("json")
			tagJsons := strings.Split(tag1, ",")
			if ok1 && len(tagJsons) > 0 {
				mapJsonColumn[tagJsons[0]] = column
			}
		}
	}
	return mapJsonColumn
}

