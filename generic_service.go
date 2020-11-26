package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

type GenericService struct {
	*ViewService
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewGenericServiceWithVersionAndMapper(db *sql.DB, modelType reflect.Type, tableName string, versionField string, mapper Mapper) *GenericService {
	defaultViewService := NewViewServiceWithMapper(db, modelType, tableName, mapper)
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			dbFieldName, exist := GetColumnNameByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &GenericService{ViewService: defaultViewService, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}
		}
	}
	return &GenericService{defaultViewService, versionField, -1, ""}
}
func NewGenericServiceWithVersion(db *sql.DB, modelType reflect.Type, tableName string, versionField string) *GenericService {
	return NewGenericServiceWithVersionAndMapper(db, modelType, tableName, versionField, nil)
}
func NewGenericService(db *sql.DB, modelType reflect.Type, tableName string) *GenericService {
	return NewGenericServiceWithVersionAndMapper(db, modelType, tableName, "", nil)
}

func (s *GenericService) Insert(ctx context.Context, model interface{}) (int64, error) {
	return Insert(s.Database, s.table, model)
}

func (s *GenericService) Update(ctx context.Context, model interface{}) (int64, error) {
	return Update(s.Database, s.table, model)
}

func (s *GenericService) Upsert(ctx context.Context, model map[string]interface{}) (int64, error) {
	return Upsert(s.Database, s.table, model)
}

func (s *GenericService) Delete(ctx context.Context, id interface{}) (int64, error) {
	l := len(s.keys)
	if l == 1 {
		return Delete(s.Database, s.table, BuildQueryById(id, s.modelType, s.keys[0]))
	} else {
		ids := id.(map[string]interface{})
		return Delete(s.Database, s.table, MapToGORM(ids, s.modelType))
	}
}

func (s *GenericService) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	return Patch(s.Database, s.table, model, s.modelType)
}
