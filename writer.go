package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

type Writer struct {
	*Loader
	Mapper         Mapper
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionField string, options ...Mapper) *Writer {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	var loader *Loader
	if mapper != nil {
		loader = NewLoader(db, tableName, modelType, mapper.DbToModel)
	} else {
		loader = NewLoader(db, tableName, modelType)
	}
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			dbFieldName, exist := GetColumnNameByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &Writer{Loader: loader, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}
		}
	}
	return &Writer{loader, mapper, versionField, -1, ""}
}
func NewWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...Mapper) *Writer {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewWriterWithVersion(db, tableName, modelType, "", mapper)
}

func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		return Insert(s.Database, s.table, m2)
	}
	return Insert(s.Database, s.table, model)
}

func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		return Update(s.Database, s.table, m2)
	}
	return Update(s.Database, s.table, model)
}

func (s *Writer) Save(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		return Upsert(s.Database, s.table, m2)
	}
	return Upsert(s.Database, s.table, model)
}

func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	l := len(s.keys)
	if l == 1 {
		return Delete(s.Database, s.table, BuildQueryById(id, s.modelType, s.keys[0]))
	} else {
		ids := id.(map[string]interface{})
		return Delete(s.Database, s.table, MapToGORM(ids, s.modelType))
	}
}

func (s *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		return Patch(s.Database, s.table, model, s.modelType)
	}
	return Patch(s.Database, s.table, model, s.modelType)
}
