package orm

import (
	"context"
	"gorm.io/gorm"
	"reflect"
)

type ViewService struct {
	Database   *gorm.DB
	Mapper     Mapper
	modelType  reflect.Type
	modelsType reflect.Type
	keys       []string
	table      string
}

func NewViewService(db *gorm.DB, modelType reflect.Type, tableName string, mapper Mapper) *ViewService {
	idNames := FindIdFields(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	return &ViewService{db, mapper, modelType, modelsType, idNames, tableName}
}

func NewDefaultViewService(db *gorm.DB, modelType reflect.Type, tableName string) *ViewService {
	return NewViewService(db, modelType, tableName, nil)
}

func (s *ViewService) Keys() []string {
	return s.keys
}

func (s *ViewService) All(ctx context.Context) (interface{}, error) {
	var results = s.initArrayResults()
	err := FindWithResults(s.Database, s.table, results, map[string]interface{}{})
	if err == nil && s.Mapper != nil {
		r2, er2 := s.Mapper.DbToModels(ctx, results)
		if er2 != nil {
			return results, err
		}
		return r2, err
	}
	return results, err
}

func (s *ViewService) Load(ctx context.Context, id interface{}) (interface{}, error) {
	var result = s.initSingleResult()
	l := len(s.keys)
	if l <= 1 {
		query := BuildQueryById(id, s.modelType, s.keys[0])
		found, err := FindOneWithResult(s.Database, s.table, result, query)
		if found == false && err != nil {
			return nil, err
		}
		return result, nil
	}
	var ids = id.(map[string]interface{})
	query := MapToGORM(ids, s.modelType)
	found, err := FindOneWithResult(s.Database, s.table, result, query)
	if found == false && err != nil {
		return nil, err
	}
	if s.Mapper != nil {
		r2, er2 := s.Mapper.DbToModel(ctx, result)
		if er2 != nil {
			return result, er2
		}
		return r2, er2
	}
	return result, nil
}

func (s *ViewService) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	l := len(s.keys)
	if l <= 1 {
		query := BuildQueryById(id, s.modelType, s.keys[0])
		ok, er0 := FindOneWithResult(s.Database, s.table, result, query)
		if ok && er0 == nil && s.Mapper != nil {
			_, er2 := s.Mapper.DbToModel(ctx, result)
			if er2 != nil {
				return ok, er2
			}
		}
		return ok, er0
	}
	var ids = id.(map[string]interface{})
	query := MapToGORM(ids, s.modelType)
	ok, er2 := FindOneWithResult(s.Database, s.table, result, query)
	if ok && er2 == nil && s.Mapper != nil {
		_, er3 := s.Mapper.DbToModel(ctx, result)
		if er3 != nil {
			return ok, er3
		}
	}
	return ok, er2
}

func (s *ViewService) Exist(ctx context.Context, id interface{}) (bool, error) {
	var result = s.initSingleResult()
	l := len(s.keys)
	if l <= 1 {
		query := BuildQueryById(id, s.modelType, s.keys[0])
		return Exists(s.Database, s.table, result, query)
	}
	var ids = id.(map[string]interface{})
	query := MapToGORM(ids, s.modelType)
	return Exists(s.Database, s.table, result, query)
}

func (s *ViewService) initSingleResult() interface{} {
	return reflect.New(s.modelType).Interface()
}

func (s *ViewService) initArrayResults() interface{} {
	return reflect.New(s.modelsType).Interface()
}
