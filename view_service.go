package sql

import (
	"context"
	"database/sql"
	"reflect"
)

type ViewService struct {
	Database   *sql.DB
	Mapper     Mapper
	modelType  reflect.Type
	modelsType reflect.Type
	keys       []string
	table      string
}

func NewViewService(db *sql.DB, modelType reflect.Type, tableName string, mapper Mapper) *ViewService {
	idNames := FindIdFields(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	return &ViewService{db, mapper, modelType, modelsType, idNames, tableName}
}

func NewDefaultViewService(db *sql.DB, modelType reflect.Type, tableName string) *ViewService {
	return NewViewService(db, modelType, tableName, nil)
}

func (s *ViewService) Keys() []string {
	return s.keys
}

func (s *ViewService) All(ctx context.Context) (interface{}, error) {
	queryGetAll := BuildSelectAllQuery(s.table)
	// fmt.Printf("queryGetAll: %v\n", queryGetAll)
	row, err := s.Database.Query(queryGetAll)
	result, err := ScanByModelType(row, s.modelType)
	return result, err
}

func (s *ViewService) FindById(ctx context.Context, ids map[string]interface{}) (interface{}, error) {
	queryFindById, values := BuildFindById(s.table, ids, s.modelType)
	// fmt.Printf("queryFindById: %v\n", queryFindById)
	row, err := s.Database.Query(queryFindById, values...)
	result, err := ScanByModelType(row, s.modelType)
	return result, err
}
