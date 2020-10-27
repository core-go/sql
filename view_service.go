package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
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

func (s *ViewService) sqlKeys() []string {
	return s.keys
}

func (s *ViewService) sqlAll(ctx context.Context) (interface{}, error) {
	queryGetAll := s.GetAll()

	fmt.Printf("queryGetAll: %v\n", queryGetAll)
	result, err := s.Database.Exec(queryGetAll)
	return result, err
}

func (s *ViewService) SQLFindById(ids map[string]interface{}) (interface{}, error) {
	queryFindById, values := s.FindById(ids)
	fmt.Printf("queryFindById: %v\n", queryFindById)
	result, err := s.Database.Exec(queryFindById, values...)
	return result, err
}

func (s *ViewService) FindById(ids map[string]interface{}) (string, []interface{}) {
	modelType := s.modelType
	numField := modelType.NumField()
	var idFieldNames []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				idFieldNames = append(idFieldNames, field.Name)
			}
		}
	}
	query := make(map[string]interface{})
	if len(idFieldNames) > 1 {
		idsMap := make(map[string]interface{})
		for i := 0; i < len(idFieldNames); i += 2 {
			idsMap[idFieldNames[i]] = idFieldNames[i+1]
		}
		query = s.BuildQueryByMulId(idsMap, modelType)
	} else {
		query = s.BuildQueryBySingleId(ids[idFieldNames[0]], modelType, idFieldNames[0])
	}
	var queryArr []string
	var values []interface{}
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", key))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", s.table, q), values
}

func (s *ViewService) GetAll() string {
	return fmt.Sprintf("SELECT * FROM %v", s.table)
}

func (s *ViewService) BuildQueryBySingleId(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}

func (s *ViewService) BuildQueryByMulId(ids map[string]interface{}, modelType reflect.Type) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var columnName string
	for colName, value := range ids {
		columnName, _ = GetColumnName(modelType, colName)
		queryGen[columnName] = value
	}
	return queryGen
}

func (s *ViewService) initSingleResult() interface{} {
	return reflect.New(s.modelType).Interface()
}

func (s *ViewService) initArrayResults() interface{} {
	return reflect.New(s.modelsType).Interface()
}
