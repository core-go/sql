package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type ViewService struct {
	Database          *sql.DB
	Mapper            Mapper
	modelType         reflect.Type
	modelsType        reflect.Type
	keys              []string
	mapJsonColumnKeys map[string]string
	table             string
}

func NewViewService(db *sql.DB, modelType reflect.Type, tableName string, mapper Mapper) *ViewService {
	_, idNames := FindNames(modelType)
	mapJsonColumnKeys := mapJsonColumn(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	return &ViewService{db, mapper, modelType, modelsType, idNames, mapJsonColumnKeys, tableName}
}

func NewDefaultViewService(db *sql.DB, modelType reflect.Type, tableName string) *ViewService {
	return NewViewService(db, modelType, tableName, nil)
}

func (s *ViewService) Keys() []string {
	return s.keys
}

func (s *ViewService) All(ctx context.Context) (interface{}, error) {
	queryGetAll := BuildSelectAllQuery(s.table)
	row, err := s.Database.Query(queryGetAll)
	result, err := ScanByModelType(row, s.modelType)
	return result, err
}

func (s *ViewService) Load(ctx context.Context, ids interface{}) (interface{}, error) {
	queryFindById, values := BuildFindById(s.Database, s.table, ids, s.mapJsonColumnKeys, s.keys)
	row := s.Database.QueryRow(queryFindById, values...)
	result, err2 := ScanRow(row, s.modelType)
	return result, err2
}

func (s *ViewService) Exist(ctx context.Context, id interface{}) (bool, error) {
	var count int32
	var where string
	var driver = getDriver(s.Database)
	var values []interface{}
	if len(s.keys) == 1 {
		where = fmt.Sprintf("where %s = %s", s.mapJsonColumnKeys[s.keys[0]], BuildMarkByDriver(0, driver))
		values = append(values, id)
	} else {
		queres := make([]string, 0)
		var ids = id.(map[string]interface{})
		for k, idk := range ids {
			columnName := s.mapJsonColumnKeys[k]
			queres = append(queres, fmt.Sprintf("%s = %s", columnName, BuildMarkByDriver(0, driver)))
			values = append(values, idk)
		}
		where = "where " + strings.Join(queres, " and ")
	}
	row := s.Database.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s %s", s.table, where), values...)
	if err := row.Scan(&count); err != nil {
		return false, err
	} else {
		if count >= 1 {
			return true, nil
		}
		return false, nil
	}
}

func (s *ViewService) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	l := len(s.keys)
	var values []interface{}
	var driver = getDriver(s.Database)
	var where string = ""
	if l <= 1 {
		where = fmt.Sprintf("where %s = %s", s.mapJsonColumnKeys[s.keys[0]], BuildMarkByDriver(0, driver))
		values = append(values, id)
	} else {
		queres := make([]string, 0)
		var ids = id.(map[string]interface{})
		for k, idk := range ids {
			columnName := s.mapJsonColumnKeys[k]
			queres = append(queres, fmt.Sprintf("%s = %s", columnName, BuildMarkByDriver(0, driver)))
			values = append(values, idk)
		}
		where = "where " + strings.Join(queres, " and ")
	}
	row, err1 := s.Database.Query(fmt.Sprintf("SELECT * FROM %s %s LIMIT 1", s.table, where), values...)
	if err1 != nil {
		return false, err1
	}
	r, err2 := ScanByModelType(row, s.modelType)
	if err2 != nil {
		return false, err2
	}
	if len(r) == 0 {
		return false, errors.New("record not found")
	}
	reflect.ValueOf(result).Elem().Set(reflect.ValueOf(r[0]).Elem())
	if s.Mapper != nil {
		_, er3 := s.Mapper.DbToModel(ctx, result)
		if er3 != nil {
			return true, er3
		}
	}
	return true, nil
}

func mapJsonColumn(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	columnNameKeys := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				if has := strings.Contains(ormTag, "column"); has {
					str1 := strings.Split(ormTag, ";")
					num := len(str1)
					for i := 0; i < num; i++ {
						str2 := strings.Split(str1[i], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								tagj, ok1 := field.Tag.Lookup("json")
								t := strings.Split(tagj, ",")
								if ok1 && len(t) > 0 {
									json := t[0]
									columnNameKeys[json] = str2[j+1]
								}
							}
						}
					}
				}
			}
		}
	}
	return columnNameKeys
}
