package sql

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/sirupsen/logrus"
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
	fieldsIndex       map[string]int
	table             string
}

func NewViewServiceWithMapper(db *sql.DB, modelType reflect.Type, tableName string, mapper Mapper) *ViewService {
	_, idNames := FindNames(modelType)
	mapJsonColumnKeys := MapJsonColumn(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	fieldsIndex, er0 := GetColumnIndexes(modelType)
	if er0 != nil {
		panic(er0)
	}
	return &ViewService{db, mapper, modelType, modelsType, idNames, mapJsonColumnKeys, fieldsIndex, tableName}
}

func NewViewService(db *sql.DB, modelType reflect.Type, tableName string) *ViewService {
	return NewViewServiceWithMapper(db, modelType, tableName, nil)
}

func (s *ViewService) Keys() []string {
	return s.keys
}

func (s *ViewService) All(ctx context.Context) (interface{}, error) {
	queryGetAll := BuildSelectAllQuery(s.table)
	models := reflect.New(s.modelsType).Interface()
	err := QueryWithType(s.Database, models, s.modelType, s.fieldsIndex, queryGetAll)
	return models, err
}

func (s *ViewService) Load(ctx context.Context, ids interface{}) (interface{}, error) {
	queryFindById, values := BuildFindById(s.Database, s.table, ids, s.mapJsonColumnKeys, s.keys)
	if GetDriverName(s.Database) == DriverOracle {
		for i := 0; i < len(values); i++ {
			count := i + 1
			queryFindById = strings.Replace(queryFindById, "?", ":val"+fmt.Sprintf("%v", count), 1)
		}
	}
	result, err1 := QueryRow(s.Database, s.modelType, s.fieldsIndex, queryFindById, values...)
	return result, err1
}

func (s *ViewService) Exist(ctx context.Context, id interface{}) (bool, error) {
	var count int32
	var where string
	var driver = GetDriverName(s.Database)
	var values []interface{}
	colNumber := 1
	if len(s.keys) == 1 {
		where = fmt.Sprintf("where %s = %s", s.mapJsonColumnKeys[s.keys[0]], BuildParam(colNumber, driver))
		values = append(values, id)
		colNumber++
	} else {
		queres := make([]string, 0)
		var ids = id.(map[string]interface{})
		for k, idk := range ids {
			columnName := s.mapJsonColumnKeys[k]
			queres = append(queres, fmt.Sprintf("%s = %s", columnName, BuildParam(colNumber, driver)))
			values = append(values, idk)
			colNumber++
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
	var values []interface{}
	sql, values := BuildFindById(s.Database, s.table, id, s.mapJsonColumnKeys, s.keys)
	rowData, err1 := QueryRow(s.Database, s.modelType, s.fieldsIndex, sql, values...)
	if err1 != nil {
		return false, err1
	}
	reflect.ValueOf(result).Elem().Set(reflect.ValueOf(rowData).Elem())
	if s.Mapper != nil {
		_, er3 := s.Mapper.DbToModel(ctx, result)
		if er3 != nil {
			return true, er3
		}
	}
	return true, nil
}

func MapJsonColumn(modelType reflect.Type) map[string]string {
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
