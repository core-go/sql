package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
)

type Writer struct {
	*Loader
	jsonColumnMap  map[string]string
	Mapper         Mapper
	versionField   string
	versionIndex   int
	versionDBField string
	schema         *Schema
	BoolSupport    bool
	ToArray        func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...Mapper) (*Writer, error) {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewWriterWithVersionAndArray(db, tableName, modelType, "", nil, mapper)
}
func NewWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionField string, options ...Mapper) (*Writer, error) {
	return NewWriterWithVersionAndArray(db, tableName, modelType, versionField, nil, options...)
}
func NewWriterWithVersionAndArray(db *sql.DB, tableName string, modelType reflect.Type, versionField string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...Mapper) (*Writer, error) {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewSqlWriterWithVersion(db, tableName, modelType, versionField, mapper, toArray)
}
func NewSqlWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionField string, mapper Mapper, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) (*Writer, error) {
	var loader *Loader
	var err error
	if mapper != nil {
		loader, err = NewSqlLoader(db, tableName, modelType, mapper.DbToModel, toArray, options...)
	} else {
		loader, err = NewSqlLoader(db, tableName, modelType, nil, toArray, options...)
	}
	if err != nil {
		return nil, err
	}
	driver := GetDriver(db)
	boolSupport := driver == DriverPostgres
	schema := CreateSchema(modelType)
	jsonColumnMap := MakeJsonColumnMap(modelType)
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &Writer{Loader: loader, BoolSupport: boolSupport, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, ToArray: toArray, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}, nil
		}
	}
	return &Writer{Loader: loader, BoolSupport: boolSupport, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, ToArray: toArray, versionField: versionField, versionIndex: -1}, nil
}
func NewWriterWithMap(db *sql.DB, tableName string, modelType reflect.Type, mapper Mapper, options ...func(i int) string) (*Writer, error) {
	return NewSqlWriterWithVersion(db, tableName, modelType, "", mapper, nil, options...)
}
func NewWriterWithMapAndArray(db *sql.DB, tableName string, modelType reflect.Type, mapper Mapper, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(i int) string) (*Writer, error) {
	return NewSqlWriterWithVersion(db, tableName, modelType, "", mapper, toArray, options...)
}
func NewWriterWithArray(db *sql.DB, tableName string, modelType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...Mapper) (*Writer, error) {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewWriterWithVersionAndArray(db, tableName, modelType, "", toArray, mapper)
}

func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		queryInsert, values := BuildToInsertWithVersion(s.table, m2, s.versionIndex, s.BuildParam, s.BoolSupport, s.ToArray, s.schema)
		result, err := s.Database.ExecContext(ctx, queryInsert, values...)
		if err != nil {
			return handleDuplicate(s.Database, err)
		}
		return result.RowsAffected()
	}
	queryInsert, values := BuildToInsertWithVersion(s.table, model, s.versionIndex, s.BuildParam, s.BoolSupport, s.ToArray, s.schema)
	result, err := s.Database.ExecContext(ctx, queryInsert, values...)
	if err != nil {
		return handleDuplicate(s.Database, err)
	}
	return result.RowsAffected()
}

func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		query, values := BuildToUpdateWithVersion(s.table, m2, s.versionIndex, s.BuildParam, s.BoolSupport, s.ToArray, s.schema)
		result, err := s.Database.ExecContext(ctx, query, values...)
		if err != nil {
			return -1, err
		}
		return result.RowsAffected()
	}
	query, values := BuildToUpdateWithVersion(s.table, model, s.versionIndex, s.BuildParam, s.BoolSupport, s.ToArray, s.schema)
	result, err := s.Database.ExecContext(ctx, query, values...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func (s *Writer) Save(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		return SaveWithArray(ctx, s.Database, s.table, m2, s.ToArray, s.schema)
	}
	return SaveWithArray(ctx, s.Database, s.table, model, s.ToArray, s.schema)
}
func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	l := len(s.keys)
	if l == 1 {
		return Delete(ctx, s.Database, s.table, BuildQueryById(id, s.modelType, s.keys[0]), s.BuildParam)
	} else {
		ids := id.(map[string]interface{})
		return Delete(ctx, s.Database, s.table, MapToGORM(ids, s.modelType), s.BuildParam)
	}
}
func (s *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
	}
	MapToDB(&model, s.modelType)
	dbColumnMap := JSONToColumns(model, s.jsonColumnMap)
	query, values := BuildToPatchWithVersion(s.table, dbColumnMap, s.schema.SKeys, s.BuildParam, s.ToArray, s.versionDBField, s.schema.Fields)
	result, err := s.Database.ExecContext(ctx, query, values...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}
func MapToDB(model *map[string]interface{}, modelType reflect.Type) {
	for colName, value := range *model {
		if boolValue, boolOk := value.(bool); boolOk {
			index := GetIndexByTag("json", colName, modelType)
			if index > -1 {
				valueS := modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				valueInt, err := strconv.Atoi(valueS)
				if err != nil {
					(*model)[colName] = valueS
				} else {
					(*model)[colName] = valueInt
				}
				continue
			}
		}
		(*model)[colName] = value
	}
}
