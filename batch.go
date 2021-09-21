package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)
func BuildToUpdateBatch(table string, models interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...bool) ([]Statement, error) {
	return BuildToUpdateBatchWithVersion(table, models, -1, buildParam, toArray, options...)
}
func BuildInsertStatements(table string, models interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...bool) ([]Statement, error) {
	return BuildInsertStatementsWithVersion(table, models, -1, buildParam, toArray, false, options...)
}
func BuildInsertStatementsWithVersion(table string, models interface{}, versionIndex int, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, includeNull bool, options ...bool) ([]Statement, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	strt := Schema{Columns: cols, Keys: keys, Fields: schema}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToInsertWithSchema(table, model, versionIndex, buildParam, boolSupport, includeNull, toArray, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToUpdateBatchWithVersion(table string, models interface{}, versionIndex int, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...bool) ([]Statement, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	strt := Schema{Columns: cols, Keys: keys, Fields: schema}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToUpdateWithSchema(table, model, versionIndex, buildParam, boolSupport, toArray, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToInsertBatch(table string, models interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(int) string) (string, []interface{}, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuildByDriver(driver)
	}

	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("models must be a slice")
	}
	if s.Len() == 0 {
		return "", nil, nil
	}
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, _, schema := MakeSchema(modelType)
	slen := s.Len()
	if driver != DriverOracle {
		i := 1
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			values := make([]string, 0)
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if driver == DriverPostgres {
								if boolValue {
									values = append(values, "true")
								} else {
									values = append(values, "false")
								}
							} else {
								if boolValue {
									if fdb.True != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, "'1'")
									}
								} else {
									if fdb.False != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, "'0'")
									}
								}
							}
						} else {
							if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
								values = append(values, buildParam(i))
								i = i + 1
								args = append(args, toArray(fieldValue))
							} else {
								values = append(values, buildParam(i))
								i = i + 1
								args = append(args, fieldValue)
							}
						}
					}
				}
			}
			x := "(" + strings.Join(values, ",") + ")"
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
			table,
			strings.Join(cols, ","),
			strings.Join(placeholders, ","),
		))
		return query, args, nil
	} else {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			i := 1
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if !isNil {
					iCols = append(iCols, col)
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.True)
								} else {
									values = append(values, "'1'")
								}
							} else {
								if fdb.False != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.False)
								} else {
									values = append(values, "'0'")
								}
							}
						} else {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			x := fmt.Sprintf("into %s(%s)values(%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf("insert all %s select * from dual", strings.Join(placeholders, " "))
		return query, args, nil
	}
}
func BuildToSaveBatch(table string, models interface{}, drive string, options ...func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models must be a slice")
	}
	if s.Len() == 0 {
		return nil, nil
	}
	var toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	if len(options) > 0 {
		toArray = options[0]
	}
	buildParam := GetBuildByDriver(drive)
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	strt := Schema{Columns: cols, Keys: keys, Fields: schema}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args, err := BuildToSaveWithSchema(table, model, drive, buildParam, toArray, strt)
		if err != nil {
			return stmts, err
		}
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
