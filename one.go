package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func RemoveItem(slice []string, val string) []string {
	for i, item := range slice {
		if item == val {
			return RemoveIndex(slice, i)
		}
	}
	return slice
}


func BuildToInsert(table string, model interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options...bool) (string, []interface{}) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	return BuildToInsertWithSchema(table, model, buildParam, toArray, boolSupport)
}
func BuildToInsertWithSchema(table string, model interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, boolSupport bool, options...Schema) (string, []interface{}) {
	modelType := reflect.TypeOf(model)
	var cols []string
	var schema map[string]FieldDB
	if len(options) > 0 {
		cols = options[0].Columns
		schema = options[0].Fields
	} else {
		cols, _, schema = MakeSchema(modelType)
	}
	mv := reflect.ValueOf(model)
	values := make([]string, 0)
	args := make([]interface{}, 0)
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
		if isNil {
			values = append(values, "null")
		} else {
			v, ok := GetDBValue(fieldValue)
			if ok {
				values = append(values, v)
			} else {
				if boolValue, ok := fieldValue.(bool); ok {
					if boolSupport {
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
	return fmt.Sprintf("insert into %v(%v) values (%v)", table, strings.Join(cols, ","), strings.Join(values, ",")), args
}

func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string, boolSupport bool, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version index not found")
	}

	var versionValue int64 = 1
	_, err := setValue(model, versionIndex, &versionValue)
	if err != nil {
		panic(err)
	}
	i := 1
	modelType := reflect.TypeOf(model)
	cols, _, schema := MakeSchema(modelType)
	mv := reflect.ValueOf(model)
	values := make([]string, 0)
	args := make([]interface{}, 0)
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
					if boolSupport {
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
					}else{
						values = append(values, buildParam(i))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
		}
	}
	column := strings.Join(cols, ",")
	return fmt.Sprintf("insert into %v(%v)values(%v)", table, column, strings.Join(values, ",")), args
}
func QuoteByDriver(key, driver string) string {
	switch driver {
	case DriverMysql:
		return fmt.Sprintf("`%s`", key)
	case DriverMssql:
		return fmt.Sprintf(`[%s]`, key)
	default:
		return fmt.Sprintf(`"%s"`, key)
	}
}
func BuildResult(result int64, err error) (int64, error) {
	if err != nil {
		return result, err
	} else {
		return result, nil
	}
}
