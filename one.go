package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
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

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func BuildToInsert(table string, model interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...bool) (string, []interface{}) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	modelType := reflect.TypeOf(model)
	cols, _, schema := MakeSchema(modelType)
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

func BuildToInsertWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string, driver string, toArray func(interface{}) interface {
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
					if driver == DriverPostgres || driver == DriverCassandra {
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
					if driver == DriverPostgres && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
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

func QuoteColumnName(str string) string {
	//if strings.Contains(str, ".") {
	//	var newStrs []string
	//	for _, str := range strings.Split(str, ".") {
	//		newStrs = append(newStrs, str)
	//	}
	//	return strings.Join(newStrs, ".")
	//}
	return str
}

func BuildMapDataAndKeys(model interface{}, update bool) (map[string]interface{}, map[string]interface{}, []string, []string) {
	var mapData = make(map[string]interface{})
	var mapKey = make(map[string]interface{})
	keys := make([]string, 0)
	columns := make([]string, 0)
	mv := reflect.Indirect(reflect.ValueOf(model))
	modelType := mv.Type()
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		if colName, isKey, exist := CheckByIndex(modelType, i, update); exist {
			f := mv.Field(i)
			fieldValue := f.Interface()
			isNil := false
			if f.Kind() == reflect.Ptr {
				if reflect.ValueOf(fieldValue).IsNil() {
					isNil = true
				} else {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			if isKey {
				columns = append(columns, colName)
				if !isNil {
					mapKey[colName] = fieldValue
				}
			} else {
				keys = append(keys, colName)
				if !isNil {
					if boolValue, ok := fieldValue.(bool); ok {
						valueS, okS := modelType.Field(i).Tag.Lookup(strconv.FormatBool(boolValue))
						if okS {
							mapData[colName] = valueS
						} else {
							mapData[colName] = boolValue
						}
					} else {
						mapData[colName] = fieldValue
					}
				}
			}
		}
	}
	return mapData, mapKey, keys, columns
}
func CheckByIndex(modelType reflect.Type, index int, update bool) (col string, isKey bool, colExist bool) {
	field := modelType.Field(index)
	tag, _ := field.Tag.Lookup("gorm")
	if strings.Contains(tag, IgnoreReadWrite) {
		return "", false, false
	}
	if update {
		if strings.Contains(tag, "update:false") {
			return "", false, false
		}
	}
	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					isKey := strings.Contains(tag, "primary_key")
					return str2[j+1], isKey, true
				}
			}
		}
	}
	return "", false, false
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
