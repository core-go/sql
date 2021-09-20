package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

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
func BuildToUpdate(table string, model interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options...bool) (string, []interface{}) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	return BuildToUpdateWithSchema(table, model, buildParam, toArray, boolSupport)
}
func BuildToUpdateWithSchema(table string, model interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, boolSupport bool, options...Schema) (string, []interface{}) {
	var cols, keys []string
	var schema map[string]FieldDB
	modelType := reflect.TypeOf(model)
	if len(options) > 0 {
		m := options[0]
		cols = m.Columns
		keys = m.Keys
		schema = m.Fields
	} else {
		cols, keys, schema = MakeSchema(modelType)
	}
	mv := reflect.ValueOf(model)
	values := make([]string, 0)
	where := make([]string, 0)
	args := make([]interface{}, 0)
	i := 1
	for _, col := range cols {
		fdb := schema[col]
		if !fdb.Key && fdb.Update {
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
				values = append(values, col+"=null")
			} else {
				v, ok := GetDBValue(fieldValue)
				if ok {
					values = append(values, col+"="+v)
				} else {
					if boolValue, ok := fieldValue.(bool); ok {
						if boolSupport {
							if boolValue {
								values = append(values, col+"=true")
							} else {
								values = append(values, col+"=false")
							}
						} else {
							if boolValue {
								if fdb.True != nil {
									values = append(values, col+"="+buildParam(i))
									i = i + 1
									args = append(args, *fdb.True)
								} else {
									values = append(values, col+"='1'")
								}
							} else {
								if fdb.False != nil {
									values = append(values, col+"="+buildParam(i))
									i = i + 1
									args = append(args, *fdb.False)
								} else {
									values = append(values, col+"='0'")
								}
							}
						}
					} else {
						if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
							values = append(values, col+"="+buildParam(i))
							i = i + 1
							args = append(args, toArray(fieldValue))
						} else {
							values = append(values, col+"="+buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
		}
	}
	for _, col := range keys {
		fdb := schema[col]
		f := mv.Field(fdb.Index)
		fieldValue := f.Interface()
		if f.Kind() == reflect.Ptr {
			if !reflect.ValueOf(fieldValue).IsNil() {
				fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
			}
		}
		v, ok := GetDBValue(fieldValue)
		if ok {
			where = append(where, col+"="+v)
		} else {
			where = append(where, col+"="+buildParam(i))
			i = i + 1
			args = append(args, fieldValue)
		}
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, ","))
	return query, args
}
func BuildToUpdateWithVersion(table string, model interface{}, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}
	valueOfModel := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
	nextVersion := currentVersion + 1
	_, err := setValue(model, versionIndex, &nextVersion)
	if err != nil {
		panic(err)
	}

	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, true)
	versionColName, exist := GetColumnNameByIndex(valueOfModel.Type(), versionIndex)
	if !exist {
		panic("version's column not found")
	}
	mapKey[versionColName] = currentVersion

	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for _, colName := range columns {
		if v1, ok := mapData[colName]; ok {
			v3, ok3 := GetDBValue(v1)
			if ok3 {
				colSet = append(colSet, fmt.Sprintf("%v = "+v3, colName))
			} else {
				values = append(values, v1)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber))
				colNumber++
			}
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}
	for _, colName := range keys {
		if v2, ok := mapKey[colName]; ok {
			v3, ok3 := GetDBValue(v2)
			if ok3 {
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+v3)
			} else {
				values = append(values, v2)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber))
			}
			colNumber++
		}
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}
func BuildToPatch(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string) (string, []interface{}) {
	scope := statement()
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if colName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, colName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	var value []interface{}

	n := len(scope.Columns)
	sets, val1, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, val1...)
	columnsKeys := len(scope.Keys)
	where, val2, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &columnsKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, val2...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildPatchWithVersion(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string, versionIndex int, versionJsonName, versionColName string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}

	currentVersion, ok := model[versionJsonName]
	if !ok {
		panic("version field not found")
	}
	nextVersion := currentVersion.(int64) + 1
	model[versionJsonName] = nextVersion

	scope := statement()
	var value []interface{}
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if columName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, columName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	scope.Values = append(scope.Values, currentVersion)
	scope.Keys = append(scope.Keys, versionColName)

	n := len(scope.Columns)
	sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, setVal...)
	numKeys := len(scope.Keys)
	where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, whereVal...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildToDelete(table string, ids map[string]interface{}, buildParam func(int) string) (string, []interface{}) {
	var values []interface{}
	var queryArr []string
	i := 1
	for key, value := range ids {
		queryArr = append(queryArr, fmt.Sprintf("%v = %v", QuoteColumnName(key), buildParam(i)))
		values = append(values, value)
		i++
	}
	q := strings.Join(queryArr, " and ")
	return fmt.Sprintf("delete from %v where %v", table, q), values
}
