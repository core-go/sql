package sql

import (
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

func BuildInsertSql(table string, model interface{}, i int, buildParam func(int) string) (string, []interface{}) {
	mapData, mapPrimaryKeyValue, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	for _, columnName := range keys {
		if value, ok := mapData[columnName]; ok && value != nil {
			cols = append(cols, QuoteColumnName(columnName))
			values = append(values, value)
		}
	}
	for columnName, value := range mapPrimaryKeyValue {
		cols = append(cols, QuoteColumnName(columnName))
		values = append(values, value)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	value := fmt.Sprintf("(%v)", BuildParametersFrom(i, numCol, buildParam))
	return fmt.Sprintf("insert into %v %v values %v", table, column, value), values
}

func BuildInsertSqlWithVersion(table string, model interface{}, i int, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version index not found")
	}

	var versionValue int64 = 1
	_, err := setValue(model, versionIndex, &versionValue)
	if err != nil {
		panic(err)
	}
	mapData, mapPrimaryKeyValue, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	for _, columnName := range keys {
		if value, ok := mapData[columnName]; ok && value != nil {
			cols = append(cols, QuoteColumnName(columnName))
			values = append(values, value)
		}
	}
	for columnName, value := range mapPrimaryKeyValue {
		cols = append(cols, QuoteColumnName(columnName))
		values = append(values, value)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	value := fmt.Sprintf("(%v)", BuildParametersFrom(i, numCol, buildParam))
	return fmt.Sprintf("INSERT INTO %v %v VALUES %v", table, column, value), values
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

func BuildMapDataAndKeys(model interface{}, update bool) (map[string]interface{}, map[string]interface{}, []string) {
	var mapValue = make(map[string]interface{})
	var mapPrimaryKeyValue = make(map[string]interface{})
	keysOfMapValue := make([]string, 0)
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	modelType := modelValue.Type()
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		if colName, isKey, exist := CheckByIndex(modelType, index, update); exist {
			fieldValue := modelValue.Field(index).Interface()
			if !isKey {
				if boolValue, ok := fieldValue.(bool); ok {
					mapValue[colName] = modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				} else {
					if boolPointer, okPointer := fieldValue.(*bool); okPointer {
						mapValue[colName] = modelType.Field(index).Tag.Get(strconv.FormatBool(*boolPointer))
					} else {
						mapValue[colName] = fieldValue
					}
				}
				keysOfMapValue = append(keysOfMapValue, colName)
			} else {
				mapPrimaryKeyValue[colName] = fieldValue
			}
		}
	}
	return mapValue, mapPrimaryKeyValue, keysOfMapValue
}

func CheckByIndex(modelType reflect.Type, index int, update bool) (col string, isKey bool, colExist bool) {
	fields := modelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")
	if strings.Contains(tag, IgnoreReadWrite) {
		return "", false, false
	}
	if update {
		if strings.Contains(tag, "updateable:false") {
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
