package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func InsertOne(db *sql.DB, table string, model interface{}) (int64, error) {
	query, values := BuildInsertOneQuery(table, model)

	result, err := db.Exec(query, values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func UpdateOne(db *sql.DB, table string, model interface{}) (int64, error) {
	query, values := BuildUpdateOneQuery(table, model)

	result, err := db.Exec(query, values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func BuildUpdateOneQuery(table string, model interface{}) (string, []interface{}) {
	mapData, _, keys := BuildMapDataAndKeys(model)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	idFields := FindIdColumns(modelType)
	query := make(map[string]interface{})
	if len(idFields) > 1 {
		idsMap := make(map[string]interface{})
		for i := 0; i < len(idFields); i += 2 {
			idsMap[idFields[i]] = idFields[i+1]
		}
		query = MapToGORM(idsMap, modelType)
	} else {
		query = BuildQueryById(mapData[idFields[0]], modelType, idFields[0])
	}

	for _, gormColumnName := range idFields {
		if _, exist := Find(idFields, gormColumnName); exist {
			delete(mapData, gormColumnName)
			keys = RemoveItem(keys, gormColumnName)
		}
	}

	var values []interface{}
	var updateQuery []string
	for _, key := range keys {
		if v, ok := mapData[key]; ok {
			values = append(values, v)
			updateQuery = append(updateQuery, fmt.Sprintf("%v=?", QuoteColumnName(key)))
		}
	}

	setValueUpdate := strings.Join(updateQuery, ",")
	var queryArr []string
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", key))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("UPDATE %v SET %v WHERE %v", table, setValueUpdate, q), values
}

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

func BuildInsertOneQuery(table string, model interface{}) (string, []interface{}) {
	mapData, mapPrimaryKeyValue, keys := BuildMapDataAndKeys(model)
	var cols []string
	var values []interface{}
	for _, columnName := range keys {
		if value, ok := mapData[columnName]; ok {
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
	var arrValue []string
	for i := 0; i < numCol; i++ {
		arrValue = append(arrValue, "?")
	}
	value := fmt.Sprintf("(%v)", strings.Join(arrValue, ","))
	return fmt.Sprintf("INSERT INTO %v %v VALUES %v", table, column, value), values
}

func QuoteColumnName(str string) string {
	if strings.Contains(str, ".") {
		var newStrs []string
		for _, str := range strings.Split(str, ".") {
			newStrs = append(newStrs, str)
		}
		return strings.Join(newStrs, ".")
	}

	return str
}

func BuildMapDataAndKeys(model interface{}) (map[string]interface{}, map[string]interface{}, []string) {
	var mapValue = make(map[string]interface{})
	var mapPrimaryKeyValue = make(map[string]interface{})
	keysOfMapValue := make([]string, 0)
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	modelType := modelValue.Type()
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		if colName, isKey, exist := CheckByIndex(modelType, index); exist {
			if colName != "-" {
				fieldValue := modelValue.Field(index).Interface()
				if !isKey {
					mapValue[colName] = fieldValue
					keysOfMapValue = append(keysOfMapValue, colName)
				} else {
					mapPrimaryKeyValue[colName] = fieldValue
				}
			}
		} else {
			panic("cannot find column name")
		}
	}
	return mapValue, mapPrimaryKeyValue, keysOfMapValue
}

func CheckByIndex(modelType reflect.Type, index int) (col string, isKey bool, colExist bool) {
	fields := modelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

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
	case "mysql":
		return fmt.Sprintf("%s", key)
	case "mssql":
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
