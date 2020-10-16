package sql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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
	mapData, keys := BuildMapDataAndKeys(model)
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
	mapData, _ := BuildMapDataAndKeys(model)
	var cols []string
	var values []interface{}
	for col, v := range mapData {
		cols = append(cols, QuoteColumnName(col))
		values = append(values, v)
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
		newStrs := []string{}
		for _, str := range strings.Split(str, ".") {
			newStrs = append(newStrs, str)
		}
		return strings.Join(newStrs, ".")
	}

	return str
}

func BuildMapDataAndKeys(model interface{}) (map[string]interface{}, []string) {
	var mapValue = make(map[string]interface{})
	keys := make([]string, 0)
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	modelType := modelValue.Type()
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		if colName, exist := GetColumnNameByIndex(modelType, index); exist {
			fieldValue := modelValue.Field(index).Interface()
			mapValue[colName] = fieldValue
			keys = append(keys, colName)
		} else {
			panic("cannot find column name")
		}
	}
	return mapValue, keys
}

