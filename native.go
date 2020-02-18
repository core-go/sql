package sql

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"strings"
)

// raw query
func RawInsert(db *gorm.DB, table string, model interface{}) (int64, error) {
	queryInsert, values := BuildInsertQuery(db, table, model)

	fmt.Printf("queryInsert: %v\n", queryInsert)
	result := db.Exec(queryInsert, values...)
	return HandleResult(result)
}

func RawUpdate(db *gorm.DB, table string, model interface{}, query map[string]interface{}) (int64, error) {
	queryUpdate, values := BuildUpdateQuery(db, table, model, query)

	fmt.Printf("queryUpdate: %v\n", queryUpdate)
	result := db.Exec(queryUpdate, values...)
	return HandleResult(result)
}

func RawDelete(db *gorm.DB, table string, query map[string]interface{}) (int64, error) {
	queryDelete, values := BuildDeleteQuery(db, table, query)

	fmt.Printf("queryUpdate: %v\n", queryDelete)
	result := db.Exec(queryDelete, values...)
	return HandleResult(result)
}

func BuildMapData(model interface{}) map[string]interface{} {
	var mapValue = make(map[string]interface{})
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	modelType := modelValue.Type()
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		if colName, exist := GetColumnNameByIndex(modelType, index); exist {
			fieldvalue := modelValue.Field(index).Interface()
			mapValue[colName] = fieldvalue
		} else {
			panic("cannot find column name")
		}
	}
	return mapValue
}

func BuildInsertQuery(db *gorm.DB, table string, model interface{}) (string, []interface{}) {
	mapData := BuildMapData(model)
	mainscope := db.NewScope(model)
	var cols []string
	var values []interface{}
	for col, v := range mapData {
		cols = append(cols, mainscope.Quote(col))
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

func BuildUpdateQuery(db *gorm.DB, table string, model interface{}, query map[string]interface{}) (string, []interface{}) {
	mapData := BuildMapData(model)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	idFields := FindIdFields(modelType)
	mainscope := db.NewScope(model)
	for _, idField := range idFields {
		if idCol, exist := GetColumnName(modelType, idField); exist {
			delete(mapData, idCol)
		}
	}
	var values []interface{}
	for _, v := range mapData {
		values = append(values, v)
	}
	var updateQuery []string
	for col := range mapData {
		updateQuery = append(updateQuery, fmt.Sprintf("%v=?", mainscope.Quote(col)))
	}
	updatedValue := strings.Join(updateQuery, ",")
	var queryArr []string
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", key))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("UPDATE %v SET %v WHERE %v", table, updatedValue, q), values
}

func BuildDeleteQuery(db *gorm.DB, table string, query map[string]interface{}) (string, []interface{}) {
	var values []interface{}
	var queryArr []string
	mainscope := db.NewScope("")
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", mainscope.Quote(key)))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("DELETE FROM %v WHERE %v", table, q), values
}

func HandleResult(result *gorm.DB) (int64, error) {
	if err := result.Error; err != nil {
		return result.RowsAffected, err
	} else {
		return result.RowsAffected, nil
	}
}

func GetColumnNameByIndex(ModelType reflect.Type, index int) (col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}
