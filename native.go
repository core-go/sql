package sql

import (
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"strings"
)

// raw query
func Save(db *gorm.DB, table string, model interface{}) (int64, error) {
	placeholders := make([]string, 0)
	attrs, _, err := ExtractMapValue(model, placeholders)
	if err != nil {
		return 0, fmt.Errorf("cannot extract object's values: %w", err)
	}
	mainScope := db.NewScope(model)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	pkey := FindIdFields(modelType)
	size := len(attrs)
	dbColumns := make([]string, 0, size)
	variables := make([]string, 0, size)
	for _, key := range SortedKeys(attrs) {
		mainScope.AddToVars(attrs[key])
		dbColumns = append(dbColumns, mainScope.Quote(key))
		variables = append(variables, "?")
	}

	valueQuery := "(" + strings.Join(variables, ", ") + ")"
	placeholders = append(placeholders, valueQuery)

	// Also append variables to mainScope
	var queryString string
	if a := db.Dialect().GetName(); a == "postgres" || a == "sqlite3" {
		uniqueCols := make([]string, 0)
		setColumns := make([]string, 0)
		for _, key := range SortedKeys(attrs) {
			for _, i2 := range pkey {
				if strings.ToLower(i2) == strings.ToLower(key) {
					uniqueCols = append(uniqueCols, mainScope.Quote(key))
				}
			}
			setColumns = append(setColumns, mainScope.Quote(key)+" = ?")
			mainScope.AddToVars(attrs[key])
		}
		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			mainScope.Quote(table),
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(uniqueCols, ", "),
			strings.Join(setColumns, ", "),
		)
	} else if db.Dialect().GetName() == "mysql" {
		var setColumns []string

		for _, key := range SortedKeys(attrs) {
			setColumns = append(setColumns, mainScope.Quote(key)+" = ?")
			mainScope.AddToVars(attrs[key])
		}
		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			mainScope.Quote(table),
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(setColumns, ", "),
		)
	} else if db.Dialect().GetName() == "mssql" {
		uniqueCols := make([]string, 0)
		setColumns := make([]string, 0)
		for _, key := range SortedKeys(attrs) {
			for _, i2 := range pkey {
				if strings.ToLower(i2) == strings.ToLower(key) {
					onDupe := table + "." + key + " = " + "temp." + key
					uniqueCols = append(uniqueCols, onDupe)
				}
			}
			mainScope.AddToVars(attrs[key])
			setColumns = append(setColumns, mainScope.Quote(key)+" = temp."+key)
		}
		queryString = fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			mainScope.Quote(table),
			strings.Join(placeholders, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", db.Dialect().GetName())
	}
	mainScope.Raw(queryString)

	x := db.Exec(mainScope.SQL, mainScope.SQLVars...)
	return x.RowsAffected, x.Error

}

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
			fieldValue := modelValue.Field(index).Interface()
			mapValue[colName] = fieldValue
		} else {
			panic("cannot find column name")
		}
	}
	return mapValue
}

func BuildInsertQuery(db *gorm.DB, table string, model interface{}) (string, []interface{}) {
	mapData := BuildMapData(model)
	mainScope := db.NewScope(model)
	var cols []string
	var values []interface{}
	for col, v := range mapData {
		cols = append(cols, mainScope.Quote(col))
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
	mainScope := db.NewScope(model)
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
		updateQuery = append(updateQuery, fmt.Sprintf("%v=?", mainScope.Quote(col)))
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
	mainScope := db.NewScope("")
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", mainScope.Quote(key)))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("DELETE FROM %v WHERE %v", table, q), values
}

func BuildInsertSQL(db *gorm.DB, tableName string, model map[string]interface{}) (string, []interface{}) {
	var cols []string
	var values []interface{}
	subScope := db.NewScope("")
	for col, v := range model {
		cols = append(cols, subScope.Quote(col))
		values = append(values, v)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	var arrValue []string
	for i := 0; i < numCol; i++ {
		arrValue = append(arrValue, "?")
	}
	value := fmt.Sprintf("(%v)", strings.Join(arrValue, ","))
	return fmt.Sprintf("INSERT INTO %v %v VALUES %v", subScope.Quote(tableName), column, value), values
}
