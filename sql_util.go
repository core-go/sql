package sql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func GetDriverName(db *sql.DB) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*godror.drv":
		return DriverOracle
	default:
		return "no support"
	}
}

func Query(db *sql.DB, results interface{}, sql string, values ...interface{}) error {
	rows, er1 := db.Query(sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	modelType := reflect.TypeOf(results).Elem().Elem()
	fieldsIndex, er0 := GetColumnIndexes(modelType)
	if er0 != nil {
		return er0
	}

	tb, er2 := Scans(rows, modelType, fieldsIndex)
	if er2 != nil {
		return er2
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}

func QueryWithType(db *sql.DB, results interface{}, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) error {
	rows, er1 := db.Query(sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	tb, er3 := Scans(rows, modelType, fieldsIndex)
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}

func QueryRow(db *sql.DB, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	strSQL := "LIMIT 1"
	if GetDriverName(db) == DriverOracle {
		strSQL = "AND ROWNUM = 1"
	}
	rows, er1 := db.Query(sql+" " +strSQL, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex)
	if er2 != nil {
		return nil, er2
	}
	er3 := rows.Close()
	if er3 != nil {
		return nil, er3
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return nil, er3
	}
	if tb == nil {
		return nil, errors.New("not found record")
	}
	return tb, nil
}

func appendToArray(arr interface{}, item interface{}) interface{} {
	arrValue := reflect.ValueOf(arr)
	elemValue := reflect.Indirect(arrValue)

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = reflect.Indirect(itemValue)
	}
	elemValue.Set(reflect.Append(elemValue, itemValue))
	return arr
}

func GetColumnIndexes(modelType reflect.Type) (map[string]int, error) {
	mapp := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return mapp, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := FindTag(ormTag, "column")
		if ok {
			mapp[column] = i
		}
	}
	return mapp, nil
}

func FindTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}

func GetColumnsSelect(modelType reflect.Type) []string {
	numField := modelType.NumField()
	columnNameKeys := make([]string, 0)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		if has := strings.Contains(ormTag, "column"); has {
			str1 := strings.Split(ormTag, ";")
			num := len(str1)
			for i := 0; i < num; i++ {
				str2 := strings.Split(str1[i], ":")
				for j := 0; j < len(str2); j++ {
					if str2[j] == "column" {
						columnName := str2[j+1]
						columnNameKeys = append(columnNameKeys, columnName)
					}
				}
			}
		}
	}
	return columnNameKeys
}

// StructScan : transfer struct to slice for scan
func StructScan(s interface{}) (r []interface{}) {
	if s != nil {
		vals := reflect.ValueOf(s).Elem()
		for i := 0; i < vals.NumField(); i++ {
			r = append(r, vals.Field(i).Addr().Interface())
		}
	}

	return
}

// StructScan : transfer struct to slice for scan
func StructScanByIndex(s interface{}, fieldsIndex map[string]int, columns []string) (r []interface{}) {
	if s != nil {
		maps := reflect.Indirect(reflect.ValueOf(s))
		fieldsIndexSelected := make([]int, 0)
		for _, columnsName := range columns {
			columnsName = strings.ToLower(columnsName)
			if index, ok := fieldsIndex[columnsName]; ok {
				fieldsIndexSelected = append(fieldsIndexSelected, index)
				r = append(r, maps.Field(index).Addr().Interface())
			} else {
				var ignore interface{}
				r = append(r, &ignore)
			}
		}
	}
	return
}

func Scans(rows *sql.Rows, modelType reflect.Type, fieldsIndex map[string]int) (t []interface{}, err error) {
	columns, er0 := rows.Columns()
	if er0 != nil {
		return nil, er0
	}
	for rows.Next() {
		initModel := reflect.New(modelType).Interface()
		if err = rows.Scan(StructScanByIndex(initModel, fieldsIndex, columns)...); err == nil {
			t = append(t, initModel)
		}
	}
	return
}

//Rows
func ScanByModelType(rows *sql.Rows, modelType reflect.Type) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(modelType).Interface()
		if err = rows.Scan(StructScan(gTb)...); err == nil {
			t = append(t, gTb)
		}
	}

	return
}

//Rows
func Scan(rows *sql.Rows, structType reflect.Type, fieldsIndex map[string]int) (t interface{}, err error) {
	columns, _ := rows.Columns()
	for rows.Next() {
		gTb := reflect.New(structType).Interface()
		if err = rows.Scan(StructScanByIndex(gTb, fieldsIndex, columns)...); err == nil {
			t = gTb
			break
		}
	}
	return
}

//Row
func ScanRow(row *sql.Row, structType reflect.Type) (t interface{}, err error) {
	t = reflect.New(structType).Interface()
	err = row.Scan(StructScan(t)...)
	return
}
