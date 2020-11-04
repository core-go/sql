package sql

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func Query(db *sql.DB, results interface{}, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) error {
	rows, err1 := db.Query(sql, values...)
	if err1 != nil {
		return err1
	}
	defer rows.Close()
	if fieldsIndex == nil {
		tb, err2 := ScanByModelType(rows, modelType)
		if err2 != nil {
			return err2
		}
		for _, element := range tb {
			appendToArray(results, element)
		}
	} else {
		tb, err2 := Scans(rows, modelType, fieldsIndex)
		if err2 != nil {
			return err2
		}
		for _, element := range tb {
			appendToArray(results, element)
		}
	}
	rerr := rows.Close()
	if rerr != nil {
		return rerr
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func QueryOne(db *sql.DB, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	rows, err1 := db.Query(sql+" LIMIT 1", values...)
	if err1 != nil {
		return nil, err1
	}
	tb, err2 := Scan(rows, modelType, fieldsIndex)
	if err2 != nil {
		return nil, err2
	}
	rerr := rows.Close()
	if rerr != nil {
		return nil, rerr
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return nil, rerr
	}
	if tb == nil {
		return nil, errors.New("Not found record.")
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
	columns, _ := rows.Columns()
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
