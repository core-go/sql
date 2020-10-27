package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Statement struct {
	Query  string
	Values []interface{}
}

var formatDateUpdate = "2006-01-02 15:04:05"

func InsertMany(db *sql.DB, tableName string, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := insertObjSetSQL(db, tableName, objSet, false, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

// Separate objects into several size
func splitObjects(objArr []interface{}, size int) [][]interface{} {
	var chunkSet [][]interface{}
	var chunk []interface{}

	for len(objArr) > size {
		chunk, objArr = objArr[:size], objArr[size:]
		chunkSet = append(chunkSet, chunk)
	}
	if len(objArr) > 0 {
		chunkSet = append(chunkSet, objArr[:])
	}

	return chunkSet
}

func RawInsertManySkipErrors(db *sql.DB, tableName string, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := insertObjSetSQL(db, tableName, objSet, true, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func insertObjSetSQL(db *sql.DB, tableName string, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}

	firstAttrs, err := ExtractMapValue(objects[0], excludeColumns)
	if err != nil {
		return 0, err
	}

	attrSize := len(firstAttrs)
	modelType := reflect.TypeOf(objects[0])
	pkey := FindIdFields(modelType)
	// Scope to eventually run SQL
	mainScope := Statement{}
	// Store placeholders for embedding variables
	placeholders := make([]string, 0, attrSize)

	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, QuoteColumnName(key))
	}

	for _, obj := range objects {
		objAttrs, err := ExtractMapValue(obj, excludeColumns)
		if err != nil {
			return 0, err
		}

		// If object sizes are different, SQL statement loses consistency
		if len(objAttrs) != attrSize {
			return 0, errors.New("attribute sizes are inconsistent")
		}

		scope := Statement{}

		// Append variables
		variables := make([]string, 0, attrSize)
		for _, key := range SortedKeys(objAttrs) {
			scope.Values = append(scope.Values, objAttrs[key])
			variables = append(variables, "?")
		}

		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)

		// Also append variables to mainScope
		mainScope.Values = append(mainScope.Values, scope.Values...)
	}
	var query string
	driver := reflect.TypeOf(db.Driver()).String()
	if skipDuplicate {
		if driver == "*postgres.Driver" {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
				tableName,
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
			)

		} else if driver == "*mysql.MySQLDriver" {
			var qKey []string
			for _, i2 := range pkey {
				key := i2 + " = " + i2
				qKey = append(qKey, key)
			}
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
				tableName,
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(qKey, ", "),
			)
		} else {
			return 0, fmt.Errorf("only support skip duplicate on mysql and postgresql, current vendor is %s", driver)
		}
	}
	{
		query = fmt.Sprintf(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			tableName,
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
		))
	}
	mainScope.Query = query

	x, err := db.Exec(mainScope.Query, mainScope.Values...)
	fmt.Println(err)
	return x.RowsAffected()
}

func InterfaceSlice(slice interface{}) ([]interface{}, error) {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("InterfaceSlice() given a non-slice type")
	}
	ret := make([]interface{}, s.Len())

	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}
	return ret, nil
}

func UpdateMany(db *sql.DB, tableName string, objects []interface{}) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	var placeholder []string
	firstAttrs, err := ExtractMapValue(objects[0], placeholder)
	if err != nil {
		return 0, err
	}
	modelType := reflect.TypeOf(objects[0])
	objAttrs := make(map[string]interface{})
	pkey := FindIdFields(modelType)
	mainScope := Statement{}
	query := ""
	dbColumns := make([]string, 0)
	driver := reflect.TypeOf(db.Driver()).String()
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, QuoteByDriver(key, driver))
	}

	for _, obj := range objects {
		var keyValues string
		sets := ""
		var fields []string
		objAttrs, err = ExtractMapValue(obj, placeholder)
		if err != nil {
			return 0, err
		}
		var keys []string

		for _, key := range SortedKeys(objAttrs) {
			for _, i2 := range pkey {
				if strings.ToLower(i2) == strings.ToLower(key) {
					if _, ok := objAttrs[key]; !ok {
						return 0, errors.New("could not find field")
					}
					switch v := objAttrs[key].(type) {
					case int:
						where := QuoteByDriver(key, driver) + " = " + strconv.Itoa(v)
						keys = append(keys, where)
					case float64:
						where := QuoteByDriver(key, driver) + " = " + strconv.FormatFloat(v, 'f', -1, 64)
						keys = append(keys, where)
					case bool:
						where := QuoteByDriver(key, driver) + " = " + strconv.FormatBool(v)
						keys = append(keys, where)
					case time.Time:
						where := QuoteByDriver(key, driver) + " = " + v.Format(formatDateUpdate)
						keys = append(keys, where)
					case *time.Time:
						where := QuoteByDriver(key, driver) + " = " + v.Format(formatDateUpdate)
						keys = append(keys, where)
					case string:
						if driver == "*postgres.Driver" {
							where := QuoteByDriver(key, driver) + " = " + `E'` + EscapeString(v) + `'`
							keys = append(keys, where)
							break
						} else if driver == "*mssql.Driver" {
							where := QuoteByDriver(key, driver) + " = " + `'` + EscapeStringForSelect(v) + `'`
							keys = append(keys, where)
							break
						}
						where := QuoteByDriver(key, driver) + " = " + `'` + EscapeString(v) + `'`
						keys = append(keys, where)
					default:
						return 0, errors.New("unsupported type")
					}
				}
			}
		}
		for _, i2 := range dbColumns {
			s := i2
			//s = s[1 : len(s)-1]
			if _, ok := objAttrs[s]; !ok {
				return 0, errors.New("could not find field")
			}
			var d = objAttrs[s]
			fmt.Print(reflect.TypeOf(d))
			switch v := objAttrs[s].(type) {
			case int, *int:
				v = reflect.Indirect(reflect.ValueOf(v)).Interface()
				if c, ok := v.(int); ok {
					where := strconv.Itoa(c)
					fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
				}
			case float64, *float64:
				v = reflect.Indirect(reflect.ValueOf(v)).Interface()
				if c, ok := v.(float64); ok {
					where := strconv.FormatFloat(c, 'f', -1, 64)
					fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
				}
			case bool, *bool:
				v = reflect.Indirect(reflect.ValueOf(v)).Interface()
				if c, ok := v.(bool); ok {
					where := strconv.FormatBool(c)
					fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
				}
			case string, *string:
				//v = `'` + v + `'`
				v = reflect.Indirect(reflect.ValueOf(v)).Interface()
				if c, ok := v.(string); ok {
					if driver == "*postgres.Driver" {
						fields = append(fields, fmt.Sprintf("%s = E'%s'", i2, EscapeString(c)))
						break
					} else if driver == "*mssql.Driver" {
						fields = append(fields, fmt.Sprintf("%s = '%s'", i2, EscapeStringForSelect(c)))
						break
					}
					fields = append(fields, fmt.Sprintf("%s = '%s'", i2, EscapeString(c)))
				}
				break
			case time.Time, *time.Time:
				v = reflect.Indirect(reflect.ValueOf(v)).Interface()
				if c, ok := v.(time.Time); ok {
					fields = append(fields, fmt.Sprintf("%s = '%s'", i2, EscapeString(c.Format(formatDateUpdate))))
				}
			default:
				return 0, errors.New("unsupported type")
			}
			sets = strings.Join(fields, ", ")
		}
		keyValues = keyValues + strings.Join(keys, " and ")
		query = query + fmt.Sprintf("UPDATE %s SET %s WHERE %s; ", tableName, sets, keyValues)
	}
	//UPDATE users SET name='hello', age=18 WHERE id IN (10, 11)

	//query := fmt.Sprintf("update %s IN (%s)", strings.Join(pkey, ", "))
	//db.Table(tableName).Where(query, ).Updates(objAttrs)
	mainScope.Query = query
	//fmt.Print(fmt.Sprintf("mainScope.SQL:  %s", mainScope.SQL))
	x, err := db.Exec(mainScope.Query)
	fmt.Println(err)
	return x.RowsAffected()
}
