package sql

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"strconv"
	"strings"
)

// Insert multiple records at once
// [objects]        Must be a slice of struct
// [chunkSize]      Number of records to insert at once.
//                  Embedding a large number of variables at once will raise an error beyond the limit of prepared statement.
//                  Larger size will normally lead the better performance, but 2000 to 3000 is reasonable.
// [excludeColumns] Columns you want to exclude from insert. You can omit if there is no column you want to exclude.
func InsertMany(db *gorm.DB, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := insertObjSet(db, objSet, false, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func InsertManySkipErrors(db *gorm.DB, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := insertObjSet(db, objSet, true, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func insertObjSet(db *gorm.DB, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
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
	mainScope := db.NewScope(objects[0])
	// Store placeholders for embedding variables
	placeholders := make([]string, 0, attrSize)

	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, mainScope.Quote(key))
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

		scope := db.NewScope(obj)

		// Append variables
		variables := make([]string, 0, attrSize)
		for _, key := range SortedKeys(objAttrs) {
			scope.AddToVars(objAttrs[key])
			variables = append(variables, "?")
		}

		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)

		// Also append variables to mainScope
		mainScope.SQLVars = append(mainScope.SQLVars, scope.SQLVars...)
	}
	var query string
	if skipDuplicate {
		if db.Dialect().GetName() == "postgres" {
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
				mainScope.QuotedTableName(),
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
			)

		} else if db.Dialect().GetName() == "mysql" {
			var qKey []string
			for _, i2 := range pkey {
				key := i2 + " = " + i2
				qKey = append(qKey, key)
			}
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
				mainScope.QuotedTableName(),
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(qKey, ", "),
			)
		} else {
			return 0, fmt.Errorf("only support skip duplicate on mysql and postgresql, current vendor is %s", db.Dialect().GetName())
		}
	}
	{
		query = fmt.Sprintf(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			mainScope.QuotedTableName(),
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
		))
	}
	mainScope.Raw(query)

	x := db.Exec(mainScope.SQL, mainScope.SQLVars...)
	return x.RowsAffected, x.Error
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

func UpdateMany(db *gorm.DB, tableName string, objects []interface{}) (int64, error) {
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
	mainScope := db.NewScope(objects[0])
	query := ""
	dbColumns := make([]string, 0)
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, mainScope.Quote(key))
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
						where := mainScope.Quote(key) + " = " + strconv.Itoa(v)
						keys = append(keys, where)
					case float64:
						where := mainScope.Quote(key) + " = " + strconv.FormatFloat(v, 'f', -1, 64)
						keys = append(keys, where)
					case bool:
						where := mainScope.Quote(key) + " = " + strconv.FormatBool(v)
						keys = append(keys, where)
					case string:
						if db.Dialect().GetName() == "postgres" {
							where := mainScope.Quote(key) + " = " + `E'` + EscapeString(v) + `'`
							keys = append(keys, where)
							break
						} else if db.Dialect().GetName() == "mssql" {
							where := mainScope.Quote(key) + " = " + `'` + EscapeStringForSelect(v) + `'`
							keys = append(keys, where)
							break
						}
						where := mainScope.Quote(key) + " = " + `'` + EscapeString(v) + `'`
						keys = append(keys, where)
					default:
						return 0, errors.New("unsupported type")
					}
				}
			}
		}
		for _, i2 := range dbColumns {
			s := i2
			s = s[1 : len(s)-1]
			if _, ok := objAttrs[s]; !ok {
				return 0, errors.New("could not find field")
			}
			switch v := objAttrs[s].(type) {
			case int:
				where := strconv.Itoa(v)
				fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
			case float64:
				where := strconv.FormatFloat(v, 'f', -1, 64)
				fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
			case bool:
				where := strconv.FormatBool(v)
				fields = append(fields, fmt.Sprintf("%s = %s", i2, where))
			case string:
				//v = `'` + v + `'`
				if db.Dialect().GetName() == "postgres" {
					fields = append(fields, fmt.Sprintf("%s = E'%s'", i2, EscapeString(v)))
					break
				} else if db.Dialect().GetName() == "mssql" {
					fields = append(fields, fmt.Sprintf("%s = '%s'", i2, EscapeStringForSelect(v)))
					break
				}
				fields = append(fields, fmt.Sprintf("%s = '%s'", i2, EscapeString(v)))
			default:
				return 0, errors.New("unsupported type")
			}
			sets = strings.Join(fields, ", ")
		}
		keyValues = keyValues + strings.Join(keys, " and ")
		query = query + fmt.Sprintf("update %s set %s where %s; ", tableName, sets, keyValues)
	}
	//UPDATE users SET name='hello', age=18 WHERE id IN (10, 11)

	//query := fmt.Sprintf("update %s IN (%s)", strings.Join(pkey, ", "))
	//db.Table(tableName).Where(query, ).Updates(objAttrs)
	mainScope.Raw(query)
	x := db.Exec(mainScope.SQL)
	return x.RowsAffected, x.Error
}
