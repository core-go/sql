package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var formatDateUpdate = "2006-01-02 15:04:05"

func ExecuteStatements(ctx context.Context, db *sql.DB, sts []Statement) (int64, error) {
	return ExecuteBatch(ctx, db, sts, true, false)
}
func ExecuteAll(ctx context.Context, db *sql.DB, sts []Statement) (int64, error) {
	return ExecuteBatch(ctx, db, sts, false, true)
}
func ExecuteBatch(ctx context.Context, db *sql.DB, sts []Statement, firstRowSuccess bool, countAll bool) (int64, error) {
	if sts == nil || len(sts) == 0 {
		return 0, nil
	}
	driver := GetDriver(db)
	tx, er0 := db.Begin()
	if er0 != nil {
		return 0, er0
	}
	result, er1 := tx.Exec(sts[0].Sql, sts[0].Args...)
	if er1 != nil {
		_ = tx.Rollback()
		str := er1.Error()
		if driver == DriverPostgres && strings.Contains(str, "pq: duplicate key value violates unique constraint") {
			return 0, nil //pq: duplicate key value violates unique constraint "aa_pkey"
		} else if driver == DriverMysql && strings.Contains(str, "Error 1062: Duplicate entry") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverOracle && strings.Contains(str, "ORA-00001: unique constraint") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverMssql && strings.Contains(str, "Violation of PRIMARY KEY constraint") {
			return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else {
			return 0, er1
		}
	}
	rowAffected, er2 := result.RowsAffected()
	if er2 != nil {
		tx.Rollback()
		return 0, er2
	}
	if firstRowSuccess {
		if rowAffected == 0 {
			return 0, nil
		}
	}
	count := rowAffected
	for i := 1; i < len(sts); i++ {
		r2, er3 := tx.Exec(sts[i].Sql, sts[i].Args...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	if er6 != nil {
		return count, er6
	}
	if countAll {
		return count, nil
	}
	return 1, nil
}

func InsertMany(db *sql.DB, tableName string, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := InsertManyRaw(db, tableName, objSet, false, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func TransactionInsertMany(db *sql.DB, tableName string, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := InsertInTransaction(db, tableName, objSet, false, excludeColumns...)
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

func InsertManySkipErrors(db *sql.DB, tableName string, objects []interface{}, chunkSize int, excludeColumns ...string) (int64, error) {
	// Split records with specified size not to exceed Database parameter limit
	if chunkSize <= 0 {
		chunkSize = len(objects)
	}
	var c int64 = 0
	for _, objSet := range splitObjects(objects, chunkSize) {
		count, err := InsertManyRaw(db, tableName, objSet, true, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func InsertManyRaw(db *sql.DB, tableName string, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	driverName := GetDriver(db)
	firstAttrs, _, err := ExtractMapValue(objects[0], excludeColumns)
	if err != nil {
		return 0, err
	}

	attrSize := len(firstAttrs)
	modelType := reflect.TypeOf(objects[0])
	pkey := FindIdFields(modelType)
	// Scope to eventually run SQL
	mainScope := BatchStatement{}
	// Store placeholders for embedding variables
	placeholders := make([]string, 0, attrSize)

	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, QuoteColumnName(key))
	}

	for _, obj := range objects {
		objAttrs, _, err := ExtractMapValue(obj, excludeColumns)
		if err != nil {
			return 0, err
		}

		// If object sizes are different, SQL statement loses consistency
		if len(objAttrs) != attrSize {
			return 0, errors.New("attribute sizes are inconsistent")
		}

		scope := BatchStatement{}

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
	if skipDuplicate {
		if driverName == DriverPostgres {
			query = fmt.Sprintf("insert into %s (%s) values %s on conflict do nothing",
				tableName,
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
			)

		} else if driverName == DriverMysql {
			var qKey []string
			for _, i2 := range pkey {
				key := i2 + " = " + i2
				qKey = append(qKey, key)
			}
			query = fmt.Sprintf("insert into %s (%s) values %s on duplicate key update %s",
				tableName,
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
				strings.Join(qKey, ", "),
			)
		} else {
			return 0, fmt.Errorf("only support skip duplicate on mysql and postgresql, current vendor is %s", driverName)
		}
	}
	{
		query = fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
			tableName,
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
		))
	}
	mainScope.Query = query

	x, err := db.Exec(mainScope.Query, mainScope.Values...)
	if err != nil {
		return -1, err
	}
	return x.RowsAffected()
}

func InsertInTransaction(db *sql.DB, tableName string, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	driver := GetDriver(db)
	firstAttrs, _, err := ExtractMapValue(objects[0], excludeColumns)
	if err != nil {
		return 0, err
	}

	attrSize := len(firstAttrs)
	modelType := reflect.TypeOf(objects[0])
	pkey := FindIdFields(modelType)
	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range SortedKeys(firstAttrs) {
		dbColumns = append(dbColumns, QuoteColumnName(key))
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}

	for _, obj := range objects {
		// Scope to eventually run SQL
		mainScope := BatchStatement{}
		// Store placeholders for embedding variables
		placeholders := make([]string, 0, attrSize)
		objAttrs, _, err := ExtractMapValue(obj, excludeColumns)
		if err != nil {
			return 0, err
		}

		// If object sizes are different, SQL statement loses consistency
		if len(objAttrs) != attrSize {
			return 0, errors.New("attribute sizes are inconsistent")
		}

		scope := BatchStatement{}

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

		var query string
		if skipDuplicate {
			if driver == DriverPostgres {
				query = fmt.Sprintf("insert into %s (%s) values %s on conflict do nothing",
					tableName,
					strings.Join(dbColumns, ", "),
					strings.Join(placeholders, ", "),
				)

			} else if driver == DriverMysql {
				var qKey []string
				for _, i2 := range pkey {
					key := i2 + " = " + i2
					qKey = append(qKey, key)
				}
				query = fmt.Sprintf("insert into %s (%s) values %s on duplicate key update %s",
					tableName,
					strings.Join(dbColumns, ", "),
					strings.Join(placeholders, ", "),
					strings.Join(qKey, ", "),
				)
			} else {
				return 0, fmt.Errorf("only support skip duplicate on mysql and postgresql, current vendor is %s", driver)
			}
		} else {
			query = fmt.Sprintf("insert into %s (%s) values %s",
				tableName,
				strings.Join(dbColumns, ", "),
				strings.Join(placeholders, ", "),
			)
		}
		query = ReplaceParameters(driver, query, len(mainScope.Values))
		mainScope.Query = query

		_, execErr := tx.Exec(mainScope.Query, mainScope.Values...)
		if execErr != nil {
			_ = tx.Rollback()
			return 0, execErr
		}
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	count := len(objects)
	return int64(count), err
}

func InterfaceSlice(slice interface{}) ([]interface{}, error) {
	s := reflect.Indirect(reflect.ValueOf(slice))
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
	var placeholder []string
	driverName := GetDriver(db)
	var query []string
	var value [][]interface{}
	if len(objects) == 0 {
		return 0, nil
	}
	valueDefault := objects[0]
	statement := newStatement(valueDefault, placeholder...)
	columns := make([]string, 0) // columns set value for update
	for _, key := range SortedKeys(statement.Attributes) {
		columns = append(columns, QuoteByDriver(key, driverName))
	}
	for _, obj := range objects {
		scope := newStatement(obj, placeholder...)
		// Append variables set column
		for _, key := range SortedKeys(scope.Attributes) {
			scope.Values = append(scope.Values, scope.Attributes[key])
		}
		// Append variables where
		for _, key := range scope.Keys {
			scope.Values = append(scope.Values, scope.AttributeKeys[key])
		}
		// Also append variables to mainScope
		//statement.Values = append(statement.Values, scope.Values...)

		n := len(scope.Columns)
		sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		value = append(value, setVal)
		numKeys := len(scope.Keys)
		where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		value = append(value, whereVal)
		query = append(query, fmt.Sprintf(fmt.Sprintf("update %s set %s where %s",
			tableName,
			sets,
			where,
		)))
	}
	var count int64
	for i := 0; i < len(query); i++ {
		x, execErr := db.Exec(query[i], value[i]...)
		if execErr != nil {
			return 0, execErr
		}
		rowsAffected, _ := x.RowsAffected()
		count += rowsAffected
	}
	return count, nil
}

func UpdateInTransaction(db *sql.DB, tableName string, objects []interface{}) (int64, error) {
	var placeholder []string
	driverName := GetDriver(db)
	var query []string
	var value [][]interface{}
	if len(objects) == 0 {
		return 0, nil
	}
	valueDefault := objects[0]
	statement := newStatement(valueDefault, placeholder...)
	columns := make([]string, 0) // columns set value for update
	for _, key := range SortedKeys(statement.Attributes) {
		columns = append(columns, QuoteByDriver(key, driverName))
	}
	for _, obj := range objects {
		scope := newStatement(obj, placeholder...)
		// Append variables set column
		for _, key := range SortedKeys(scope.Attributes) {
			scope.Values = append(scope.Values, scope.Attributes[key])
		}
		// Append variables where
		for _, key := range scope.Keys {
			scope.Values = append(scope.Values, scope.AttributeKeys[key])
		}
		// Also append variables to mainScope
		//statement.Values = append(statement.Values, scope.Values...)

		n := len(scope.Columns)
		sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		value = append(value, setVal)
		numKeys := len(scope.Keys)
		where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		value = append(value, whereVal)
		query = append(query, fmt.Sprintf(fmt.Sprintf("update %s set %s where %s",
			tableName,
			sets,
			where,
		)))
	}
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}

	for i := 0; i < len(query); i++ {
		_, execErr := tx.Exec(query[i], value[i]...)
		if execErr != nil {
			_ = tx.Rollback()
			return 0, execErr
		}
	}

	err = tx.Commit()
	if err != nil {
		return 0, err
	}
	total := int64(len(query))
	return total, err
}

func PatchMaps(db *sql.DB, tableName string, objects []map[string]interface{}, idTagJsonNames []string, idColumNames []string) (int64, error) {
	driverName := GetDriver(db)
	var query []string
	var value [][]interface{}
	if len(objects) == 0 {
		return 0, nil
	}
	for _, obj := range objects {
		scope := statement()
		// Append variables set column
		for key, _ := range obj {
			if _, ok := Find(idTagJsonNames, key); !ok {
				scope.Columns = append(scope.Columns, key)
				scope.Values = append(scope.Values, obj[key])
			}
		}
		// Append variables where
		for i, key := range idTagJsonNames {
			scope.Values = append(scope.Values, obj[key])
			scope.Keys = append(scope.Keys, idColumNames[i])
		}

		n := len(scope.Columns)
		sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		value = append(value, setVal)
		numKeys := len(scope.Keys)
		where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		value = append(value, whereVal)
		query = append(query, fmt.Sprintf("update %s set %s where %s",
			tableName,
			sets,
			where,
		))
	}

	var count int64
	for i := 0; i < len(query); i++ {
		x, execErr := db.Exec(query[i], value[i]...)
		if execErr != nil {
			return 0, execErr
		}
		rowsAffected, _ := x.RowsAffected()
		count += rowsAffected
	}
	return count, nil
}

func GetValueColumn(value interface{}, driverName string) (string, error) {
	str := ""
	switch v := value.(type) {
	case int:
		str = strconv.Itoa(v)
	case int64:
		str = strconv.Itoa(int(v))
	case float64:
		str = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		str = strconv.FormatBool(v)
	case time.Time:
		str = formatStringByDriver(v.Format(formatDateUpdate), driverName)
	case *time.Time:
		str = formatStringByDriver(v.Format(formatDateUpdate), driverName)
	case string:
		str = formatStringByDriver(v, driverName)
	default:
		return "", errors.New("unsupported type")
	}
	return str, nil
}

func formatStringByDriver(v, driverName string) string {
	if driverName == DriverPostgres {
		return `E'` + EscapeString(v) + `'`
	} else if driverName == DriverMssql {
		return `'` + EscapeStringForSelect(v) + `'`
	} else {
		return `'` + EscapeString(v) + `'`
	}
	return v
}

func BuildSqlParametersByColumns(columns []string, values []interface{}, n int, start int, driverName string, joinStr string) (string, error) {
	arr := make([]string, n)
	j := start
	for i, _ := range arr {
		columnName := columns[i]
		value, err := GetValueColumn(values[j], driverName)
		if err == nil {
			arr[i] = fmt.Sprintf("%s = %s", columnName, value)
		} else {
			return "", err
		}
		j++
	}
	return strings.Join(arr, joinStr), nil
}

func BuildSqlParametersAndValues(columns []string, values []interface{}, n *int, start int, driverName string, joinStr string) (string, []interface{}, error) {
	arr := make([]string, *n)
	j := start
	var valueParams []interface{}
	for i, _ := range arr {
		columnName := columns[i]
		if values[j] == nil {
			arr[i] = fmt.Sprintf("%s = null", columnName)
			copy(values[i:], values[i+1:])
			values[len(values)-1] = ""
			values = values[:len(values)-1]
			*n--
		} else {
			arr[i] = fmt.Sprintf("%s = %s", columnName, BuildParametersFrom(start, 1, driverName))
			valueParams = append(valueParams, values[j])
		}
		j++
	}
	return strings.Join(arr, joinStr), valueParams, nil
}
