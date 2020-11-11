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
	driver := GetDriverName(db)
	tx, er0 := db.Begin()
	if er0 != nil {
		return 0, er0
	}
	result, er1 := db.Exec(sts[0].Sql, sts[0].Args...)
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
		r2, er3 := db.Exec(sts[i].Sql, sts[i].Args...)
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
		count, err := InsertObjSetSQL(db, tableName, objSet, false, excludeColumns...)
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
		count, err := TransactionInsertObjSetSQL(db, tableName, objSet, false, excludeColumns...)
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
		count, err := InsertObjSetSQL(db, tableName, objSet, true, excludeColumns...)
		c = c + count
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func InsertObjSetSQL(db *sql.DB, tableName string, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	driverName := GetDriverName(db)
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
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
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
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
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

func TransactionInsertObjSetSQL(db *sql.DB, tableName string, objects []interface{}, skipDuplicate bool, excludeColumns ...string) (int64, error) {
	if len(objects) == 0 {
		return 0, nil
	}
	driverName := GetDriverName(db)
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
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
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
			query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
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
		query = fmt.Sprintf(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			tableName,
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
		))
	}
	mainScope.Query = query

	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}

	x, execErr := db.Exec(mainScope.Query, mainScope.Values...)
	if execErr != nil {
		_ = tx.Rollback()
		fmt.Println(execErr)
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("sql: transaction has already been rolled back")
		return 0, err
	}
	count, _ := x.RowsAffected()
	fmt.Println(count, " Rows Updated")
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
	var placeholder []string
	driverName := GetDriverName(db)
	var query []string
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
		sets, err1 := BuildSqlParametersByColumns(scope.Columns, scope.Values, n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		columnsKeys := scope.Keys
		where, err2 := BuildSqlParametersByColumns(columnsKeys, scope.Values, len(columnsKeys), n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		query = append(query, fmt.Sprintf(fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			sets,
			where,
		)))
	}

	statement.Query = strings.Join(query, "; ")
	x, err := db.Exec(statement.Query) // return just one success query
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func TransactionUpdateMany(db *sql.DB, tableName string, objects []interface{}) (int64, error) {
	var placeholder []string
	driverName := GetDriverName(db)
	var query []string
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
		sets, err1 := BuildSqlParametersByColumns(scope.Columns, scope.Values, n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		columnsKeys := scope.Keys
		where, err2 := BuildSqlParametersByColumns(columnsKeys, scope.Values, len(columnsKeys), n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		query = append(query, fmt.Sprintf(fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			sets,
			where,
		)))
	}

	statement.Query = strings.Join(query, "; ")

	tx, err := db.Begin()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	x, execErr := db.Exec(statement.Query)
	if execErr != nil {
		_ = tx.Rollback()
		fmt.Println(execErr)
	}
	err = tx.Commit()
	if err != nil {
		fmt.Println("sql: transaction has already been rolled back")
		return 0, err
	}
	_, err = x.RowsAffected()
	total := int64(len(query))
	fmt.Println(total, " Rows Inserted")
	return total, err
}

func PatchMaps(db *sql.DB, tableName string, objects []map[string]interface{}, idTagJsonNames []string, idColumNames []string) (int64, error) {
	driverName := GetDriverName(db)
	var query []string
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
		sets, err1 := BuildSqlParametersByColumns(scope.Columns, scope.Values, n, 0, driverName, ", ")
		if err1 != nil {
			return 0, err1
		}
		columnsKeys := scope.Keys
		where, err2 := BuildSqlParametersByColumns(columnsKeys, scope.Values, len(columnsKeys), n, driverName, " and ")
		if err2 != nil {
			return 0, err2
		}
		query = append(query, fmt.Sprintf("UPDATE %s SET %s WHERE %s",
			tableName,
			sets,
			where,
		))
	}

	sql := strings.Join(query, "; ")
	x, err := db.Exec(sql)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func getValueColumn(value interface{}, driverName string) (string, error) {
	str := ""
	switch v := value.(type) {
	case int:
		str = strconv.Itoa(v)
	case float64:
		str = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		str = strconv.FormatBool(v)
	case time.Time:
		str = v.Format(formatDateUpdate)
	case *time.Time:
		str = v.Format(formatDateUpdate)
	case string:
		if driverName == DriverPostgres {
			str = `E'` + EscapeString(v) + `'`
		} else if driverName == DriverMssql {
			str = `'` + EscapeStringForSelect(v) + `'`
		} else {
			str = `'` + EscapeString(v) + `'`
		}
	default:
		return "", errors.New("unsupported type")
	}
	return str, nil
}

func BuildSqlParametersByColumns(columns []string, values []interface{}, n int, start int, driverName string, joinStr string) (string, error) {
	arr := make([]string, n)
	j := start
	for i, _ := range arr {
		columnName := columns[i]
		value, err := getValueColumn(values[j], driverName)
		if err == nil {
			arr[i] = fmt.Sprintf("%s = %s", columnName, value)
		} else {
			return "", err
		}
		j++
	}
	return strings.Join(arr, joinStr), nil
}
