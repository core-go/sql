package sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func Count(ctx context.Context, db *sql.DB, sql string, values ...interface{}) (int64, error) {
	var total int64
	row := db.QueryRowContext(ctx, sql, values...)
	err2 := row.Scan(&total)
	if err2 != nil {
		return total, err2
	}
	return total, nil
}
func QueryMapWithTx(ctx context.Context, db *sql.Tx, transform func(s string) string, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return nil, er1
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	colMaps := make([]string, len(cols))
	if transform != nil {
		for i, colName := range cols {
			colMaps[i] = transform(colName)
		}
	} else {
		for i, colName := range cols {
			colMaps[i] = colName
		}
	}
	res := make([]map[string]interface{}, 0)
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}
		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			x := *val
			switch s := x.(type) {
			case *[]byte:
				x2 := *s
				s2 := string(x2)
				m[colMaps[i]] = s2
			case []byte:
				s2 := string(s)
				m[colMaps[i]] = s2
			default:
				m[colMaps[i]] = *val
			}
		}
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		res = append(res, m)
	}
	return res, nil
}
func QueryMap(ctx context.Context, db *sql.DB, transform func(s string) string, sql string, values ...interface{}) ([]map[string]interface{}, error) {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return nil, er1
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	colMaps := make([]string, len(cols))
	if transform != nil {
		for i, colName := range cols {
			colMaps[i] = transform(colName)
		}
	} else {
		for i, colName := range cols {
			colMaps[i] = colName
		}
	}
	res := make([]map[string]interface{}, 0)
	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}
		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}
		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, _ := range cols {
			val := columnPointers[i].(*interface{})
			x := *val
			switch s := x.(type) {
			case *[]byte:
				x2 := *s
				s2 := string(x2)
				m[colMaps[i]] = s2
			case []byte:
				s2 := string(s)
				m[colMaps[i]] = s2
			default:
				m[colMaps[i]] = *val
			}
		}
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		res = append(res, m)
	}
	return res, nil
}
func Query(ctx context.Context, db *sql.DB, results interface{}, sql string, values ...interface{}) error {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	modelType := reflect.TypeOf(results).Elem().Elem()

	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

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
func QueryTx(ctx context.Context, tx *sql.Tx, results interface{}, sql string, values ...interface{}) error {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()

	modelType := reflect.TypeOf(results).Elem().Elem()
	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

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
func QueryByStatement(ctx context.Context, stm *sql.Stmt, results interface{}, values ...interface{}) error {
	rows, er1 := stm.QueryContext(ctx, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()

	modelType := reflect.TypeOf(results).Elem().Elem()
	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

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
func QueryAndCount(ctx context.Context, db *sql.DB, results interface{}, count *int64, sql string, values ...interface{}) error {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	modelType := reflect.TypeOf(results).Elem().Elem()

	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

	tb, c, er3 := ScansAndCount(rows, modelType, fieldsIndex)
	*count = c
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
func QueryRow(ctx context.Context, db *sql.DB, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	strSQL := "limit 1"
	driver := GetDriver(db)
	if driver == DriverOracle {
		strSQL = "AND ROWNUM = 1"
	}
	s := sql + " " + strSQL
	rows, er1 := db.QueryContext(ctx, s, values...)
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
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
	}
	return tb, nil
}
func QueryRowTx(ctx context.Context, tx *sql.Tx, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
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
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
	}
	return tb, nil
}
func QueryRowByStatement(ctx context.Context, stm *sql.Stmt, modelType reflect.Type, fieldsIndex map[string]int, values ...interface{}) (interface{}, error) {
	rows, er1 := stm.QueryContext(ctx, values...)
	// rows, er1 := db.Query(s, values...)
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
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
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
	ma := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return ma, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := FindTag(ormTag, "column")
		column = strings.ToLower(column)
		if ok {
			ma[column] = i
		}
	}
	return ma, nil
}

func GetIndexesByTagJson(modelType reflect.Type) (map[string]int, error) {
	ma := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return ma, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tagJson := field.Tag.Get("json")
		if len(tagJson) > 0 {
			ma[tagJson] = i
		}
	}
	return ma, nil
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
func GetColumnNameForSearch(modelType reflect.Type, sortField string) string {
	sortField = strings.TrimSpace(sortField)
	i, _, column := GetFieldByJson(modelType, sortField)
	if i > -1 {
		return column
	}
	return sortField // injection
}
func GetSortType(sortType string) string {
	if sortType == "-" {
		return desc
	} else {
		return asc
	}
}
func GetColumns(cols []string, err error) ([]string, error) {
	if cols == nil || err != nil {
		return cols, err
	}
	c2 := make([]string, 0)
	for _, c := range cols {
		s := strings.ToLower(c)
		c2 = append(c2, s)
	}
	return c2, nil
}
func Scans(rows *sql.Rows, modelType reflect.Type, fieldsIndex map[string]int) (t []interface{}, err error) {
	columns, er0 := GetColumns(rows.Columns())
	if er0 != nil {
		return nil, er0
	}
	for rows.Next() {
		initModel := reflect.New(modelType).Interface()
		r, swapValues := StructScan(initModel, columns, fieldsIndex, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(initModel, &swapValues)
			t = append(t, initModel)
		}
	}
	return
}
func StructScan(s interface{}, columns []string, fieldsIndex map[string]int, indexIgnore int) (r []interface{}, swapValues map[int]interface{}) {
	if s != nil {
		modelType := reflect.TypeOf(s).Elem()
		swapValues = make(map[int]interface{}, 0)
		maps := reflect.Indirect(reflect.ValueOf(s))

		if columns == nil {
			for i := 0; i < maps.NumField(); i++ {
				tagBool := modelType.Field(i).Tag.Get("true")
				if tagBool == "" {
					r = append(r, maps.Field(i).Addr().Interface())
				} else {
					var str string
					swapValues[i] = reflect.New(reflect.TypeOf(str)).Elem().Addr().Interface()
					r = append(r, swapValues[i])
				}
			}
			return
		}

		for i, columnsName := range columns {
			if i == indexIgnore {
				continue
			}
			var index int
			var ok bool
			var modelField reflect.StructField
			var valueField reflect.Value
			if fieldsIndex == nil {
				if modelField, ok = modelType.FieldByName(columnsName); !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				valueField = maps.FieldByName(columnsName)
			} else {
				if index, ok = fieldsIndex[columnsName]; !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				modelField = modelType.Field(index)
				valueField = maps.Field(index)
			}
			x := valueField.Addr().Interface()
			tagBool := modelField.Tag.Get("true")
			if tagBool == "" {
				r = append(r, x)
			} else {
				var str string
				y := reflect.New(reflect.TypeOf(str))
				swapValues[index] = y.Elem().Addr().Interface()
				r = append(r, swapValues[index])
			}
		}
	}
	return
}
func SwapValuesToBool(s interface{}, swap *map[int]interface{}) {
	if s != nil {
		modelType := reflect.TypeOf(s).Elem()
		maps := reflect.Indirect(reflect.ValueOf(s))
		for index, element := range *swap {
			dbValue2, ok2 := element.(*bool)
			if ok2 {
				if maps.Field(index).Kind() == reflect.Ptr {
					maps.Field(index).Set(reflect.ValueOf(dbValue2))
				} else {
					maps.Field(index).SetBool(*dbValue2)
				}
			} else {
				dbValue, ok := element.(*string)
				if ok {
					var isBool bool
					if *dbValue == "true" {
						isBool = true
					} else if *dbValue == "false" {
						isBool = false
					} else {
						boolStr := modelType.Field(index).Tag.Get("true")
						isBool = *dbValue == boolStr
					}
					if maps.Field(index).Kind() == reflect.Ptr {
						maps.Field(index).Set(reflect.ValueOf(&isBool))
					} else {
						maps.Field(index).SetBool(isBool)
					}
				}
			}
		}
	}
}
func ScansAndCount(rows *sql.Rows, modelType reflect.Type, fieldsIndex map[string]int) ([]interface{}, int64, error) {
	var t []interface{}
	columns, er0 := GetColumns(rows.Columns())
	if er0 != nil {
		return nil, 0, er0
	}
	var count int64
	for rows.Next() {
		initModel := reflect.New(modelType).Interface()
		var c []interface{}
		c = append(c, &count)
		r, swapValues := StructScan(initModel, columns, fieldsIndex, 0)
		c = append(c, r...)
		if err := rows.Scan(c...); err == nil {
			SwapValuesToBool(initModel, &swapValues)
			t = append(t, initModel)
		}
	}
	return t, count, nil
}

func ScanByModelType(rows *sql.Rows, modelType reflect.Type) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(modelType).Interface()
		r, swapValues := StructScan(gTb, nil, nil, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(gTb, &swapValues)
			t = append(t, gTb)
		}
	}

	return
}

func Scan(rows *sql.Rows, structType reflect.Type, fieldsIndex map[string]int) (t interface{}, err error) {
	columns, er0 := GetColumns(rows.Columns())
	err = er0
	if err != nil {
		return
	}
	for rows.Next() {
		gTb := reflect.New(structType).Interface()
		r, swapValues := StructScan(gTb, columns, fieldsIndex, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(gTb, &swapValues)
			t = gTb
			break
		}
	}
	return
}

//Row
func ScanRow(row *sql.Row, structType reflect.Type) (t interface{}, err error) {
	t = reflect.New(structType).Interface()
	r, swapValues := StructScan(t, nil, nil, -1)
	err = row.Scan(r...)
	SwapValuesToBool(t, &swapValues)
	return
}
func ToCamelCase(s string) string {
	s2 := strings.ToLower(s)
	s1 := string(s2[0])
	for i := 1; i < len(s); i++ {
		if string(s2[i-1]) == "_" {
			s1 = s1[:len(s1)-1]
			s1 += strings.ToUpper(string(s2[i]))
		} else {
			s1 += string(s2[i])
		}
	}
	return s1
}

type Proxy interface {
	BeginTransaction(ctx context.Context, timeout int64) (string, error)
	CommitTransaction(ctx context.Context, tx string) error
	RollbackTransaction(ctx context.Context, tx string) error
	Exec(ctx context.Context, query string, values ...interface{}) (int64, error)
	ExecBatch(ctx context.Context, master bool, stm ...Statement) (int64, error)
	Query(ctx context.Context, result interface{}, query string, values ...interface{}) error
	ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error)
	ExecBatchWithTx(ctx context.Context, tx string, commit bool, master bool, stm ...Statement) (int64, error)
	QueryWithTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error

	Insert(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	Update(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	Save(ctx context.Context, table string, model interface{}, driver string, options...*Schema) (int64, error)
	InsertBatch(ctx context.Context, table string, models interface{}, driver string, options...*Schema) (int64, error)
	UpdateBatch(ctx context.Context, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	SaveBatch(ctx context.Context, table string, models interface{}, driver string, options...*Schema) (int64, error)
	InsertWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	UpdateWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	SaveWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, driver string, options...*Schema) (int64, error)
	InsertBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...*Schema) (int64, error)
	UpdateBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	SaveBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...*Schema) (int64, error)

	InsertAndCommit(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	UpdateAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, buildParam func(int) string, boolSupport bool, options...*Schema) (int64, error)
	SaveAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, options...*Schema) (int64, error)
	InsertBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options...*Schema) (int64, error)
	UpdateBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*Schema)
	SaveBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options...*Schema) (int64, error)
}
