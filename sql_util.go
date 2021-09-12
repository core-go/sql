package sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"time"
)

func GetDriver(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*godror.drv":
		return DriverOracle
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*sqlite3.SQLiteDriver":
		return DriverSqlite3
	default:
		return DriverNotSupport
	}
}

type JStatement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
	Dates  []int         `mapstructure:"dates" json:"dates,omitempty" gorm:"column:dates" bson:"dates,omitempty" dynamodbav:"dates,omitempty" firestore:"dates,omitempty"`
}

const (
	t1 = "2006-01-02T15:04:05Z"
	t2 = "2006-01-02T15:04:05-0700"
	t3 = "2006-01-02T15:04:05.0000000-0700"

	l1 = len(t1)
	l2 = len(t2)
	l3 = len(t3)
)

func ToDates(args []interface{}) []int {
	if args == nil || len(args) == 0 {
		ag2 := make([]int, 0)
		return ag2
	}
	var dates []int
	for i, arg := range args {
		if _, ok := arg.(time.Time); ok {
			dates = append(dates, i)
		}
		if _, ok := arg.(*time.Time); ok {
			dates = append(dates, i)
		}
	}
	return dates
}

func ParseDates(args []interface{}, dates []int) []interface{} {
	if args == nil || len(args) == 0 {
		ag2 := make([]interface{}, 0)
		return ag2
	}
	if dates == nil || len(dates) == 0 {
		return args
	}
	res := append([]interface{}{}, args...)
	for _, d := range dates {
		if d >= len(args) {
			break
		}
		a := args[d]
		if s, ok := a.(string); ok {
			switch len(s) {
			case l1:
				t, err := time.Parse(t1, s)
				if err == nil {
					res[d] = t
				}
			case l2:
				t, err := time.Parse(t2, s)
				if err == nil {
					res[d] = t
				}
			case l3:
				t, err := time.Parse(t3, s)
				if err == nil {
					res[d] = t
				}
			}
		}
	}
	return res
}
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
	s1 := string(s[0])
	for i := 1; i < len(s); i++ {
		if string(s[i-1]) == "_" {
			s1 = s1[:len(s1)-1]
			s1 += strings.ToUpper(string(s2[i]))
		} else {
			s1 += string(s[i])
		}
	}
	return s1
}
func BuildStatement(query string, values ...interface{}) *JStatement {
	stm := JStatement{Query: query}
	l := len(values)
	if l > 0 {
		ag2 := make([]interface{}, 0)
		dates := make([]int, 0)
		for i := 0; i < l; i++ {
			arg := values[i]
			if _, ok := arg.(time.Time); ok {
				dates = append(dates, i)
			} else if _, ok := arg.(*time.Time); ok {
				dates = append(dates, i)
			}
			ag2 = append(ag2, values[i])
		}
		stm.Params = ag2
		if len(dates) > 0 {
			stm.Dates = dates
		}
	}
	return &stm
}
func BuildJStatements(sts ...Statement) []JStatement {
	b := make([]JStatement, 0)
	if sts == nil || len(sts) == 0 {
		return b
	}
	for _, s := range sts {
		j := JStatement{Query: s.Query}
		if s.Params != nil && len(s.Params) > 0 {
			j.Params = s.Params
			j.Dates = ToDates(s.Params)
		}
		b = append(b, j)
	}
	return b
}
type Proxy interface {
	BeginTransaction(ctx context.Context) (string, error)
	CommitTransaction(ctx context.Context, tx string) (string, error)
	RollbackTransaction(ctx context.Context, tx string) (string, error)
	Exec(ctx context.Context, query string, values ...interface{}) (int64, error)
	ExecBatch(ctx context.Context, stm...Statement) (int64, error)
	Query(ctx context.Context, result interface{}, query string, values ...interface{}) error
	ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error)
	ExecBatchWithTx(ctx context.Context, tx string, commit bool, stm...Statement) (int64, error)
	QueryWithTx(ctx context.Context, result interface{}, tx string, query string, values ...interface{}) error
}
