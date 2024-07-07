package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

const IgnoreReadWrite = "-"
const txs = "tx"

func Any(ctx context.Context, db Executor, sql string, values ...interface{}) (bool, error) {
	rows, err := db.QueryContext(ctx, sql, values...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		return true, nil
	}
	return false, nil
}

func GetExec(ctx context.Context, db *sql.DB, opts ...string) Executor {
	name := txs
	if len(opts) > 0 && len(opts[0]) > 0 {
		name = opts[0]
	}
	txi := ctx.Value(name)
	if txi != nil {
		txx, ok := txi.(*sql.Tx)
		if ok {
			return txx
		}
	}
	return db
}
func GetTx(ctx context.Context) *sql.Tx {
	txi := ctx.Value(txs)
	if txi != nil {
		txx, ok := txi.(*sql.Tx)
		if ok {
			return txx
		}
	}
	return nil
}
func GetTxId(ctx context.Context) *string {
	txi := ctx.Value("txId")
	if txi != nil {
		txx, ok := txi.(*string)
		if ok {
			return txx
		}
	}
	return nil
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
func QueryMap(ctx context.Context, db Executor, transform func(s string) string, sql string, values ...interface{}) ([]map[string]interface{}, error) {
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
func QueryTx(ctx context.Context, tx *sql.Tx, fieldsIndex map[string]int, results interface{}, sql string, values ...interface{}) error {
	return QueryTxWithArray(ctx, tx, fieldsIndex, results, nil, sql, values...)
}
func QueryTxWithArray(ctx context.Context, tx *sql.Tx, fieldsIndex map[string]int, results interface{}, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, sql string, values ...interface{}) error {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()

	modelType := reflect.TypeOf(results).Elem().Elem()
	tb, er3 := Scan(rows, modelType, fieldsIndex, toArray)
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
	return QueryRowWithArray(ctx, db, modelType, fieldsIndex, nil, sql, values...)
}
func QueryRowWithArray(ctx context.Context, db *sql.DB, modelType reflect.Type, fieldsIndex map[string]int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, sql string, values ...interface{}) (interface{}, error) {
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
	tb, er2 := Scan(rows, modelType, fieldsIndex, toArray)
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
	if len(tb) == 0 {
		return nil, nil
	} else {
		return tb[0], nil
	}
}
func QueryRowTx(ctx context.Context, tx *sql.Tx, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	return QueryRowTxWithArray(ctx, tx, modelType, fieldsIndex, nil, sql, values...)
}
func QueryRowTxWithArray(ctx context.Context, tx *sql.Tx, modelType reflect.Type, fieldsIndex map[string]int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, sql string, values ...interface{}) (interface{}, error) {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex, toArray)
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
func QueryRowByStatement(ctx context.Context, stm *sql.Stmt, modelType reflect.Type, fieldsIndex map[string]int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, values ...interface{}) (interface{}, error) {
	rows, er1 := stm.QueryContext(ctx, values...)
	// rows, er1 := db.Query(s, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex, toArray)
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
func ScanRow(rows *sql.Rows, s interface{}, columns []string, fieldsIndex map[string]int, options ...func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) error {
	r, swapValues := StructScan(s, columns, fieldsIndex, options...)
	err := rows.Scan(r...)
	if err == nil {
		SwapValuesToBool(s, &swapValues)
	}
	return err
}
func ScanRowsWithArray(rows *sql.Rows, structType reflect.Type, fieldsIndex map[string]int, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (t interface{}, err error) {
	columns, er0 := GetColumns(rows.Columns())
	err = er0
	if err != nil {
		return
	}
	if fieldsIndex == nil {
		fieldsIndex, er0 = GetColumnIndexes(structType)
		if er0 != nil {
			err = er0
			return
		}
	}
	for rows.Next() {
		gTb := reflect.New(structType).Interface()
		r, swapValues := StructScanAndIgnore(gTb, columns, fieldsIndex, toArray, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(gTb, &swapValues)
			t = gTb
			break
		}
	}
	return
}

// Row
func ScanRowWithArray(row *sql.Row, structType reflect.Type, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (t interface{}, err error) {
	t = reflect.New(structType).Interface()
	r, swapValues := StructScan(t, nil, nil, toArray)
	err = row.Scan(r...)
	SwapValuesToBool(t, &swapValues)
	return
}
func MapModels(ctx context.Context, models interface{}, mp func(context.Context, interface{}) (interface{}, error)) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(models))
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	if vo.Kind() == reflect.Slice {
		le := vo.Len()
		for i := 0; i < le; i++ {
			x := vo.Index(i)
			k := x.Kind()
			if k == reflect.Struct {
				y := x.Addr().Interface()
				mp(ctx, y)
			} else {
				y := x.Interface()
				mp(ctx, y)
			}

		}
	}
	return models, nil
}
