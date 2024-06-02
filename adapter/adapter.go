package adapter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"

	q "github.com/core-go/sql"
)

type Adapter[T any] struct {
	DB            *sql.DB
	Table         string
	Schema        *q.Schema
	JsonColumnMap map[string]string
	BuildParam    func(int) string
	Driver        string
	BoolSupport   bool
	ToArray       func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	TxKey          string
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewAdapter[T any](db *sql.DB, tableName string, opts ...func(int) string) (*Adapter[T], error) {
	return NewSqlAdapterWithVersionAndArray[T](db, tableName, "", nil, opts...)
}
func NewAdapterWithVersion[T any](db *sql.DB, tableName string, versionField string, opts ...func(int) string) (*Adapter[T], error) {
	return NewSqlAdapterWithVersionAndArray[T](db, tableName, versionField, nil, opts...)
}
func NewAdapterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...string) (*Adapter[T], error) {
	var versionField string
	if len(opts) > 0 && len(opts[0]) > 0 {
		versionField = opts[0]
	}
	return NewSqlAdapterWithVersionAndArray[T](db, tableName, versionField, toArray)
}
func NewSqlAdapterWithVersionAndArray[T any](db *sql.DB, tableName string, versionField string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(int) string) (*Adapter[T], error) {
	var buildParam func(i int) string
	if len(opts) > 0 && opts[0] != nil {
		buildParam = opts[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	driver := q.GetDriver(db)
	boolSupport := driver == q.DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	schema := q.CreateSchema(modelType)
	jsonColumnMapT := q.MakeJsonColumnMap(modelType)
	jsonColumnMap := q.GetWritableColumns(schema.Fields, jsonColumnMapT)
	adapter := &Adapter[T]{DB: db, Table: tableName, Schema: schema, JsonColumnMap: jsonColumnMap, BuildParam: buildParam, Driver: driver, BoolSupport: boolSupport, ToArray: toArray, TxKey: "tx", versionField: "", versionIndex: -1}
	if len(versionField) > 0 {
		index := q.FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := q.GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			adapter.versionField = versionField
			adapter.versionIndex = index
			adapter.versionDBField = dbFieldName
		}
	}
	return adapter, nil
}

func (a *Adapter[T]) Create(ctx context.Context, model interface{}) (int64, error) {
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	query, args := q.BuildToInsertWithVersion(a.Table, model, a.versionIndex, a.BuildParam, a.BoolSupport, a.ToArray, a.Schema)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
func (a *Adapter[T]) Update(ctx context.Context, model interface{}) (int64, error) {
	query, args := q.BuildToUpdateWithVersion(a.Table, model, a.versionIndex, a.BuildParam, a.BoolSupport, a.ToArray, a.Schema)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
func (a *Adapter[T]) Save(ctx context.Context, model interface{}) (int64, error) {
	query, args, err := q.BuildToSaveWithSchema(a.Table, model, a.Driver, a.BuildParam, a.ToArray, a.Schema)
	if err != nil {
		return 0, err
	}
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
func (a *Adapter[T]) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(model, a.JsonColumnMap)
	query, args := q.BuildToPatchWithVersion(a.Table, colMap, a.Schema.SKeys, a.BuildParam, a.ToArray, a.versionDBField, a.Schema.Fields)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
func handleDuplicate(db *sql.DB, err error) (int64, error) {
	x := err.Error()
	driver := q.GetDriver(db)
	if driver == q.DriverPostgres && strings.Contains(x, "pq: duplicate key value violates unique constraint") {
		return 0, nil
	} else if driver == q.DriverMysql && strings.Contains(x, "Error 1062: Duplicate entry") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driver == q.DriverOracle && strings.Contains(x, "ORA-00001: unique constraint") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driver == q.DriverMssql && strings.Contains(x, "Violation of PRIMARY KEY constraint") {
		return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
	} else if driver == q.DriverSqlite3 && strings.Contains(x, "UNIQUE constraint failed") {
		return 0, nil
	}
	return 0, err
}
