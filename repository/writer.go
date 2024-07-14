package dao

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"

	q "github.com/core-go/sql"
)

type Writer[T any] struct {
	DB            *sql.DB
	Table         string
	Schema        *q.Schema
	Keys          []string
	JsonColumnMap map[string]string
	BuildParam    func(int) string
	Driver        string
	BoolSupport   bool
	ToArray       func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	TxKey        string
	versionIndex int
	versionJson  string
	versionDB    string
}

func NewWriter[T any](db *sql.DB, tableName string, opts ...func(int) string) (*Writer[T], error) {
	return NewSqlWriterWithVersionAndArray[T](db, tableName, "", nil, opts...)
}
func NewWriterWithVersion[T any](db *sql.DB, tableName string, versionField string, opts ...func(int) string) (*Writer[T], error) {
	return NewSqlWriterWithVersionAndArray[T](db, tableName, versionField, nil, opts...)
}
func NewWriterWithArray[T any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...string) (*Writer[T], error) {
	var versionField string
	if len(opts) > 0 && len(opts[0]) > 0 {
		versionField = opts[0]
	}
	return NewSqlWriterWithVersionAndArray[T](db, tableName, versionField, toArray)
}
func NewSqlWriterWithVersionAndArray[T any](db *sql.DB, tableName string, versionField string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(int) string) (*Writer[T], error) {
	var buildParam func(i int) string
	if len(opts) > 0 && opts[0] != nil {
		buildParam = opts[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	drivr := q.GetDriver(db)
	boolSupport := drivr == q.DriverPostgres
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	_, primaryKeys := q.FindPrimaryKeys(modelType)
	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("require primary key for table '%s'", tableName)
	}
	schema := q.CreateSchema(modelType)
	jsonColumnMapT := q.MakeJsonColumnMap(modelType)
	jsonColumnMap := q.GetWritableColumns(schema.Fields, jsonColumnMapT)
	adapter := &Writer[T]{DB: db, Table: tableName, Schema: schema, Keys: primaryKeys, JsonColumnMap: jsonColumnMap, BuildParam: buildParam, Driver: drivr, BoolSupport: boolSupport, ToArray: toArray, TxKey: "tx", versionIndex: -1}
	if len(versionField) > 0 {
		index := q.FindFieldIndex(modelType, versionField)
		if index >= 0 {
			versionJson, versionDB, exist := q.GetFieldByIndex(modelType, index)
			if !exist {
				versionDB = strings.ToLower(versionField)
			}
			adapter.versionIndex = index
			adapter.versionJson = versionJson
			adapter.versionDB = versionDB
		}
	}
	return adapter, nil
}

func (a *Writer[T]) Create(ctx context.Context, model T) (int64, error) {
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	query, args := q.BuildToInsertWithVersion(a.Table, model, a.versionIndex, a.BuildParam, a.BoolSupport, a.ToArray, a.Schema)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return q.HandleDuplicate(a.DB, err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, err
	}
	if rowsAffected > 0 && a.versionIndex >= 0 {
		vo := reflect.ValueOf(model)
		if vo.Kind() == reflect.Ptr {
			vo = reflect.Indirect(vo)
		}
		setVersion(vo, a.versionIndex)
	}
	return res.RowsAffected()
}
func (a *Writer[T]) Update(ctx context.Context, model T) (int64, error) {
	query, args := q.BuildToUpdateWithVersion(a.Table, model, a.versionIndex, a.BuildParam, a.BoolSupport, a.ToArray, a.Schema)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, err
	}
	vo := reflect.ValueOf(model)
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	if rowsAffected < 1 {
		var values []interface{}
		query1 := fmt.Sprintf("select %s from %s ", a.Schema.SColumns[0], a.Table)
		le := len(a.Keys)
		var where []string
		for i := 0; i < le; i++ {
			where = append(where, fmt.Sprintf("%s = %s", a.Schema.Keys[i].Column), a.BuildParam(i+1))
		}
		query2 := query1 + " where " + strings.Join(where, " and ")
		rows, er2 := tx.QueryContext(ctx, query2, values...)
		if er2 != nil {
			return -1, err
		}
		defer rows.Close()
		for rows.Next() {
			return -1, nil
		}
		return 0, nil
	} else if a.versionIndex >= 0 {
		currentVersion := vo.Field(a.versionIndex).Interface()
		increaseVersion(vo, a.versionIndex, currentVersion)
	}
	return res.RowsAffected()
}
func (a *Writer[T]) Save(ctx context.Context, model T) (int64, error) {
	query, args, err := q.BuildToSaveWithSchema(a.Table, model, a.Driver, a.BuildParam, a.ToArray, a.Schema)
	if err != nil {
		return 0, err
	}
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, err
	}
	if rowsAffected > 0 && a.versionIndex >= 0 {
		vo := reflect.ValueOf(model)
		if vo.Kind() == reflect.Ptr {
			vo = reflect.Indirect(vo)
		}
		currentVersion := vo.Field(a.versionIndex).Interface()
		increaseVersion(vo, a.versionIndex, currentVersion)
	}
	return res.RowsAffected()
}
func (a *Writer[T]) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(model, a.JsonColumnMap)
	query, args := q.BuildToPatchWithVersion(a.Table, colMap, a.Schema.SKeys, a.BuildParam, a.ToArray, a.versionDB, a.Schema.Fields)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return -1, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return rowsAffected, err
	}
	if rowsAffected < 1 {
		var query2 string
		var values []interface{}
		query1 := fmt.Sprintf("select %s from %s ", a.Schema.SColumns[0], a.Table)
		if len(a.Keys) == 1 {
			query2, values = q.BuildFindByIdWithDB(a.DB, query1, model[a.Keys[0]], a.JsonColumnMap, a.Keys, a.BuildParam)
		} else {
			im := make(map[string]interface{})
			le := len(a.Keys)
			for i := 0; i < le; i++ {
				im[a.Keys[i]] = model[a.Keys[i]]
			}
			query2, values = q.BuildFindByIdWithDB(a.DB, query1, im, a.JsonColumnMap, a.Keys, a.BuildParam)
		}
		rows, er2 := tx.QueryContext(ctx, query2, values...)
		if er2 != nil {
			return -1, err
		}
		defer rows.Close()
		for rows.Next() {
			return -1, nil
		}
		return 0, nil
	} else if a.versionIndex >= 0 {
		currentVersion, vok := model[a.versionJson]
		if !vok {
			return -1, fmt.Errorf("%s must be in model for patch", a.versionJson)
		}
		ok := increaseMapVersion(model, a.versionJson, currentVersion)
		if !ok {
			return -1, errors.New("do not support this version type")
		}
	}
	return rowsAffected, err
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

func setVersion(vo reflect.Value, versionIndex int) bool {
	versionType := vo.Field(versionIndex).Type().String()
	switch versionType {
	case "int32":
		vo.Field(versionIndex).Set(reflect.ValueOf(int32(1)))
		return true
	case "int":
		vo.Field(versionIndex).Set(reflect.ValueOf(1))
		return true
	case "int64":
		vo.Field(versionIndex).Set(reflect.ValueOf(int64(1)))
		return true
	default:
		return false
	}
}
func increaseVersion(vo reflect.Value, versionIndex int, curVer interface{}) bool {
	versionType := vo.Field(versionIndex).Type().String()
	switch versionType {
	case "int32":
		nextVer := curVer.(int32) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	case "int":
		nextVer := curVer.(int) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	case "int64":
		nextVer := curVer.(int64) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	default:
		return false
	}
}
func increaseMapVersion(model map[string]interface{}, name string, currentVersion interface{}) bool {
	if versionI32, ok := currentVersion.(int32); ok {
		model[name] = versionI32 + 1
		return true
	} else if versionI, ok := currentVersion.(int); ok {
		model[name] = versionI + 1
		return true
	} else if versionI64, ok := currentVersion.(int64); ok {
		model[name] = versionI64 + 1
		return true
	} else {
		return false
	}
}
