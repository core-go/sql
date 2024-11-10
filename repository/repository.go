package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	q "github.com/core-go/sql"
)

type Repository[T any, K any] struct {
	*Writer[*T]
	Map    map[string]int
	Fields string
	IdMap  bool
}

func NewRepository[T any, K any](db *sql.DB, tableName string, opts ...func(int) string) (*Repository[T, K], error) {
	return NewRepositoryWithVersionAndArray[T, K](db, tableName, "", nil, opts...)
}
func NewRepositoryWithVersion[T any, K any](db *sql.DB, tableName string, versionField string, opts ...func(int) string) (*Repository[T, K], error) {
	return NewRepositoryWithVersionAndArray[T, K](db, tableName, versionField, nil, opts...)
}
func NewSqlRepositoryWithArray[T any, K any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(int) string) (*Repository[T, K], error) {
	return NewRepositoryWithVersionAndArray[T, K](db, tableName, "", toArray, opts...)
}
func NewRepositoryWithVersionAndArray[T any, K any](db *sql.DB, tableName string, versionField string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(int) string) (*Repository[T, K], error) {
	repo, err := NewSqlWriterWithVersionAndArray[*T](db, tableName, versionField, toArray, opts...)
	if err != nil {
		return nil, err
	}

	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("T must be a struct")
	}

	var k K
	kType := reflect.TypeOf(k)
	idMap := false
	if len(repo.Keys) > 1 {
		if kType.Kind() == reflect.Map {
			idMap = true
		} else if kType.Kind() != reflect.Struct {
			return nil, errors.New("for composite keys, K must be a struct or a map")
		}
	}

	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	fields := q.BuildFieldsBySchema(repo.Schema)
	return &Repository[T, K]{repo, fieldsIndex, fields, idMap}, nil
}

func (a *Repository[T, K]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := fmt.Sprintf("select %s from %s", a.Fields, a.Table)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	err := q.Query(ctx, tx, a.Map, &objs, query)
	return objs, err
}
func toMap(obj interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	im := make(map[string]interface{})
	er2 := json.Unmarshal(b, &im)
	return im, er2
}
func (a *Repository[T, K]) getId(k K) (interface{}, error) {
	if len(a.Keys) >= 2 && !a.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (a *Repository[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return nil, er0
	}
	var objs []T
	query := fmt.Sprintf("select %s from %s ", a.Fields, a.Table)
	query1, args := q.BuildFindByIdWithDB(a.DB, query, ip, a.JsonColumnMap, a.Keys, a.BuildParam)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	err := q.QueryWithArray(ctx, tx, a.Map, &objs, a.ToArray, query1, args...)
	if err != nil {
		return nil, err
	}
	if len(objs) > 0 {
		return &objs[0], nil
	}
	return nil, nil
}
func (a *Repository[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return false, er0
	}
	query := fmt.Sprintf("select %s from %s ", a.Schema.SColumns[0], a.Table)
	query1, args := q.BuildFindByIdWithDB(a.DB, query, ip, a.JsonColumnMap, a.Keys, a.BuildParam)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	rows, err := tx.QueryContext(ctx, query1, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		return true, nil
	}
	return false, nil
}
func (a *Repository[T, K]) Delete(ctx context.Context, id K) (int64, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return -1, er0
	}
	query := fmt.Sprintf("delete from %s ", a.Table)
	query1, args := q.BuildFindByIdWithDB(a.DB, query, ip, a.JsonColumnMap, a.Keys, a.BuildParam)
	tx := q.GetExec(ctx, a.DB, a.TxKey)
	res, err := tx.ExecContext(ctx, query1, args...)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
