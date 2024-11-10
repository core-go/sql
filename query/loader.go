package query

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	q "github.com/core-go/sql"
)

type Loader[T any, K any] struct {
	DB            *sql.DB
	Table         string
	JsonColumnMap map[string]string
	FieldMap      map[string]int
	Fields        string
	Columns       []string
	Field0        string
	Keys          []string
	BuildParam    func(i int) string
	Map           func(*T)
	ToArray       func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
	IdMap bool
}

func NewLoader[T any, K any](db *sql.DB, tableName string, opts ...func(int) string) (*Loader[T, K], error) {
	return NewLoaderWithMapAndArray[T, K](db, tableName, nil, nil, opts...)
}
func NewLoaderWithArray[T any, K any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(int) string) (*Loader[T, K], error) {
	return NewLoaderWithMapAndArray[T, K](db, tableName, toArray, nil, opts...)
}
func NewLoaderWithMap[T any, K any](db *sql.DB, tableName string, mp func(*T), opts ...func(int) string) (*Loader[T, K], error) {
	return NewLoaderWithMapAndArray[T, K](db, tableName, nil, mp, opts...)
}
func NewLoaderWithMapAndArray[T any, K any](db *sql.DB, tableName string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, mp func(*T), opts ...func(int) string) (*Loader[T, K], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("T must be a struct")
	}
	jsonColumnMap := q.MakeJsonColumnMap(modelType)
	_, primaryKeys := q.FindPrimaryKeys(modelType)
	var k K
	kType := reflect.TypeOf(k)
	idMap := false
	if len(primaryKeys) <= 0 {
		panic(fmt.Sprintf("require primary key for table '%s'", tableName))
	}
	if len(primaryKeys) > 1 {
		if kType.Kind() == reflect.Map {
			idMap = true
		} else if kType.Kind() != reflect.Struct {
			return nil, errors.New("for composite keys, K must be a struct or a map")
		}
	}
	var buildParam func(int) string
	if len(opts) > 0 && opts[0] != nil {
		buildParam = opts[0]
	} else {
		buildParam = q.GetBuild(db)
	}
	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	columns := q.GetFields(modelType)
	if len(columns) <= 0 {
		return nil, errors.New("there must be at least 1 column")
	}
	field0 := columns[0]
	fields := strings.Join(columns, ",")
	return &Loader[T, K]{db, tableName, jsonColumnMap, fieldsIndex, fields, columns, field0, primaryKeys, buildParam, mp, toArray, idMap}, nil
}
func (a *Loader[T, K]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := fmt.Sprintf("select %s from %s", a.Fields, a.Table)
	err := q.QueryWithArray(ctx, a.DB, a.FieldMap, &objs, a.ToArray, query)
	if a.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			a.Map(&objs[i])
		}
	}
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
func (a *Loader[T, K]) getId(k K) (interface{}, error) {
	if len(a.Keys) >= 2 && !a.IdMap {
		ri, err := toMap(k)
		return ri, err
	} else {
		return k, nil
	}
}
func (a *Loader[T, K]) Load(ctx context.Context, id K) (*T, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return nil, er0
	}
	var objs []T
	query := fmt.Sprintf("select %s from %s ", a.Fields, a.Table)
	query1, args := q.BuildFindByIdWithDB(a.DB, query, ip, a.JsonColumnMap, a.Keys, a.BuildParam)
	err := q.QueryWithArray(ctx, a.DB, a.FieldMap, &objs, a.ToArray, query1, args...)
	if err != nil {
		return nil, err
	}
	if len(objs) > 0 {
		if a.Map != nil {
			a.Map(&objs[0])
		}
		return &objs[0], nil
	}
	return nil, nil
}
func (a *Loader[T, K]) Exist(ctx context.Context, id K) (bool, error) {
	ip, er0 := a.getId(id)
	if er0 != nil {
		return false, er0
	}
	query := fmt.Sprintf("select %s from %s ", a.Field0, a.Table)
	query1, args := q.BuildFindByIdWithDB(a.DB, query, ip, a.JsonColumnMap, a.Keys, a.BuildParam)
	rows, err := a.DB.QueryContext(ctx, query1, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		return true, nil
	}
	return false, nil
}
