package query

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type Query[T any, K any, F any] struct {
	*Loader[T, K]
	BuildQuery func(F) (string, []interface{})
	Mp         func(*T)
	Map        map[string]int
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewQuery[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), opts ...func(*T)) (*Query[T, K, F], error) {
	return NewQueryWithArray[T, K, F](db, table, buildQuery, nil, nil, opts...)
}
func NewQueryWithArray[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, buildParam func(int) string, opts ...func(*T)) (*Query[T, K, F], error) {
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	adapter, err := NewLoaderWithMapAndArray[T, K](db, table, toArray, mp, buildParam)
	if err != nil {
		return nil, err
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	builder := &Query[T, K, F]{Loader: adapter, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp, ToArray: toArray}
	return builder, nil
}

func (b *Query[T, K, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
	var objs []T
	query, args := b.BuildQuery(filter)
	total, er2 := q.BuildFromQuery(ctx, b.DB, b.Map, &objs, query, args, limit, offset, b.ToArray)
	if b.Mp != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Mp(&objs[i])
		}
	}
	return objs, total, er2
}
