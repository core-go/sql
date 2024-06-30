package adapter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type SearchAdapter[T any, K any, F any] struct {
	*Adapter[T, K]
	BuildQuery func(F) (string, []interface{})
	Mp         func(*T)
	Map        map[string]int
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewSearchAdapter[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), options ...func(*T)) (*SearchAdapter[T, K, F], error) {
	return NewSearchAdapterWithArray[T, K, F](db, table, buildQuery, nil, "", nil, options...)
}
func NewSearchAdapterWithVersion[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), versionField string, options ...func(*T)) (*SearchAdapter[T, K, F], error) {
	return NewSearchAdapterWithArray[T, K, F](db, table, buildQuery, nil, versionField, nil, options...)
}
func NewSearchAdapterWithArray[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, versionField string, buildParam func(int) string, opts ...func(*T)) (*SearchAdapter[T, K, F], error) {
	adapter, err := NewSqlAdapterWithVersionAndArray[T, K](db, table, versionField, toArray, buildParam)
	if err != nil {
		return nil, err
	}
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
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
	builder := &SearchAdapter[T, K, F]{Adapter: adapter, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp, ToArray: toArray}
	return builder, nil
}

func (b *SearchAdapter[T, K, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
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
