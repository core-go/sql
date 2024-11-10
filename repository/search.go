package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"

	q "github.com/core-go/sql"
)

type SearchRepository[T any, K any, F any] struct {
	*Repository[T, K]
	BuildQuery func(F) (string, []interface{})
	Mp         func(*T)
	Map        map[string]int
	ToArray    func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewSearchRepository[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), options ...func(*T)) (*SearchRepository[T, K, F], error) {
	return NewSearchRepositoryWithArray[T, K, F](db, table, buildQuery, nil, "", nil, options...)
}
func NewSearchRepositoryWithVersion[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), versionField string, options ...func(*T)) (*SearchRepository[T, K, F], error) {
	return NewSearchRepositoryWithArray[T, K, F](db, table, buildQuery, nil, versionField, nil, options...)
}
func NewSearchRepositoryWithArray[T any, K any, F any](db *sql.DB, table string, buildQuery func(F) (string, []interface{}), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, versionField string, buildParam func(int) string, opts ...func(*T)) (*SearchRepository[T, K, F], error) {
	repo, err := NewRepositoryWithVersionAndArray[T, K](db, table, versionField, toArray, buildParam)
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
	builder := &SearchRepository[T, K, F]{Repository: repo, Map: fieldsIndex, BuildQuery: buildQuery, Mp: mp, ToArray: toArray}
	return builder, nil
}

func (b *SearchRepository[T, K, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
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
