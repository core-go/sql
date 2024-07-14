package dao

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"

	q "github.com/core-go/sql"
)

type SearchBuilder[T any, F any] struct {
	Database    *sql.DB
	BuildQuery  func(F) (string, []interface{})
	fieldsIndex map[string]int
	Map         func(*T)
	ToArray     func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}

func NewSearchBuilder[T any, F any](db *sql.DB, buildQuery func(F) (string, []interface{}), opts ...func(*T)) (*SearchBuilder[T, F], error) {
	return NewSearchBuilderWithArray[T, F](db, buildQuery, nil, opts...)
}
func NewSearchBuilderWithArray[T any, F any](db *sql.DB, buildQuery func(F) (string, []interface{}), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, opts ...func(*T)) (*SearchBuilder[T, F], error) {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		return nil, errors.New("T must be a struct")
	}
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	fieldsIndex, err := q.GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	builder := &SearchBuilder[T, F]{Database: db, fieldsIndex: fieldsIndex, BuildQuery: buildQuery, Map: mp, ToArray: toArray}
	return builder, nil
}

func (b *SearchBuilder[T, F]) Search(ctx context.Context, m F, limit int64, offset int64) ([]T, int64, error) {
	sql, params := b.BuildQuery(m)
	var objs []T
	total, er2 := q.BuildFromQuery(ctx, b.Database, b.fieldsIndex, &objs, sql, params, limit, offset, b.ToArray)
	if b.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Map(&objs[i])
		}
	}
	return objs, total, er2
}
