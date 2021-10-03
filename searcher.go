package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Searcher struct {
	search  func(ctx context.Context, searchModel interface{}, results interface{}, limit int64, options ...int64) (int64, string, error)
	ToArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
func NewSearcher(search func(context.Context, interface{}, interface{}, int64, ...int64) (int64, string, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) *Searcher {
	return NewSearcherWithArray(search, toArray)
}
func NewSearcherWithArray(search func(context.Context, interface{}, interface{}, int64, ...int64) (int64, string, error), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) *Searcher {
	return &Searcher{search: search, ToArray: toArray}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, limit int64, options ...int64) (int64, string, error) {
	return s.search(ctx, m, results, limit, options...)
}

func NewSearcherWithQuery(db *sql.DB, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, error) {
	builder, err := NewSearchBuilderWithArray(db, modelType, buildQuery, toArray, options...)
	if err != nil {
		return nil, err
	}
	return NewSearcherWithArray(builder.Search, toArray), nil
}
