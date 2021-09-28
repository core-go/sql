package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type Searcher struct {
	search func(ctx context.Context, searchModel interface{}, results interface{}, limit int64, toArray func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}, options...int64) (int64, string, error)
}

func NewSearcher(search func(context.Context, interface{}, interface{}, int64, func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, ...int64) (int64, string, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, limit int64, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options...int64) (int64, string, error) {
	return s.search(ctx, m, results, limit, toArray, options...)
}

func NewSearcherWithQuery(db *sql.DB, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, error) {
	builder, err := NewSearchBuilder(db, modelType, buildQuery, options...)
	if err != nil {
		return nil, err
	}
	return NewSearcher(builder.Search), nil
}
