package sql

import (
	"context"
	"database/sql"
	"reflect"
)

type SearchBuilder struct {
	Database   *sql.DB
	BuildQuery func(sm interface{}) (string, []interface{})
	ModelType  reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSearchBuilder(db *sql.DB, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) *SearchBuilder {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	builder := &SearchBuilder{Database: db, BuildQuery: buildQuery, ModelType: modelType, Map: mp}
	return builder
}

func (b *SearchBuilder) Search(ctx context.Context, m interface{}, results interface{}, pageIndex int64, pageSize int64, options...int64) (int64, error) {
	sql, params := b.BuildQuery(m)
	var firstPageSize int64
	if len(options) > 0 && options[0] > 0 {
		firstPageSize = options[0]
	} else {
		firstPageSize = 0
	}
	return BuildFromQuery(ctx, b.Database, results, sql, params, pageIndex, pageSize, firstPageSize, b.Map)
}
