package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
)

type SearchBuilder struct {
	Database    *sql.DB
	BuildQuery  func(sm interface{}) (string, []interface{})
	ModelType   reflect.Type
	Map         func(ctx context.Context, model interface{}) (interface{}, error)
	fieldsIndex map[string]int
}

func NewSearchBuilder(db *sql.DB, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*SearchBuilder, error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, err
	}
	builder := &SearchBuilder{Database: db, fieldsIndex: fieldsIndex, BuildQuery: buildQuery, ModelType: modelType, Map: mp}
	return builder, nil
}

func (b *SearchBuilder) Search(ctx context.Context, m interface{}, results interface{}, limit int64, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...int64) (int64, string, error) {
	sql, params := b.BuildQuery(m)
	var offset int64 = 0
	if len(options) > 0 && options[0] > 0 {
		offset = options[0]
	}
	total, err := BuildFromQuery(ctx, b.Database, b.fieldsIndex, results, sql, params, limit, offset, toArray, b.Map)
	return total, "", err
}
