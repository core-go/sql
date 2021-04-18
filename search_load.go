package sql

import (
	"context"
	"database/sql"
	"reflect"
)

func NewSearchLoader(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader) {
	build := GetBuild(db)
	return NewSqlSearchLoader(db, tableName, modelType, buildQuery, build, options...)
}

func NewSqlSearchLoader(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), buildParam func(i int) string, options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	loader := NewSqlLoader(db, tableName, modelType, mp, buildParam)
	searcher := NewSearcherWithQuery(db, modelType, buildQuery, options...)
	return searcher, loader
}
