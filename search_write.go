package sql

import (
	"database/sql"
	"reflect"
)

func NewSearchWriterWithVersionAndMap(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), versionField string, mapper Mapper, options ...func(i int) string) (*Searcher, *Writer) {
	if mapper == nil {
		searcher := NewSearcherWithQuery(db, modelType, buildQuery)
		writer := NewSqlWriterWithVersion(db, tableName, modelType, versionField, mapper, options...)
		return searcher, writer
	} else {
		searcher := NewSearcherWithQuery(db, modelType, buildQuery, mapper.DbToModel)
		writer := NewSqlWriterWithVersion(db, tableName, modelType, versionField, mapper, options...)
		return searcher, writer
	}
}
func NewSearchWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), versionField string, options...Mapper) (*Searcher, *Writer) {
	var mapper Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, versionField, mapper)
}
func NewSearchWriterWithMap(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), mapper Mapper, options...string) (*Searcher, *Writer) {
	var versionField string
	if len(options) > 0 {
		versionField = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, versionField, mapper)
}
func NewSearchWriter(db *sql.DB, tableName string, modelType reflect.Type, buildQuery func(interface{}) (string, []interface{}), options...Mapper) (*Searcher, *Writer) {
	build := GetBuild(db)
	var mapper Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	return NewSearchWriterWithVersionAndMap(db, tableName, modelType, buildQuery, "", mapper, build)
}
