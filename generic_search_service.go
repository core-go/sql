package sql

import (
	"database/sql"
	"reflect"
)

func NewGenericSearchService(db *sql.DB, modelType reflect.Type, tableName string, searchBuilder SearchResultBuilder, versionField string, mapper Mapper) (*GenericService, *SearchService) {
	genericService := NewGenericService(db, modelType, tableName, versionField, mapper)
	searchService := NewSearchService(db, modelType, tableName, searchBuilder)
	return genericService, searchService
}

func NewDefaultGenericSearchService(db *sql.DB, modelType reflect.Type, tableName string, searchBuilder SearchResultBuilder) (*GenericService, *SearchService) {
	genericService := NewDefaultGenericService(db, modelType, tableName)
	searchService := NewSearchService(db, modelType, tableName, searchBuilder)
	return genericService, searchService
}
