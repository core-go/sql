package sql

import (
	"database/sql"
	"reflect"
)

func NewViewSearchService(db *sql.DB, modelType reflect.Type, tableName string, searchBuilder SearchResultBuilder, mapper Mapper) (*ViewService, *SearchService) {
	viewService := NewViewService(db, modelType, tableName, mapper)
	searchService := NewSearchService(db, modelType, tableName, searchBuilder)
	return viewService, searchService
}
