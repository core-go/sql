package sql

import (
	"gorm.io/gorm"
	"reflect"
)

func NewViewSearchService(db *gorm.DB, modelType reflect.Type, tableName string, searchBuilder SearchResultBuilder, mapper Mapper) (*ViewService, *SearchService) {
	viewService := NewViewService(db, modelType, tableName, mapper)
	searchService := NewSearchService(db, modelType, tableName, searchBuilder)
	return viewService, searchService
}
