package sql

import (
	"context"
	s "github.com/common-go/search"
	"github.com/jinzhu/gorm"
	"reflect"
)

type SearchService struct {
	Database      *gorm.DB
	modelType     reflect.Type
	table         string
	searchBuilder SearchResultBuilder
}

func NewSearchService(db *gorm.DB, modelType reflect.Type, tableName string, searchBuilder SearchResultBuilder) *SearchService {
	return &SearchService{db, modelType, tableName, searchBuilder}
}

func (s *SearchService) Search(ctx context.Context, m interface{}) (*s.SearchResult, error) {
	return s.searchBuilder.BuildSearchResult(ctx, m, s.modelType, s.table)
}
