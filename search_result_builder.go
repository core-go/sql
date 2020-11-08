package orm

import (
	"context"
	"reflect"

	s "github.com/common-go/search"
)

type SearchResultBuilder interface {
	BuildSearchResult(ctx context.Context, m interface{}, modelType reflect.Type, tableName string) (*s.SearchResult, error)
}
