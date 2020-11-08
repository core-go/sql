package orm

import (
	s "github.com/common-go/search"
	"reflect"
)

type DefaultSortBuilder struct {
}

func (b *DefaultSortBuilder) BuildSort(s s.SearchModel, modelType reflect.Type) string {
	if len(s.Sort) == 0 {
		return ""
	}
	return BuildSort(s.Sort, modelType)
}

