package sql

import (
	s "github.com/common-go/search"
	"reflect"
)

type SortBuilder interface {
	BuildSort(searchModel s.SearchModel, modelType reflect.Type) string
}
