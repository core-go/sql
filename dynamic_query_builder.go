package sql

import (
	"reflect"
)

type DynamicQueryBuilder interface {
	BuildQuery(sm interface{}, resultModelType reflect.Type) DynamicQuery
}
