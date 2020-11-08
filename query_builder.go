package orm

import (
	"reflect"
)

type QueryBuilder interface {
	BuildQuery(sm interface{}, resultModelType reflect.Type, tableName string, provider string) (string, []interface{})
}
