package builder

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"

	tem "github.com/core-go/sql/template"
)

type QueryBuilder[F any] struct {
	Template  tem.Template
	ModelType *reflect.Type
	Map       func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}
	Param     func(int) string
	BuildSort func(string, reflect.Type) string
	Q         func(string) string
	ToArray   func(interface{}) interface {
		driver.Valuer
		sql.Scanner
	}
}
type Builder[F any] interface {
	BuildQuery(F) (string, []interface{})
}

func UseQuery[F any](id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(string) string) (func(F) (string, []interface{}), error) {
	b, err := NewQueryBuilder[F](id, m, modelType, mp, param, buildSort, opts...)
	if err != nil {
		return nil, err
	}
	return b.BuildQuery, nil
}
func UseQueryWithArray[F any](id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (func(F) (string, []interface{}), error) {
	b, err := NewQueryBuilderWithArray[F](id, m, modelType, mp, param, buildSort, nil, opts...)
	if err != nil {
		return nil, err
	}
	return b.BuildQuery, nil
}
func GetQuery[F any](isTemplate bool, query func(F) (string, []interface{}), id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(string) string) (func(F) (string, []interface{}), error) {
	if !isTemplate {
		return query, nil
	}
	b, err := NewQueryBuilder[F](id, m, modelType, mp, param, buildSort, opts...)
	if err != nil {
		return nil, err
	}
	return b.BuildQuery, nil
}
func GetQueryWithArray[F any](isTemplate bool, query func(F) (string, []interface{}), id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (func(F) (string, []interface{}), error) {
	if !isTemplate {
		return query, nil
	}
	b, err := NewQueryBuilderWithArray[F](id, m, modelType, mp, param, buildSort, nil, opts...)
	if err != nil {
		return nil, err
	}
	return b.BuildQuery, nil
}
func UseQueryBuilder[F any](id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(string) string) (Builder[F], error) {
	return NewQueryBuilder[F](id, m, modelType, mp, param, buildSort, opts...)
}
func GetQueryBuilder[F any](isTemplate bool, builder Builder[F], id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(string) string) (Builder[F], error) {
	if !isTemplate {
		return builder, nil
	}
	return NewQueryBuilder[F](id, m, modelType, mp, param, buildSort, opts...)
}
func NewQueryBuilderWithArray[F any](id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, q func(string) string, opts ...func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}) (*QueryBuilder[F], error) {
	b, err := NewQueryBuilder[F](id, m, modelType, mp, param, buildSort, q)
	if err != nil {
		return b, err
	}
	if len(opts) > 0 && opts[0] != nil {
		b.ToArray = opts[0]
	}
	return b, nil
}
func NewQueryBuilder[F any](id string, m map[string]*tem.Template, modelType *reflect.Type, mp func(interface{}, *reflect.Type, ...func(string, reflect.Type) string) map[string]interface{}, param func(i int) string, buildSort func(string, reflect.Type) string, opts ...func(string) string) (*QueryBuilder[F], error) {
	t, ok := m[id]
	if !ok || t == nil {
		return nil, errors.New("cannot get the template with id " + id)
	}
	var q func(string) string
	if len(opts) > 0 {
		q = opts[0]
	} else {
		q = tem.Q
	}
	return &QueryBuilder[F]{Template: *t, ModelType: modelType, Map: mp, Param: param, BuildSort: buildSort, Q: q}, nil
}
func (b *QueryBuilder[F]) BuildQuery(f F) (string, []interface{}) {
	m := b.Map(f, b.ModelType, b.BuildSort)
	if b.Q != nil {
		q, ok := m["q"]
		if ok {
			s, ok := q.(string)
			if ok {
				m["q"] = b.Q(s)
			}
		}
	}
	return tem.Build(m, b.Template, b.Param, b.ToArray)
}
