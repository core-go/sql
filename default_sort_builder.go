package sql

import (
	s "github.com/common-go/search"
	"github.com/jinzhu/gorm"
	"reflect"
	"strings"
)

const desc = "DESC"
const asc = "ASC"

type DefaultSortBuilder struct {
}

func (b *DefaultSortBuilder) BuildSort(s s.SearchModel, modelType reflect.Type) string {
	var sort = make([]string, 0)

	if len(s.Sort) == 0 {
		return ""
	}

	if strings.Index(s.Sort, ",") < 0 {
		columnName := b.getColumnName(s.Sort, modelType)
		sortType := b.getSortType(s.SortType)
		sort = append(sort, columnName+" "+sortType)
	} else {
		sorts := strings.Split(s.Sort, ",")
		for i := 0; i < len(sorts); i++ {
			sortField := strings.TrimSpace(sorts[i])
			params := strings.Split(sortField, " ")

			if len(params) > 0 {
				columnName := b.getColumnName(params[0], modelType)
				sortType := b.getSortType(params[1])
				sort = append(sort, columnName+" "+sortType)
			}
		}
	}

	return strings.Join(sort, ",")
}

func (b *DefaultSortBuilder) getColumnName(sortField string, modelType reflect.Type) string {
	sortField = strings.TrimSpace(sortField)
	i, _, column := GetFieldByJson(modelType, sortField)
	if i > -1 {
		return column
	}
	return gorm.ToColumnName(sortField)
}

func (b *DefaultSortBuilder) getSortType(sortType string) string {
	t := desc
	if strings.ToUpper(sortType) != strings.ToUpper(desc) {
		t = asc
	}
	return t
}
