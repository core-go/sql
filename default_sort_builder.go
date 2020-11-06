package sql

import (
	s "github.com/common-go/search"
	"gorm.io/gorm"
	schema "gorm.io/gorm/schema"
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
	sorts := strings.Split(s.Sort, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField
		c := sortField[0:1]
		if c == "-" || c == "+" {
			fieldName = sortField[1:]
		}
		columnName := GetColumnNameForSearch(modelType, fieldName)
		sortType := GetSortType(c)
		sort = append(sort, columnName+" "+sortType)
	}
	return strings.Join(sort, ",")
}

func GetColumnNameForSearch(modelType reflect.Type, sortField string) string {
	sortField = strings.TrimSpace(sortField)
	i, _, column := GetFieldByJson(modelType, sortField)
	if i > -1 {
		return column
	}
	return GormToColumnName(sortField)
}

func GetSortType(sortType string) string {
	if sortType == "-" {
		return desc
	} else  {
		return asc
	}
}
