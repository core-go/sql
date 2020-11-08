package orm

import (
	"fmt"
	s "github.com/common-go/search"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type DefaultDynamicQueryBuilder struct {
}

const (
	layoutISODate = "2006-01-02"
)

type DynamicConditionQuery struct {
	ColumnName        string
	OperatorCondition string
	Value             string
}

func (b *DefaultDynamicQueryBuilder) GetColumnNameFromSqlBuilderTag(typeOfField reflect.StructField) *string {
	tag := typeOfField.Tag
	properties := strings.Split(tag.Get("sql_builder"), ";")
	for _, property := range properties {
		if strings.HasPrefix(property, "column:") {
			column := property[7:]
			return &column
		}
	}
	return nil
}

func (b *DefaultDynamicQueryBuilder) BuildQuery(sm interface{}, resultModelType reflect.Type) DynamicQuery {
	// var query = map[string]interface{}{}
	if _, ok := sm.(*s.SearchModel); ok {
		return DynamicQuery{}
	}

	rawConditions := make([]string, 0)
	queryValues := make([]interface{}, 0)
	fields := make([]string, 0)
	var keyword string
	keywordFormat := map[string]string{
		"prefix":  "%v%%",
		"contain": "%%%v%%",
	}

	value := reflect.Indirect(reflect.ValueOf(sm))
	typeOfValue := value.Type()
	numField := value.NumField()
	for i := 0; i < numField; i++ {
		field := value.Field(i)
		kind := field.Kind()
		interfaceOfField := field.Interface()
		typeOfField := value.Type().Field(i)

		if v, ok := interfaceOfField.(*s.SearchModel); ok {
			if len(v.Fields) > 0 {
				for _, key := range v.Fields {
					i, _, columnName := GetFieldByJson(resultModelType, key)
					if len(columnName) < 0 {
						fields = fields[len(fields):]
						break
					} else if i == -1 {
						columnName = GormToColumnName(key)
					}
					fields = append(fields, columnName)
				}
			}
		}

		columnName, existCol := GetColumnName(value.Type(), typeOfField.Name)
		if !existCol {
			columnName, _ = GetColumnName(resultModelType, typeOfField.Name)
		}
		columnNameFromSqlBuilderTag := b.GetColumnNameFromSqlBuilderTag(typeOfField)
		if columnNameFromSqlBuilderTag != nil {
			columnName = *columnNameFromSqlBuilderTag
		}

		if kind == reflect.Ptr && field.IsNil() {
			continue
		} else if v, ok := interfaceOfField.(*s.SearchModel); ok {
			if len(v.Excluding) > 0 {
				r := regexp.MustCompile(`[A-Z]`)
				for key, val := range v.Excluding {
					columnName = r.ReplaceAllStringFunc(key, func(m string) string {
						out := "_" + strings.ToLower(m)
						return out
					})
					if len(val) > 0 {
						rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s (?)", columnName, "NOT", In))
						queryValues = append(queryValues, val)
					}
				}
			} else if len(v.Keyword) > 0 {
				keyword = strings.TrimSpace(v.Keyword)
			}
			continue
		} else if dateRange, ok := interfaceOfField.(s.DateRange); ok {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, GreaterEqualThan))
			queryValues = append(queryValues, dateRange.StartDate)
			var eDate = dateRange.EndDate.Add(time.Hour * 24)
			dateRange.EndDate = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, LighterThan))
			queryValues = append(queryValues, dateRange.EndDate)
		} else if kind == reflect.String {
			var searchValue string
			if field.Len() > 0 {
				const defaultKey = "contain"
				if key, ok := typeOfValue.Field(i).Tag.Lookup("match"); ok {
					if format, exist := keywordFormat[key]; exist {
						searchValue = fmt.Sprintf(format, interfaceOfField)
					} else {
						log.Panicf("match not support \"%v\" format\n", key)

					}
				} else if format, exist := keywordFormat[defaultKey]; exist {
					searchValue = fmt.Sprintf(format, interfaceOfField)
				}
			} else if len(keyword) > 0 {
				if key, ok := typeOfValue.Field(i).Tag.Lookup("keyword"); ok {
					if format, exist := keywordFormat[key]; exist {
						searchValue = fmt.Sprintf(format, keyword)

					} else {
						log.Panicf("keyword not support \"%v\" format\n", key)
					}
				}
			}
			if len(searchValue) > 0 {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, Like))
				queryValues = append(queryValues, searchValue)
			}
		} else if kind == reflect.Slice {
			if field.Len() > 0 {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s (?)", columnName, In))
				queryValues = append(queryValues, interfaceOfField)
			}
		} else {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, Exact))
			queryValues = append(queryValues, interfaceOfField)
		}
	}

	dynamicQuery := DynamicQuery{RawQuery: strings.Join(rawConditions, " AND "), Value: queryValues, Fields: fields}
	return dynamicQuery
}
