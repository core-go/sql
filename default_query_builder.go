package sql

import (
	"fmt"
	model "github.com/common-go/search"
	"github.com/jinzhu/gorm"
	"log"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type DefaultQueryBuilder struct {
}

const (
	Exact            = "="
	Like             = "LIKE"
	GreaterEqualThan = ">="
	LighterEqualThan = "<="
	LighterThan      = "<"
	In               = "IN"
)

func (b *DefaultQueryBuilder) GetColumnNameFromSqlBuilderTag(typeOfField reflect.StructField) *string {
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

func (b *DefaultQueryBuilder) BuildQuery(sm interface{}, modelType reflect.Type, tableName string, sql string) (string, []interface{}) {
	s1 := ""
	rawConditions := make([]string, 0)
	queryValues := make([]interface{}, 0)
	sortString := ""
	fields := make([]string, 0)
	var keyword string
	var keywordFormat map[string]string

	value := reflect.Indirect(reflect.ValueOf(sm))
	typeOfValue := value.Type()
	numField := value.NumField()
	for i := 0; i < numField; i++ {
		field := value.Field(i)
		kind := field.Kind()
		interfaceOfField := field.Interface()
		typeOfField := value.Type().Field(i)

		if v, ok := interfaceOfField.(*model.SearchModel); ok {
			if len(sql) == 0 {
				if len(v.Fields) > 0 {
					for _, key := range v.Fields {
						i, _, columnName := GetFieldByJson(modelType, key)
						if len(columnName) < 0 {
							fields = fields[len(fields):]
							break
						} else if i == -1 {
							columnName = gorm.ToColumnName(key)
						}
						fields = append(fields, columnName)
					}
				}
				if len(fields) > 0 {
					s1 = `SELECT ` + strings.Join(fields, ",") + ` FROM ` + tableName
				} else {
					s1 = `SELECT * FROM ` + tableName
				}
			} else {
				s1 = sql
			}
			if len(v.Sort) > 0 {
				var sort = make([]string, 0)

				sorts := strings.Split(v.Sort, ",")
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
				sortString = ` ORDER BY ` + strings.Join(sort, ",")
			}
		}

		columnName, existCol := GetColumnName(value.Type(), typeOfField.Name)
		if !existCol {
			columnName, _ = GetColumnName(modelType, typeOfField.Name)
		}
		columnNameFromSqlBuilderTag := b.GetColumnNameFromSqlBuilderTag(typeOfField)
		if columnNameFromSqlBuilderTag != nil {
			columnName = *columnNameFromSqlBuilderTag
		}

		if kind == reflect.Ptr && field.IsNil() {
			continue
		} else if v, ok := interfaceOfField.(*model.SearchModel); ok {
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
				keywordFormat = map[string]string{
					"prefix":  "?%",
					"contain": "%?%",
				}
			}
			continue
		} else if dateRange, ok := interfaceOfField.(model.DateRange); ok {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, GreaterEqualThan))
			queryValues = append(queryValues, dateRange.StartDate)
			var eDate = dateRange.EndDate.Add(time.Hour * 24)
			dateRange.EndDate = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, LighterThan))
			queryValues = append(queryValues, dateRange.EndDate)
		} else if dateTime, ok := interfaceOfField.(model.TimeRange); ok {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, GreaterEqualThan))
			queryValues = append(queryValues, dateTime.StartTime)
			var eDate = dateTime.EndTime.Add(time.Hour * 24)
			dateTime.EndTime = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, LighterThan))
			queryValues = append(queryValues, dateTime.EndTime)
		} else if kind == reflect.String {
			var searchValue string
			if field.Len() > 0 {
				const defaultKey = "contain"
				if key, ok := typeOfValue.Field(i).Tag.Lookup("match"); ok {
					if _, exist := keywordFormat[key]; exist {
						searchValue = `?`
						value2, valid := interfaceOfField.(string)
						if !valid {
							log.Panicf("invalid data \"%v\" \n", interfaceOfField)
						}
						//if sql == "mysql" {
						//	value2 = EscapeString(value2)
						//} else if sql == "postgres" || sql == "mssql" {
						//	value2 = EscapeStringForSelect(value2)
						//}
						value2 = value2 + `%`
						queryValues = append(queryValues, value2)
					} else {
						log.Panicf("match not support \"%v\" format\n", key)
					}
				} else if _, exist := keywordFormat[defaultKey]; exist {
					searchValue = `?`
					value2, valid := interfaceOfField.(string)
					if !valid {
						log.Panicf("invalid data \"%v\" \n", interfaceOfField)
					}
					//if sql == "mysql" {
					//	value2 = EscapeString(value2)
					//} else if sql == "postgres" || sql == "mssql" {
					//	value2 = EscapeStringForSelect(value2)
					//}
					value2 = `%` + value2 + `%`
					queryValues = append(queryValues, value2)
				}
			} else if len(keyword) > 0 {
				if key, ok := typeOfValue.Field(i).Tag.Lookup("keyword"); ok {
					if format, exist := keywordFormat[key]; exist {
						//if sql == "mysql" {
						//	keyword = EscapeString(keyword)
						//} else if sql == "postgres" || sql == "mssql" {
						//	keyword = EscapeStringForSelect(keyword)
						//}
						if format == `?%` {
							keyword = keyword + `%`
						} else if format == `%?%` {
							keyword = `%` + keyword + `%`
						} else {
							log.Panicf("keyword not support \"%v\" format\n", key)
						}
						searchValue = `?`
						queryValues = append(queryValues, keyword)
					} else {
						log.Panicf("keyword not support \"%v\" format\n", key)
					}
				}
			}
			if len(searchValue) > 0 {
				if sql == "postgres" {
					rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, `ILIKE`, searchValue))
				} else {
					rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, Like, searchValue))
				}
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
	if len(rawConditions) > 0 {
		return s1 + ` WHERE ` + strings.Join(rawConditions, " AND ") + sortString, queryValues
	}
	return s1 + sortString, queryValues
}
