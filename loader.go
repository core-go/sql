package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func BuildFields(modelType reflect.Type) string {
	columns := GetFields(modelType)
	return strings.Join(columns, ",")
}
func GetFields(modelType reflect.Type) []string {
	m := modelType
	if m.Kind() == reflect.Ptr {
		m = m.Elem()
	}
	numField := m.NumField()
	columns := make([]string, 0)
	for idx := 0; idx < numField; idx++ {
		field := m.Field(idx)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							col = str2[j+1]
							columns = append(columns, col)
						}
					}
				}
			}
		}
	}
	return columns
}
func BuildQuery(table string, modelType reflect.Type) string {
	columns := GetFields(modelType)
	return "select " + strings.Join(columns, ",") + " from " + table + " "
}
func InitFields(modelType reflect.Type, db *sql.DB) (map[string]int, string, func(i int) string, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, "", nil, "", err
	}
	fields := BuildFields(modelType)
	if db == nil {
		return fieldsIndex, fields, nil, "", nil
	}
	driver := GetDriver(db)
	buildParam := GetBuild(db)
	return fieldsIndex, fields, buildParam, driver, nil
}

// for Loader
func FindPrimaryKeys(modelType reflect.Type) ([]string, []string) {
	numField := modelType.NumField()
	var idColumnFields []string
	var idJsons []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idColumnFields = append(idColumnFields, k)
					tag1, ok1 := field.Tag.Lookup("json")
					tagJsons := strings.Split(tag1, ",")
					if ok1 && len(tagJsons) > 0 {
						idJsons = append(idJsons, tagJsons[0])
					}
				}
			}
		}
	}
	return idColumnFields, idJsons
}

func findTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}

func MapJsonColumn(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	columnNameKeys := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				if has := strings.Contains(ormTag, "column"); has {
					str1 := strings.Split(ormTag, ";")
					num := len(str1)
					for i := 0; i < num; i++ {
						str2 := strings.Split(str1[i], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								tagj, ok1 := field.Tag.Lookup("json")
								t := strings.Split(tagj, ",")
								if ok1 && len(t) > 0 {
									json := t[0]
									columnNameKeys[json] = str2[j+1]
								}
							}
						}
					}
				}
			}
		}
	}
	return columnNameKeys
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("select * from %v", table)
}

func BuildFindByIdWithDB(db *sql.DB, query string, id interface{}, mapJsonColumnKeys map[string]string, keys []string, options ...func(i int) string) (string, []interface{}) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return BuildFindById(query, buildParam, id, mapJsonColumnKeys, keys)
}

func BuildFindById(selectAll string, buildParam func(i int) string, id interface{}, mapJsonColumnKeys map[string]string, keys []string) (string, []interface{}) {
	var where = ""
	var values []interface{}
	if len(keys) == 1 {
		where = fmt.Sprintf("where %s = %s", mapJsonColumnKeys[keys[0]], buildParam(1))
		values = append(values, id)
	} else {
		conditions := make([]string, 0)
		if ids, ok := id.(map[string]interface{}); ok {
			j := 1
			for _, keyJson := range keys {
				columnName := mapJsonColumnKeys[keyJson]
				if idk, ok1 := ids[keyJson]; ok1 {
					conditions = append(conditions, fmt.Sprintf("%s = %s", columnName, buildParam(j)))
					values = append(values, idk)
					j++
				}
			}
			where = "where " + strings.Join(conditions, " and ")
		}
	}
	return fmt.Sprintf("%s %s", selectAll, where), values
}

func IsNil(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
