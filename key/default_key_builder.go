package key

import (
	"fmt"
	"reflect"
	"strings"
)

type DefaultKeyBuilder struct {
	PositionPrimaryKeysMap map[reflect.Type][]int
}

func NewDefaultKeyBuilder() *DefaultKeyBuilder {
	return &DefaultKeyBuilder{PositionPrimaryKeysMap: make(map[reflect.Type][]int)}
}

func (b *DefaultKeyBuilder) getPositionPrimaryKeys(modelType reflect.Type) []int {
	if b.PositionPrimaryKeysMap[modelType] == nil {
		var positions []int

		numField := modelType.NumField()
		for i := 0; i < numField; i++ {
			gorm := strings.Split(modelType.Field(i).Tag.Get("gorm"), ";")
			for _, value := range gorm {
				if value == "primary_key" {
					positions = append(positions, i)
					break
				}
			}
		}

		b.PositionPrimaryKeysMap[modelType] = positions
	}

	return b.PositionPrimaryKeysMap[modelType]
}

func (b *DefaultKeyBuilder) BuildKey(object interface{}) string {
	ids := make(map[string]interface{})
	objectValue := reflect.Indirect(reflect.ValueOf(object))
	positions := b.getPositionPrimaryKeys(objectValue.Type())
	var values []string
	for _, position := range positions {
		if _, colName, ok := GetFieldByIndex(objectValue.Type(), position); ok {
			ids[colName] = fmt.Sprint(objectValue.Field(position).Interface())
			values = append(values, fmt.Sprint(objectValue.Field(position).Interface()))
		}
	}
	return strings.Join(values, "-")
}

func (b *DefaultKeyBuilder) BuildKeyFromMap(keyMap map[string]interface{}, idNames []string) string {
	var values []string
	for _, key := range idNames {
		if keyVal, exist := keyMap[key]; !exist {
			values = append(values, "")
		} else {
			str, ok := keyVal.(string)
			if !ok {
				return ""
			}
			values = append(values, str)
		}
	}
	return strings.Join(values, "-")
}
func GetFieldByIndex(ModelType reflect.Type, index int) (json string, col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		json = fields.Name
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					jTag, jOk := fields.Tag.Lookup("json")
					if jOk {
						tagJsons := strings.Split(jTag, ",")
						json = tagJsons[0]
					}
					return json, str2[j+1], true
				}
			}
		}
	}
	return "", "", false
}
