package transform

import (
	"encoding/json"
	"reflect"
	"strings"
)

func ToMap(model interface{}, checkOmit bool, ignoreFields ...string) map[string]interface{} {
	modelType := reflect.TypeOf(model)
	modelValue := reflect.ValueOf(model)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
		modelValue = modelValue.Elem()
	}
	numFields := modelType.NumField()
	fields := make(map[string]interface{})
	for i := 0; i < numFields; i++ {
		tag, ok := modelType.Field(i).Tag.Lookup("json")
		if ok {
			name := strings.Split(tag, ",")
			if checkOmit {
				if !modelValue.Field(i).IsZero() {
					fields[name[0]] = modelValue.Field(i).Interface()
				}
			} else {
				fields[name[0]] = modelValue.Field(i).Interface()
			}

		}
	}
	for _, v := range ignoreFields {
		if _, ok := fields[v]; ok {
			delete(fields, v)
		}
	}
	return fields
}

func ToObject(ms map[string]interface{}, result interface{}) error {
	bytes, err := json.Marshal(ms)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, result)
}
