package convert

import (
	"encoding/json"
	"reflect"
	"strings"
)

func ToMap(in interface{}, ignoreFields ...string) map[string]interface{} {
	out := make(map[string]interface{})
	v := reflect.ValueOf(in)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	typ := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		fv := f.Interface()
		k := f.Kind()
		if k == reflect.Ptr {
			if f.IsNil() {
				continue
			} else {
				fv = reflect.Indirect(reflect.ValueOf(fv)).Interface()
			}
		} else if k == reflect.Slice {
			if f.IsNil() {
				continue
			}
		}
		n := getTag(typ.Field(i), "json")
		out[n] = fv
	}
	for _, v := range ignoreFields {
		if _, ok := out[v]; ok {
			delete(out, v)
		}
	}
	return out
}
func getTag(fi reflect.StructField, tag string) string {
	if tagv := fi.Tag.Get(tag); tagv != "" {
		arrValue := strings.Split(tagv, ",")
		if len(arrValue) > 0 {
			return arrValue[0]
		} else {
			return tagv
		}
	}
	return fi.Name
}
func ToObject(ms map[string]interface{}, result interface{}) error {
	bytes, err := json.Marshal(ms)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, result)
}
