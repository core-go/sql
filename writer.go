package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func Begin(ctx context.Context, db *sql.DB, opts ...*sql.TxOptions) (context.Context, *sql.Tx, error) {
	var tx *sql.Tx
	var err error
	if len(opts) > 0 && opts[0] != nil {
		tx, err = db.BeginTx(ctx, opts[0])
	} else {
		tx, err = db.Begin()
	}
	if err != nil {
		return ctx, tx, err
	} else {
		c2 := context.WithValue(ctx, txs, tx)
		return c2, tx, nil
	}
}
func Commit(tx *sql.Tx, err error, options ...bool) error {
	if err != nil {
		if !(len(options) > 0 && options[0] == false) {
			tx.Rollback()
		}
		return err
	}
	return tx.Commit()
}
func Rollback(tx *sql.Tx, err error, options ...int64) (int64, error) {
	tx.Rollback()
	if len(options) > 0 {
		return options[0], err
	}
	return -1, err
}
func End(tx *sql.Tx, res int64, err error, options ...bool) (int64, error) {
	er := Commit(tx, err, options...)
	return res, er
}
func Init(modelType reflect.Type, db *sql.DB) (map[string]int, *Schema, map[string]string, []string, []string, string, func(i int) string, string, error) {
	fieldsIndex, err := GetColumnIndexes(modelType)
	if err != nil {
		return nil, nil, nil, nil, nil, "", nil, "", err
	}
	schema := CreateSchema(modelType)
	fields := BuildFieldsBySchema(schema)
	jsonColumnMap := MakeJsonColumnMap(modelType)
	jm := GetWritableColumns(schema.Fields, jsonColumnMap)
	keys, arr := FindPrimaryKeys(modelType)
	if db == nil {
		return fieldsIndex, schema, jm, keys, arr, fields, nil, "", nil
	}
	driver := GetDriver(db)
	buildParam := GetBuild(db)
	return fieldsIndex, schema, jm, keys, arr, fields, buildParam, driver, nil
}
func CreateParameters(modelType reflect.Type, db *sql.DB) (*Parameters, error) {
	fieldsIndex, schema, jsonColumnMap, keys, _, fields, buildParam, _, err := Init(modelType, db)
	if err != nil {
		return nil, err
	}
	return &Parameters{DB: db, ModelType: modelType, Map: fieldsIndex, Schema: schema, JsonColumnMap: jsonColumnMap, Keys: keys, Fields: fields, BuildParam: buildParam}, nil
}

type Parameters struct {
	DB            *sql.DB
	ModelType     reflect.Type
	Map           map[string]int
	Fields        string
	Keys          []string
	Schema        *Schema
	JsonColumnMap map[string]string
	BuildParam    func(int) string
}

func BuildToDelete(table string, ids map[string]interface{}, buildParam func(int) string) (string, []interface{}) {
	var args []interface{}
	var where []string
	i := 1
	for col, value := range ids {
		where = append(where, col+"="+buildParam(i))
		args = append(args, value)
		i++
	}
	return fmt.Sprintf("delete from %v where %v", table, strings.Join(where, " and ")), args
}
func MapToDB(model *map[string]interface{}, modelType reflect.Type) {
	for colName, value := range *model {
		if boolValue, boolOk := value.(bool); boolOk {
			index := GetIndexByTag("json", colName, modelType)
			if index > -1 {
				valueS := modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				valueInt, err := strconv.Atoi(valueS)
				if err != nil {
					(*model)[colName] = valueS
				} else {
					(*model)[colName] = valueInt
				}
				continue
			}
		}
		(*model)[colName] = value
	}
}
func BuildQueryById(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}

// For ViewDefaultRepository
func GetColumnName(modelType reflect.Type, jsonName string) (col string, colExist bool) {
	index := GetIndexByTag("json", jsonName, modelType)
	if index == -1 {
		return jsonName, false
	}
	field := modelType.Field(index)
	ormTag, ok2 := field.Tag.Lookup("gorm")
	if !ok2 {
		return "", true
	}
	if has := strings.Contains(ormTag, "column"); has {
		str1 := strings.Split(ormTag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	return jsonName, false
}
func GetIndexByTag(tag, key string, modelType reflect.Type) (index int) {
	for i := 0; i < modelType.NumField(); i++ {
		f := modelType.Field(i)
		v := strings.Split(f.Tag.Get(tag), ",")[0]
		if v == key {
			return i
		}
	}
	return -1
}
func MakeJsonColumnMap(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	mapJsonColumn := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if ok {
			tag1, ok1 := field.Tag.Lookup("json")
			tagJsons := strings.Split(tag1, ",")
			if ok1 && len(tagJsons) > 0 {
				mapJsonColumn[tagJsons[0]] = column
			}
		}
	}
	return mapJsonColumn
}
func FindFieldIndex(modelType reflect.Type, fieldName string) int {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			return i
		}
	}
	return -1
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
func JSONToColumns(model map[string]interface{}, m map[string]string) map[string]interface{} {
	if model == nil || m == nil {
		return model
	}
	r := make(map[string]interface{})
	for k, v := range model {
		col, ok := m[k]
		if ok {
			r[col] = v
		}
	}
	return r
}
func GetWritableColumns(fields map[string]*FieldDB, jsonColumnMap map[string]string) map[string]string {
	m := jsonColumnMap
	for k, v := range jsonColumnMap {
		for _, db := range fields {
			if db.Column == v {
				if db.Update == false && db.Key == false {
					delete(m, k)
				}
			}
		}
	}
	return m
}

func Contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}
func BuildToPatch(table string, model map[string]interface{}, keyColumns []string, buildParam func(int) string, options ...map[string]*FieldDB) (string, []interface{}) {
	return BuildToPatchWithVersion(table, model, keyColumns, buildParam, nil, "", options...)
}
func BuildToPatchWithArray(table string, model map[string]interface{}, keyColumns []string, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...map[string]*FieldDB) (string, []interface{}) {
	return BuildToPatchWithVersion(table, model, keyColumns, buildParam, toArray, "", options...)
}

func BuildToPatchWithVersion(table string, model map[string]interface{}, keyColumns []string, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, version string, options ...map[string]*FieldDB) (string, []interface{}) { //version column name db
	var schema map[string]*FieldDB
	if len(options) > 0 {
		schema = options[0]
	}
	values := make([]string, 0)
	where := make([]string, 0)
	args := make([]interface{}, 0)
	i := 1
	for col, v := range model {
		if !Contains(keyColumns, col) && col != version {
			if v == nil {
				values = append(values, col+"=null")
			} else {
				v2, ok2 := GetDBValue(v, false, -1)
				if ok2 {
					values = append(values, col+"="+v2)
				} else {
					if boolValue, ok3 := v.(bool); ok3 {
						handled := false
						if schema != nil {
							fdb, ok4 := schema[col]
							if ok4 {
								if boolValue {
									if fdb.True != nil {
										values = append(values, col+"="+buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, col+"='1'")
									}
								} else {
									if fdb.False != nil {
										values = append(values, col+"="+buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, col+"='0'")
									}
								}
								handled = true
							}
						}
						if handled == false {
							if boolValue {
								values = append(values, col+"='1'")
							} else {
								values = append(values, col+"='0'")
							}
						}
					} else {
						values = append(values, col+"="+buildParam(i))
						i = i + 1
						if toArray != nil && reflect.TypeOf(v).Kind() == reflect.Slice {
							args = append(args, toArray(v))
						} else {
							args = append(args, v)
						}
					}
				}
			}
		}
	}
	for _, col := range keyColumns {
		v0, ok0 := model[col]
		if ok0 {
			v, ok1 := GetDBValue(v0, false, -1)
			if ok1 {
				where = append(where, col+"="+v)
			} else {
				where = append(where, col+"="+buildParam(i))
				i = i + 1
				args = append(args, v0)
			}
		}
	}
	if len(version) > 0 {
		v0, ok0 := model[version]
		if ok0 {
			switch v4 := v0.(type) {
			case int:
				values = append(values, version+"="+strconv.Itoa(v4+1))
				where = append(where, version+"="+strconv.Itoa(v4))
			case int32:
				v5 := int64(v4)
				values = append(values, version+"="+strconv.FormatInt(v5+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v5, 10))
			case int64:
				values = append(values, version+"="+strconv.FormatInt(v4+1, 10))
				where = append(where, version+"="+strconv.FormatInt(v4, 10))
			}
		}
	}
	query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, " and "))
	return query, args
}
