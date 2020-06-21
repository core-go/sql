package sql

import (
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"reflect"
	"sort"
	"strings"
)

func CreatePool(dbConfig DatabaseConfig) (*gorm.DB, error) {
	if dbConfig.Dialect == "postgres" {
		uri := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%d sslmode=disable", dbConfig.User, dbConfig.Database, dbConfig.Password, dbConfig.Host, dbConfig.Port)
		fmt.Println("uri ", uri)
		return gorm.Open("postgres", uri)
	} else if dbConfig.Dialect == "mysql" {
		uri := ""
		if dbConfig.MultiStatements {
			uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local&multiStatements=True", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
			return gorm.Open("mysql", uri)
		}
		uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
		return gorm.Open("mysql", uri)
	} else if dbConfig.Dialect == "mssql" { // mssql
		uri := fmt.Sprintf("sqlserver://%s:%s@%s:%d?Database=%s", dbConfig.User, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.Database)
		return gorm.Open("mssql", uri)
	} else { //sqlite
		return gorm.Open("sqlite3", dbConfig.Host)
	}
}

func Connect(dialect string, uri string) (*gorm.DB, error) {
	return gorm.Open(dialect, uri)
}

func Exists(db *gorm.DB, table string, model interface{}, query interface{}) (bool, error) {
	var count int32
	if err := db.Table(table).Where(query).Count(&count).Error; err != nil {
		return false, err
	} else {
		if count >= 1 {
			return true, nil
		}
		return false, nil
	}
}

func InsertWithVersion(db *gorm.DB, table string, model interface{}, versionIndex int) (int64, error) {
	var defaultVersion interface{}
	modelType := reflect.TypeOf(model).Elem()
	versionType := modelType.Field(versionIndex).Type
	switch versionType.String() {
	case "int":
		defaultVersion = int(1)
	case "int32":
		defaultVersion = int32(1)
	case "int64":
		defaultVersion = int64(1)
	default:
		panic("not support type's version")
	}
	model, err := setValue(model, versionIndex, defaultVersion)
	if err != nil {
		return 0, err
	}
	return Insert(db, table, model)
}
func setValue(model interface{}, index int, value interface{}) (interface{}, error) {
	valueObject := reflect.Indirect(reflect.ValueOf(model))
	switch reflect.ValueOf(model).Kind() {
	case reflect.Ptr:
		{
			valueObject.Field(index).Set(reflect.ValueOf(value))
			return model, nil
		}
	default:
		if modelWithTypeValue, ok := model.(reflect.Value); ok {
			_, err := setValueWithTypeValue(modelWithTypeValue, index, value)
			return modelWithTypeValue.Interface(), err
		}
	}
	return model, nil
}
func setValueWithTypeValue(model reflect.Value, index int, value interface{}) (reflect.Value, error) {
	trueValue := reflect.Indirect(model)
	switch trueValue.Kind() {
	case reflect.Struct:
		{
			val := reflect.Indirect(reflect.ValueOf(value))
			if trueValue.Field(index).Kind() == val.Kind() {
				trueValue.Field(index).Set(reflect.ValueOf(value))
				return trueValue, nil
			} else {
				return trueValue, fmt.Errorf("value's kind must same as field's kind")
			}
		}
	default:
		return trueValue, nil
	}
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

func Insert(db *gorm.DB, table string, model interface{}) (int64, error) {
	x := db.Table(table).Create(model)
	if err := x.Error; err != nil {
		dialect := db.Dialect().GetName()
		ers := err.Error() // log.Printf(err.Error())
		if dialect == "postgres" && strings.Contains(ers, "pq: duplicate key value violates unique constraint") {
			return -1, err //pq: duplicate key value violates unique constraint "aa_pkey"
		} else if dialect == "mysql" && strings.Contains(ers, "Error 1062: Duplicate entry") {
			return -1, err //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if dialect == "mssql" && strings.Contains(ers, "Violation of PRIMARY KEY constraint") {
			return -1, err //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else if dialect == "sqlite3" && strings.Contains(ers, "UNIQUE constraint failed:") {
			return -1, err //UNIQUE constraint failed: aa.interestWaning, aa.skillIncrease
		} else {
			return 0, err
		}
	} else {
		return x.RowsAffected, nil
	}
}

func Update(db *gorm.DB, table string, model interface{}, query interface{}) (int64, error) {
	result := db.Table(table).Where(query).Save(model)
	if err := result.Error; err != nil {
		return result.RowsAffected, err
	} else {
		return result.RowsAffected, nil
	}
}

func Patch(db *gorm.DB, table string, model map[string]interface{}, query map[string]interface{}) (int64, error) {
	result := db.Table(table).Where(query).Updates(model)
	if err := result.Error; err != nil {
		return result.RowsAffected, err
	}
	return result.RowsAffected, nil
}

func PatchObject(db *gorm.DB, model interface{}, updateModel interface{}) (int64, error) {
	rs := db.Model(model).Updates(updateModel)
	if err := rs.Error; err != nil {
		return rs.RowsAffected, err
	}
	return rs.RowsAffected, nil
}

func InsertOrUpdate(db *gorm.DB, table string, model interface{}, query map[string]interface{}) (int64, error) {
	//var queryModel = reflect.New(reflect.ValueOf(model).Elem().Type()).Interface()
	if isExist, _ := Exists(db, table, model, query); isExist {
		return Update(db, table, model, query)
	} else {
		return Insert(db, table, model)
	}
	//db.Table(table).Where(query).Assign(modelNonPointer).FirstOrCreate(&modelNonPointer)
}

func Delete(db *gorm.DB, table string, results interface{}, query interface{}) (int64, error) {
	if err := db.Table(table).First(results, query).Error; err != nil {
		return 0, err
	} else {
		db.Table(table).Where(query).Delete(&results)
		return 1, nil
	}
}

func FindOneWithResult(db *gorm.DB, table string, result interface{}, query interface{}) (bool, error) {
	err := db.Table(table).Set("gorm:auto_preload", true).First(result, query).Error
	if err == nil {
		return true, nil
	}
	if err.Error() == "record not found" { //record not found
		return false, err
	}
	return true, err
}

func FindWithResults(db *gorm.DB, table string, results interface{}, query ...interface{}) error {
	if err := db.Table(table).Set("gorm:auto_preload", true).Find(results, query...).Error; err == nil {
		return nil
	} else {
		return err
	}
}

func GetFieldByJson(modelType reflect.Type, jsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonName {
			if tag2, ok2 := field.Tag.Lookup("gorm"); ok2 {
				if has := strings.Contains(tag2, "column"); has {
					str1 := strings.Split(tag2, ";")
					num := len(str1)
					for k := 0; k < num; k++ {
						str2 := strings.Split(str1[k], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								return i, field.Name, str2[j+1]
							}
						}
					}
				}
			}
			return i, field.Name, ""
		}
	}
	return -1, jsonName, jsonName
}

func HandleResult(result *gorm.DB) (int64, error) {
	if err := result.Error; err != nil {
		return result.RowsAffected, err
	} else {
		return result.RowsAffected, nil
	}
}

// Obtain columns and values required for insert from interface
func ExtractMapValue(value interface{}, excludeColumns []string) (map[string]interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
		value = rv.Interface()
	}
	if rv.Kind() != reflect.Struct {
		return nil, errors.New("value must be kind of Struct")
	}

	var attrs = map[string]interface{}{}

	for _, field := range (&gorm.Scope{Value: value}).Fields() {
		// Exclude relational record because it's not directly contained in database columns
		_, hasForeignKey := field.TagSettingsGet("FOREIGNKEY")

		if !ContainString(excludeColumns, field.Struct.Name) && field.StructField.Relationship == nil && !hasForeignKey &&
			!field.IsIgnored && !IsAutoIncrementField(field) && !IsPrimaryAndBlankField(field) {
			if field.StructField.HasDefaultValue && field.IsBlank {
				// If default value presents and field is empty, assign a default value
				if val, ok := field.TagSettingsGet("DEFAULT"); ok {
					attrs[field.DBName] = val
				} else {
					attrs[field.DBName] = field.Field.Interface()
				}
			} else {
				attrs[field.DBName] = field.Field.Interface()
			}
		}
	}
	return attrs, nil
}

// For ViewDefaultRepository
func GetColumnName(modelType reflect.Type, fieldName string) (col string, colExist bool) {
	field, ok := modelType.FieldByName(fieldName)
	if !ok {
		return fieldName, false
		//return gorm.ToColumnName(fieldName), false
	}
	tag2, ok2 := field.Tag.Lookup("gorm")
	if !ok2 {
		return "", true
	}

	if has := strings.Contains(tag2, "column"); has {
		str1 := strings.Split(tag2, ";")
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
	//return gorm.ToColumnName(fieldName), false
	return fieldName, false
}

func GetColumnNameByIndex(ModelType reflect.Type, index int) (col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
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
	return "", false
}

func FindFieldByName(modelType reflect.Type, fieldName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			name1 := fieldName
			name2 := fieldName
			tag1, ok1 := field.Tag.Lookup("json")
			tag2, ok2 := field.Tag.Lookup("gorm")
			if ok1 {
				name1 = strings.Split(tag1, ",")[0]
			}
			if ok2 {
				if has := strings.Contains(tag2, "column"); has {
					str1 := strings.Split(tag2, ";")
					num := len(str1)
					for k := 0; k < num; k++ {
						str2 := strings.Split(str1[k], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								return i, name1, str2[j+1]
							}
						}
					}
				}
			}
			return i, name1, name2
		}
	}
	return -1, fieldName, fieldName
}

func FindIdFields(modelType reflect.Type) []string {
	numField := modelType.NumField()
	var idFields []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				idFields = append(idFields, field.Name)
			}
		}
	}
	return idFields
}

func BuildQueryMap(db *gorm.DB, object interface{}, onlyPrimaryKeys bool) map[string]interface{} {
	objectValue := reflect.Indirect(reflect.ValueOf(object))
	modelType := objectValue.Type()

	query := map[string]interface{}{}
	newScope := db.NewScope(object)

	for _, field := range newScope.Fields() {
		if !field.IsIgnored && !field.IsBlank {
			if !onlyPrimaryKeys || field.IsPrimaryKey {
				columnName, _ := GetColumnName(modelType, field.Name)
				query[columnName] = field.Field.Interface()
			}
		}
	}

	return query
}

func BuildQueryById(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}

func MapToGORM(ids map[string]interface{}, modelType reflect.Type) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var columnName string
	for colName, value := range ids {
		columnName, _ = GetColumnName(modelType, colName)
		queryGen[columnName] = value
	}
	return queryGen
}

// For DefaultGenericService
func BuildQueryByIdFromObject(object interface{}, modelType reflect.Type, idNames []string) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var value interface{}
	for _, colId := range idNames {
		value = reflect.Indirect(reflect.ValueOf(object)).FieldByName(colId).Interface()
		queryGen[colId] = value
	}
	return MapToGORM(queryGen, modelType)
}

func BuildQueryByIdFromMap(object map[string]interface{}, modelType reflect.Type, idNames []string) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	//var value interface{}
	for _, colId := range idNames {
		queryGen[colId] = object[colId]
	}
	return MapToGORM(queryGen, modelType)
}

// For Search
func GetSqlBuilderTags(modelType reflect.Type) []QueryType {
	numField := modelType.NumField()
	//queries := make([]QueryType, 0)
	var sqlQueries []QueryType
	for i := 0; i < numField; i++ {
		sqlQuery := QueryType{}
		field := modelType.Field(i)
		sqlTags := field.Tag.Get("sql_builder")
		tags := strings.Split(sqlTags, ";")
		for _, tag := range tags {
			key := strings.Split(tag, ":")
			switch key[0] {
			case "join":
				sqlQuery.Join = key[1]
				break
			case "select":
				sqlQuery.Select = key[1]
				break
			case "select_count":
				sqlQuery.SelectCount = key[1]
			}
		}
		if sqlQuery.Select != "" || sqlQuery.Join != "" || sqlQuery.SelectCount != "" {
			sqlQueries = append(sqlQueries, sqlQuery)
		}
	}
	return sqlQueries
}

func MapColumnToJson(query map[string]interface{}) interface{} {
	result := make(map[string]interface{})
	for k, v := range query {
		dem := strings.Count(k, "_")
		for i := 0; i < dem; i++ {
			if strings.Index(k, "_") > -1 {
				hoa := []rune(strings.ToUpper(string(k[strings.Index(k, "_")+1])))
				k = replaceAtIndex(k, hoa[0], strings.Index(k, "_")+1)
				k = strings.Replace(k, "_", "", 1)
			}
		}
		result[k] = v
	}
	return result
}
func replaceAtIndex(str string, replacement rune, index int) string {
	out := []rune(str)
	out[index] = replacement
	return string(out)
}

func GetTableName(object interface{}) string {
	objectValue := reflect.Indirect(reflect.ValueOf(object))
	tableName := objectValue.MethodByName("TableName").Call([]reflect.Value{})
	return tableName[0].String()
}

func UpdateAssociations(db *gorm.DB, obj interface{}, column string, newValues interface{}) error {
	return db.Model(obj).Association(column).Replace(newValues).Error
}

func QueryOne(db *gorm.DB, model interface{}, sql string, values ...interface{}) error {
	return db.Raw(sql, values...).Scan(model).Error
}

func Query(db *gorm.DB, list interface{}, sql string, values ...interface{}) error {
	return db.Raw(sql, values...).Find(list).Error
}

func EscapeString(value string) string {
	//replace := map[string]string{"'": `\'`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	//if strings.Contains(value, `\\`) {
	//	value = strings.Replace(value, "\\", "\\\\", -1)
	//}
	//for b, a := range replace {
	//	if strings.Contains(value, b) {
	//		value = strings.Replace(value, b, a, -1)
	//	}
	//}
	return strings.NewReplacer("\\", "\\\\", "'", `\'`, "\\0", "\\\\0", "\n", "\\n", "\r", "\\r", `"`, `\"`, "\x1a", "\\Z" /*We have more here*/).Replace(value)
}

func EscapeStringForSelect(value string) string {
	//replace := map[string]string{"'": `''`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	//if strings.Contains(value, `\\`) {
	//	value = strings.Replace(value, "\\", "\\\\", -1)
	//}
	//
	//for b, a := range replace {
	//	if strings.Contains(value, b) {
	//		value = strings.Replace(value, b, a, -1)
	//	}
	//}
	return strings.NewReplacer("'", `''` /*We have more here*/).Replace(value)
}

// Check if string value is contained in slice
func ContainString(s []string, value string) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}

// Enable map keys to be retrieved in same order when iterating
func SortedKeys(val map[string]interface{}) []string {
	var keys []string
	for key := range val {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func IsAutoIncrementField(field *gorm.Field) bool {
	if value, ok := field.TagSettingsGet("AUTO_INCREMENT"); ok {
		return strings.ToLower(value) != "false"
	}
	return false
}

func IsPrimaryAndBlankField(field *gorm.Field) bool {
	return field.IsPrimaryKey && field.IsBlank
}
