package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DBName     = "column"
	PrimaryKey = "primary_key"
)

func OpenByConfig(c DatabaseConfig) (*sql.DB, error) {
	if c.Retry.Retry1 <= 0 {
		return open(c)
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		return Open(c, durations...)
	}
}
func open(c DatabaseConfig) (*sql.DB, error) {
	if len(c.DataSourceName) > 0 {
		return sql.Open(c.Provider, c.DataSourceName)
	} else {
		dsn := BuildDataSourceName(c)
		return sql.Open(c.Provider, dsn)
	}
}
func Open(c DatabaseConfig, retries ...time.Duration) (*sql.DB, error) {
	if len(retries) == 0 {
		return open(c)
	} else {
		db, er1 := open(c)
		if er1 == nil {
			return db, er1
		}
		i := 0
		err := Retry(retries, func() (err error) {
			i = i + 1
			db2, er2 := open(c)
			if er2 == nil {
				db = db2
			}
			return er2
		})
		if err != nil {
			log.Printf("Cannot conect to database: %s.", err.Error())
		}
		return db, err
	}
}
func BuildDataSourceName(c DatabaseConfig) string {
	if c.Provider == "postgres" {
		uri := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%d sslmode=disable", c.User, c.Database, c.Password, c.Host, c.Port)
		return uri
	} else if c.Provider == "mysql" {
		uri := ""
		if c.MultiStatements {
			uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local&multiStatements=True", c.User, c.Password, c.Host, c.Port, c.Database)
			return uri
		}
		uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Provider == "mssql" { // mssql
		uri := fmt.Sprintf("sqlserver://%s:%s@%s:%d?Database=%s", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Provider == "godror" || c.Provider == "oracle" {
		return fmt.Sprintf("user=\"%s\" password=\"%s\" connectString=\"%s:%d/%s\"", c.User, c.Password, c.Host, c.Port, c.Database)
	} else { //sqlite
		return c.Host  // return sql.Open("sqlite3", c.Host)
	}
}
// for ViewService

func BuildFindById(table string, ids map[string]interface{}, modelType reflect.Type) (string, []interface{}) {
	numField := modelType.NumField()
	var idFieldNames []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				idFieldNames = append(idFieldNames, field.Name)
			}
		}
	}
	query := make(map[string]interface{})
	if len(idFieldNames) > 1 {
		idsMap := make(map[string]interface{})
		for i := 0; i < len(idFieldNames); i += 2 {
			idsMap[idFieldNames[i]] = idFieldNames[i+1]
		}
		query = BuildQueryByMulId(idsMap, modelType)
	} else {
		query = BuildQueryBySingleId(ids[idFieldNames[0]], modelType, idFieldNames[0])
	}
	var queryArr []string
	var values []interface{}
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", key))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("SELECT * FROM %v WHERE %v", table, q), values
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("SELECT * FROM %v", table)
}

func BuildQueryBySingleId(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}

func BuildQueryByMulId(ids map[string]interface{}, modelType reflect.Type) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var columnName string
	for colName, value := range ids {
		columnName, _ = GetColumnName(modelType, colName)
		queryGen[columnName] = value
	}
	return queryGen
}

func InitSingleResult(modelType reflect.Type) interface{} {
	return reflect.New(modelType).Interface()
}

func InitArrayResults(modelsType reflect.Type) interface{} {
	return reflect.New(modelsType).Interface()
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

//func InsertWithVersion(db *gorm.DB, table string, model interface{}, versionIndex int) (int64, error) {
//	var defaultVersion interface{}
//	modelType := reflect.TypeOf(model).Elem()
//	versionType := modelType.Field(versionIndex).Type
//	switch versionType.String() {
//	case "int":
//		defaultVersion = int(1)
//	case "int32":
//		defaultVersion = int32(1)
//	case "int64":
//		defaultVersion = int64(1)
//	default:
//		panic("not support type's version")
//	}
//	model, err := setValue(model, versionIndex, defaultVersion)
//	if err != nil {
//		return 0, err
//	}
//	return Insert(db, table, model)
//}
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

func Insert(db *sql.DB, table string, model interface{}) (int64, error) {
	queryInsert := BuildInsert(table, model)

	result, err := db.Exec(queryInsert)
	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	return result.RowsAffected()
}

func Update(db *sql.DB, table string, model interface{}) (int64, error) {
	query, values := BuildUpdate(table, model)

	result, err := db.Exec(query, values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Patch(db *sql.DB, table string, model map[string]interface{}, modelType reflect.Type) (int64, error) {
	fieldName, idJsonName := FindNames(modelType)
	driverName := getDriver(db)
	query := BuildPatch(table, model, fieldName, idJsonName, driverName)
	if query == "" {
		return 0, errors.New("fail to build query")
	}
	result, err := db.Exec(query)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func PatchObject(db *gorm.DB, model interface{}, updateModel interface{}) (int64, error) {
	rs := db.Model(model).Updates(updateModel)
	if err := rs.Error; err != nil {
		return rs.RowsAffected, err
	}
	return rs.RowsAffected, nil
}

//func InsertOrUpdate(db *gorm.DB, table string, model interface{}, query map[string]interface{}) (int64, error) {
//	//var queryModel = reflect.New(reflect.ValueOf(model).Elem().Type()).Interface()
//	if isExist, _ := Exists(db, table, model, query); isExist {
//		return Update(db, table, model, query)
//	} else {
//		return Insert(db, table, model)
//	}
//	//db.Table(table).Where(query).Assign(modelNonPointer).FirstOrCreate(&modelNonPointer)
//}

func Delete(db *sql.DB, table string, query map[string]interface{}) (int64, error) {
	queryDelete, values := BuildDelete(table, query)

	result, err := db.Exec(queryDelete, values...)

	if err != nil {
		fmt.Println(err)
		return -1, err
	}
	return BuildResult(result.RowsAffected())
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
func BuildInsert(table string, model interface{}) string {
	mapData, _ := BuildMapDataAndKeys(model)
	var cols []string
	var values []interface{}
	for col, v := range mapData {
		cols = append(cols, QuoteColumnName(col))
		values = append(values, v)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	var arrValue []string
	for i := 0; i < numCol; i++ {
		arrValue = append(arrValue, "?")
	}
	value := fmt.Sprintf("(%v)", strings.Join(arrValue, ","))
	return fmt.Sprintf("INSERT INTO %v %v VALUES %v", table, column, value)
}

func BuildUpdate(table string, model interface{}) (string, []interface{}) {
	mapData, keys := BuildMapDataAndKeys(model)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	idFields := FindIdColumns(modelType)
	query := make(map[string]interface{})
	if len(idFields) > 1 {
		idsMap := make(map[string]interface{})
		for i := 0; i < len(idFields); i += 2 {
			idsMap[idFields[i]] = idFields[i+1]
		}
		query = MapToGORM(idsMap, modelType)
	} else {
		query = BuildQueryById(mapData[idFields[0]], modelType, idFields[0])
	}

	for _, gormColumnName := range idFields {
		if _, exist := Find(idFields, gormColumnName); exist {
			delete(mapData, gormColumnName)
			keys = RemoveItem(keys, gormColumnName)
		}
	}

	var values []interface{}
	var updateQuery []string
	for _, key := range keys {
		if v, ok := mapData[key]; ok {
			values = append(values, v)
			updateQuery = append(updateQuery, fmt.Sprintf("%v=?", QuoteColumnName(key)))
		}
	}

	setValueUpdate := strings.Join(updateQuery, ",")
	var queryArr []string
	for key, value := range query {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", key))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("UPDATE %v SET %v WHERE %v", table, setValueUpdate, q), values
}

func BuildPatch(table string, model map[string]interface{}, idTagJsonNames []string, idColumNames []string, driverName string) string {
	scope := statement()
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			scope.Columns = append(scope.Columns, key)
			scope.Values = append(scope.Values, model[key])
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}

	n := len(scope.Columns)
	sets, err1 := BuildSqlParametersByColumns(scope.Columns, scope.Values, n, 0, driverName, ", ")
	if err1 != nil {
		return ""
	}
	columnsKeys := scope.Keys
	where, err2 := BuildSqlParametersByColumns(columnsKeys, scope.Values, len(columnsKeys), n, driverName, " and ")
	if err2 != nil {
		return ""
	}
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table,
		sets,
		where,
	)
	return query
}

func BuildDelete(table string, ids map[string]interface{}) (string, []interface{}) {

	var values []interface{}
	var queryArr []string
	for key, value := range ids {
		queryArr = append(queryArr, fmt.Sprintf("%v=?", QuoteColumnName(key)))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " AND ")
	return fmt.Sprintf("DELETE FROM %v WHERE %v", table, q), values
}

func HandleResult(result *gorm.DB) (int64, error) {
	if err := result.Error; err != nil {
		return result.RowsAffected, err
	} else {
		return result.RowsAffected, nil
	}
}

// Obtain columns and values required for insert from interface
func ExtractMapValue(value interface{}, excludeColumns []string) (map[string]interface{}, map[string]interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
		value = rv.Interface()
	}
	if rv.Kind() != reflect.Struct {
		return nil, nil, errors.New("value must be kind of Struct")
	}

	var attrs = map[string]interface{}{}
	var attrsKey = map[string]interface{}{}

	for _, field := range GetMapField(value) {
		if !ContainString(excludeColumns, GetTag(field, "fieldName")) && !IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				attrs[dBName] = field.Value.Interface()
			}
		}
		if IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				attrsKey[dBName] = field.Value.Interface()
			}
		}
		// Exclude relational record because it's not directly contained in database columns
		//_, hasForeignKey := field.TagSettingsGet("FOREIGNKEY")
		/*if !ContainString(excludeColumns, field.Struct.Name) && field.StructField.Relationship == nil && !hasForeignKey &&
			!field.IsIgnored && !IsAutoIncrementField(field) && !IsPrimaryAndBlankField(field) {
			if field.StructField.HasDefaultValue && field.IsBlank {
				// If default value presents and field is empty, assign a default value
				if val, ok := field.TagSettingsGet("DEFAULT"); ok {
					attrs[field.DBName] = val
				} else {
				}
			} else {
				attrs[field.DBName] = field.Field.Interface()
			}
		}*/
	}
	return attrs, attrsKey, nil
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

func FindIdColumns(modelType reflect.Type) []string {
	numField := modelType.NumField()
	var idFields = make([]string, 0)
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
								idFields = append(idFields, str2[j+1])
							}
						}
					}
				}
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

func Query(db *sql.DB, results interface{}, sql string, values ...interface{}) error {
	rows, err1 := db.Query(sql, values...)
	if err1 != nil {
		return err1
	}
	defer rows.Close()
	tb, err2 := ScanType(rows, results)
	if err2 != nil {
		return err2
	}
	results = tb
	rerr := rows.Close()
	if rerr != nil {
		return rerr
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

// StructScan : transfer struct to slice for scan
func StructScan(s interface{}) (r []interface{}) {
	if s != nil {
		vals := reflect.ValueOf(s).Elem()
		for i := 0; i < vals.NumField(); i++ {
			r = append(r, vals.Field(i).Addr().Interface())
		}
	}

	return
}

func ScanType(rows *sql.Rows, tb interface{}) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(reflect.TypeOf(tb).Elem()).Interface()
		if err = rows.Scan(StructScan(gTb)...); err == nil {
			t = append(t, gTb)
		}
	}

	return
}

func ScanByModelType(rows *sql.Rows, modelType reflect.Type) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(modelType).Interface()
		if err = rows.Scan(StructScan(gTb)...); err == nil {
			t = append(t, gTb)
		}
	}

	return
}

func Scan(rows *sql.Rows, structType reflect.Type) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(structType).Interface()
		if err = rows.Scan(StructScan(gTb)...); err == nil {
			t = append(t, gTb)
		}
	}

	return
}

func ScanRowType(row *sql.Row, tb interface{}) (t interface{}, err error) {
	t = reflect.New(reflect.TypeOf(tb).Elem()).Interface()
	err = row.Scan(StructScan(t)...)
	return
}

func ScanRow(row *sql.Row, structType reflect.Type) (t interface{}, err error) {
	t = reflect.New(structType).Interface()
	err = row.Scan(StructScan(t)...)
	return
}

func BuildMarkByDriver(number int, driver string) string {
	switch driver {
	case DRIVER_POSTGRES:
		return "$" + strconv.Itoa(number)
	default:
		return "?"
	}
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

func GetTag(field Field, tagName string) string {
	if tag, ok := field.Tags[tagName]; ok {
		return tag
	}
	return ""
}

func IsPrimary(field Field) bool {
	return GetTag(field, PrimaryKey) != ""
}
