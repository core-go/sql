package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DBName          = "column"
	PrimaryKey      = "primary_key"
	IgnoreReadWrite = "-"

	DriverPostgres   = "postgres"
	DriverMysql      = "mysql"
	DriverMssql      = "mssql"
	DriverOracle     = "oracle"
	DriverNotSupport = "no support"
)

func OpenByConfig(c DatabaseConfig) (*sql.DB, error) {
	if c.Mock {
		return nil, nil
	}
	if c.Retry.Retry1 <= 0 {
		return open(c)
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		return Open(c, durations...)
	}
}
func open(c DatabaseConfig) (*sql.DB, error) {
	dsn := c.DataSourceName
	if len(dsn) == 0 {
		dsn = BuildDataSourceName(c)
	}
	db, err := sql.Open(c.Provider, dsn)
	if err != nil {
		return db, err
	}
	if c.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(c.ConnMaxLifetime) * time.Second)
	}
	if c.MaxIdleConns > 0 {
		db.SetMaxIdleConns(c.MaxIdleConns)
	}
	if c.MaxOpenConns > 0 {
		db.SetMaxOpenConns(c.MaxOpenConns)
	}
	return db, err
}
func Open(c DatabaseConfig, retries ...time.Duration) (*sql.DB, error) {
	if c.Mock {
		return nil, nil
	}
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
		return c.Host // return sql.Open("sqlite3", c.Host)
	}
}

// for ViewService

func BuildFindById(db *sql.DB, table string, id interface{}, mapJsonColumnKeys map[string]string, keys []string) (string, []interface{}) {
	var where = ""
	var driver = GetDriver(db)
	var values []interface{}
	if len(keys) == 1 {
		where = fmt.Sprintf("where %s = %s", mapJsonColumnKeys[keys[0]], BuildParam(1, driver))
		values = append(values, id)
	} else {
		queres := make([]string, 0)
		if ids, ok := id.(map[string]interface{}); ok {
			j := 0
			for _, keyJson := range keys {
				columnName := mapJsonColumnKeys[keyJson]
				if idk, ok1 := ids[keyJson]; ok1 {
					queres = append(queres, fmt.Sprintf("%s = %s", columnName, BuildParam(j, driver)))
					values = append(values, idk)
					j++
				}
			}
			where = "where " + strings.Join(queres, " and ")
		}
	}
	return fmt.Sprintf("select * from %v %v", table, where), values
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("select * from %v", table)
}

func InitSingleResult(modelType reflect.Type) interface{} {
	return reflect.New(modelType).Interface()
}

func InitArrayResults(modelsType reflect.Type) interface{} {
	return reflect.New(modelsType).Interface()
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

func Insert(db *sql.DB, table string, model interface{}) (int64, error) {
	var driverName = GetDriver(db)
	queryInsert, values := BuildInsertSql(table, model, 0, driverName)

	result, err := db.Exec(queryInsert, values...)
	if err != nil {
		if err != nil {
			return handleDuplicate(db, err)
		}
	}
	return result.RowsAffected()
}

func handleDuplicate(db *sql.DB, err error) (int64, error) {
	x := err.Error()
	driverName := GetDriver(db)
	if driverName == DriverPostgres && strings.Contains(x, "pq: duplicate key value violates unique constraint") {
		return 0, nil //pq: duplicate key value violates unique constraint "aa_pkey"
	} else if driverName == DriverMysql && strings.Contains(x, "Error 1062: Duplicate entry") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driverName == DriverOracle && strings.Contains(x, "ORA-00001: unique constraint") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driverName == DriverMssql && strings.Contains(x, "Violation of PRIMARY KEY constraint") {
		return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
	}
	return 0, err
}

func InsertTx(db *sql.DB, tx *sql.Tx, table string, model interface{}) (int64, error) {
	var driverName = GetDriver(db)
	queryInsert, values := BuildInsertSql(table, model, 0, driverName)
	result, err := tx.Exec(queryInsert, values...)
	if err != nil {
		return handleDuplicate(db, err)
	}
	return result.RowsAffected()
}

func InsertWithVersion(db *sql.DB, table string, model interface{}, versionIndex int) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version index not found")
	}

	var driverName = GetDriver(db)
	queryInsert, values := BuildInsertSqlWithVersion(table, model, 0, driverName, versionIndex)

	result, err := db.Exec(queryInsert, values...)
	if err != nil {
		errstr := err.Error()
		driverName := GetDriver(db)
		if driverName == DriverPostgres && strings.Contains(errstr, "pq: duplicate key value violates unique constraint") {
			return 0, nil //pq: duplicate key value violates unique constraint "aa_pkey"
		} else if driverName == DriverMysql && strings.Contains(errstr, "Error 1062: Duplicate entry") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driverName == DriverOracle && strings.Contains(errstr, "ORA-00001: unique constraint") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driverName == DriverMssql && strings.Contains(errstr, "Violation of PRIMARY KEY constraint") {
			return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else {
			return 0, err
		}
	}
	return result.RowsAffected()
}

func Exec(stmt *sql.Stmt, values ...interface{}) (int64, error) {
	result, err := stmt.Exec(values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Update(db *sql.DB, table string, model interface{}) (int64, error) {
	driverName := GetDriver(db)
	query, values := BuildUpdateSql(table, model, 0, driverName)
	r, err0 := db.Exec(query, values...)
	if err0 != nil {
		return -1, err0
	}
	return r.RowsAffected()
}

func UpdateTx(db *sql.DB, tx *sql.Tx, table string, model interface{}) (int64, error) {
	driverName := GetDriver(db)
	query, values := BuildUpdateSql(table, model, 0, driverName)
	r, err0 := tx.Exec(query, values...)
	if err0 != nil {
		return -1, err0
	}
	return r.RowsAffected()
}

func UpdateWithVersion(db *sql.DB, table string, model interface{}, versionIndex int) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version's index not found")
	}

	driverName := GetDriver(db)
	query, values := BuildUpdateSqlWithVersion(table, model, 0, driverName, versionIndex)

	result, err := db.Exec(query, values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Patch(db *sql.DB, table string, model map[string]interface{}, modelType reflect.Type) (int64, error) {
	idcolumNames, idJsonName := FindNames(modelType)
	columNames := FindJsonName(modelType)
	driverName := GetDriver(db)
	query, value := BuildPatch(table, model, columNames, idJsonName, idcolumNames, driverName)
	if query == "" {
		return 0, errors.New("fail to build query")
	}
	result, err := db.Exec(query, value...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func PatchWithVersion(db *sql.DB, table string, model map[string]interface{}, modelType reflect.Type, versionIndex int) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version's index not found")
	}

	idcolumNames, idJsonName := FindNames(modelType)
	columNames := FindJsonName(modelType)
	driverName := GetDriver(db)
	versionJsonName, ok := GetJsonNameByIndex(modelType, versionIndex)
	if !ok {
		return 0, errors.New("version's json not found")
	}
	versionColName, ok := GetColumnNameByIndex(modelType, versionIndex)
	if !ok {
		return 0, errors.New("version's column not found")
	}

	query, value := BuildPatchWithVersion(table, model, columNames, idJsonName, idcolumNames, driverName, versionIndex, versionJsonName, versionColName)
	if query == "" {
		return 0, errors.New("fail to build query")
	}
	result, err := db.Exec(query, value...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Delete(db *sql.DB, table string, query map[string]interface{}) (int64, error) {
	driverName := GetDriver(db)
	queryDelete, values := BuildDelete(table, query, driverName)

	result, err := db.Exec(queryDelete, values...)

	if err != nil {
		return -1, err
	}
	return BuildResult(result.RowsAffected())
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

func BuildUpdateSql(table string, model interface{}, i int, driverName string) (string, []interface{}) {
	mapData, mapKey, _ := BuildMapDataAndKeys(model, true)
	var values []interface{}

	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for colName, v1 := range mapData {
		if v1 != nil {
			values = append(values, v1)
			colSet = append(colSet, fmt.Sprintf("%v = "+BuildParam(colNumber+i, driverName), colName))
			colNumber++
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}

	for colName, v2 := range mapKey {
		values = append(values, v2)
		colQuery = append(colQuery, fmt.Sprintf("%v="+BuildParam(colNumber+i, driverName), QuoteColumnName(colName)))
		colNumber++
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}

func BuildUpdateSqlWithVersion(table string, model interface{}, i int, driverName string, versionIndex int) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")

	}

	valueOfModel := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
	nextVersion := currentVersion + 1
	_, err := setValue(model, versionIndex, &nextVersion)
	if err != nil {
		panic(err)
	}

	mapData, mapKey, _ := BuildMapDataAndKeys(model, true)
	versionColName, exist := GetColumnNameByIndex(valueOfModel.Type(), versionIndex)
	if !exist {
		panic("version's column not found")
	}
	mapKey[versionColName] = currentVersion

	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for colName, v1 := range mapData {
		if v1 != nil {
			values = append(values, v1)
			colSet = append(colSet, fmt.Sprintf("%v = "+BuildParam(colNumber+i, driverName), colName))
			colNumber++
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}

	for colName, v2 := range mapKey {
		values = append(values, v2)
		colQuery = append(colQuery, fmt.Sprintf("%v="+BuildParam(colNumber+i, driverName), QuoteColumnName(colName)))
		colNumber++
	}
	queryWhere := strings.Join(colQuery, " AND ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("UPDATE %v SET %v WHERE %v", table, querySet, queryWhere)
	return query, values
}

func BuildPatch(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, driverName string) (string, []interface{}) {
	scope := statement()
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if columName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, columName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	var value []interface{}

	n := len(scope.Columns)
	sets, val1, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, driverName, ", ")
	if err1 != nil {
		return "", nil
	}
	value = append(value, val1...)
	columnsKeys := len(scope.Keys)
	where, val2, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &columnsKeys, n, driverName, " and ")
	if err2 != nil {
		return "", nil
	}
	value = append(value, val2...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildPatchWithVersion(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, driverName string, versionIndex int, versionJsonName, versionColName string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}

	currentVersion, ok := model[versionJsonName]
	if !ok {
		panic("version field not found")
	}
	nextVersion := currentVersion.(int64) + 1
	model[versionJsonName] = nextVersion

	scope := statement()
	var value []interface{}
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if columName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, columName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	scope.Values = append(scope.Values, currentVersion)
	scope.Keys = append(scope.Keys, versionColName)

	n := len(scope.Columns)
	sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, driverName, ", ")
	if err1 != nil {
		return "", nil
	}
	value = append(value, setVal...)
	numKeys := len(scope.Keys)
	where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, driverName, " and ")
	if err2 != nil {
		return "", nil
	}
	value = append(value, whereVal...)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildDelete(table string, ids map[string]interface{}, driverName string) (string, []interface{}) {

	var values []interface{}
	var queryArr []string
	for key, value := range ids {
		queryArr = append(queryArr, fmt.Sprintf("%v = %v", QuoteColumnName(key), BuildParam(1, driverName)))
		values = append(values, value)
	}
	q := strings.Join(queryArr, " and ")
	return fmt.Sprintf("delete from %v where %v", table, q), values
}

// Obtain columns and values required for insert from interface
func ExtractMapValue(value interface{}, excludeColumns *[]string, ignoreNull bool) (map[string]interface{}, map[string]interface{}, error) {
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
		if GetTag(field, IgnoreReadWrite) == IgnoreReadWrite {
			continue
		}
		if value := field.Value.Interface(); value == nil && ignoreNull {
			*excludeColumns = append(*excludeColumns, field.Tags["fieldName"])
		}
		if !ContainString(*excludeColumns, GetTag(field, "fieldName")) && !IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				attrs[dBName] = field.Value.Interface()
			}
		}
		if IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				attrsKey[dBName] = field.Value.Interface()
			}
		}
	}
	return attrs, attrsKey, nil
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

func GetJsonNameByIndex(ModelType reflect.Type, index int) (string, bool) {
	field := ModelType.Field(index)
	if tagJson, ok := field.Tag.Lookup("json"); ok {
		arrValue := strings.Split(tagJson, ",")
		if len(arrValue) > 0 {
			return arrValue[0], true
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
				k = ReplaceAtIndex(k, hoa[0], strings.Index(k, "_")+1)
				k = strings.Replace(k, "_", "", 1)
			}
		}
		result[k] = v
	}
	return result
}
func ReplaceAtIndex(str string, replacement rune, index int) string {
	out := []rune(str)
	out[index] = replacement
	return string(out)
}

func GetTableName(object interface{}) string {
	objectValue := reflect.Indirect(reflect.ValueOf(object))
	tableName := objectValue.MethodByName("TableName").Call([]reflect.Value{})
	return tableName[0].String()
}

func BuildParameters(numCol int, driver string) string {
	var arrValue []string
	for i := 0; i < numCol; i++ {
		arrValue = append(arrValue, BuildParam(i+1, driver))
	}
	return strings.Join(arrValue, ",")
}

func BuildParametersFrom(i int, numCol int, driver string) string {
	var arrValue []string
	for j := 0; j < numCol; j++ {
		arrValue = append(arrValue, BuildParam(i+j+1, driver))
	}
	return strings.Join(arrValue, ",")
}

func BuildParam(index int, driver string) string {
	switch driver {
	case DriverPostgres:
		return "$" + strconv.Itoa(index)
	case DriverOracle:
		return ":val" + strconv.Itoa(index)
	default:
		return "?"
	}
}
func BuildParamByDB(n int, db *sql.DB) string {
	driverName := GetDriver(db)
	return BuildParam(n, driverName)
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

func GetTag(field Field, tagName string) string {
	if tag, ok := field.Tags[tagName]; ok {
		return tag
	}
	return ""
}

func IsPrimary(field Field) bool {
	return GetTag(field, PrimaryKey) != ""
}
func ReplaceQueryArgs(driver string, query string) string {
	if driver == DriverOracle || driver == DriverPostgres {
		var x string
		if driver == DriverOracle {
			x = ":val"
		} else {
			x = "$"
		}
		i := 1
		k := strings.Index(query, "?")
		if k >= 0 {
			for {
				query = strings.Replace(query, "?", x+fmt.Sprintf("%v", i), 1)
				i = i + 1
				k := strings.Index(query, "?")
				if k < 0 {
					return query
				}
			}
		}
	}
	return query
}
