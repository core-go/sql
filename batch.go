package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func ExecuteStatements(ctx context.Context, tx *sql.Tx, commit bool, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	var count int64
	count = 0
	for _, stmt := range stmts {
		r2, er3 := tx.ExecContext(ctx, stmt.Query, stmt.Params...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	if commit {
		er6 := tx.Commit()
		return count, er6
	} else {
		return count, nil
	}
}
func ExecuteAll(ctx context.Context, db *sql.DB, stmts ...Statement) (int64, error) {
	if stmts == nil || len(stmts) == 0 {
		return 0, nil
	}
	tx, er1 := db.Begin()
	if er1 != nil {
		return 0, er1
	}
	var count int64
	count = 0
	for _, stmt := range stmts {
		r2, er3 := tx.ExecContext(ctx, stmt.Query, stmt.Params...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	return count, er6
}
func ExecuteBatch(ctx context.Context, db *sql.DB, sts []Statement, firstRowSuccess bool, countAll bool) (int64, error) {
	if sts == nil || len(sts) == 0 {
		return 0, nil
	}
	driver := GetDriver(db)
	tx, er0 := db.Begin()
	if er0 != nil {
		return 0, er0
	}
	result, er1 := tx.ExecContext(ctx, sts[0].Query, sts[0].Params...)
	if er1 != nil {
		_ = tx.Rollback()
		str := er1.Error()
		if driver == DriverPostgres && strings.Contains(str, "pq: duplicate key value violates unique constraint") {
			return 0, nil
		} else if driver == DriverMysql && strings.Contains(str, "Error 1062: Duplicate entry") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverOracle && strings.Contains(str, "ORA-00001: unique constraint") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverMssql && strings.Contains(str, "Violation of PRIMARY KEY constraint") {
			return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else if driver == DriverSqlite3 && strings.Contains(str, "UNIQUE constraint failed") {
			return 0, nil
		} else {
			return 0, er1
		}
	}
	rowAffected, er2 := result.RowsAffected()
	if er2 != nil {
		tx.Rollback()
		return 0, er2
	}
	if firstRowSuccess {
		if rowAffected == 0 {
			return 0, nil
		}
	}
	count := rowAffected
	for i := 1; i < len(sts); i++ {
		r2, er3 := tx.ExecContext(ctx, sts[i].Query, sts[i].Params...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	if er6 != nil {
		return count, er6
	}
	if countAll {
		return count, nil
	}
	return 1, nil
}

type Statement struct {
	Query  string        `mapstructure:"query" json:"query,omitempty" gorm:"column:query" bson:"query,omitempty" dynamodbav:"query,omitempty" firestore:"query,omitempty"`
	Params []interface{} `mapstructure:"params" json:"params,omitempty" gorm:"column:params" bson:"params,omitempty" dynamodbav:"params,omitempty" firestore:"params,omitempty"`
}
type Statements interface {
	Exec(ctx context.Context, db *sql.DB) (int64, error)
	Add(sql string, args []interface{}) Statements
	Clear() Statements
}

func NewDefaultStatements(successFirst bool) *DefaultStatements {
	stmts := make([]Statement, 0)
	s := &DefaultStatements{Statements: stmts, SuccessFirst: successFirst}
	return s
}
func NewStatements(successFirst bool) Statements {
	return NewDefaultStatements(successFirst)
}

type DefaultStatements struct {
	Statements   []Statement
	SuccessFirst bool
}

func (s *DefaultStatements) Exec(ctx context.Context, db *sql.DB) (int64, error) {
	if s.SuccessFirst {
		return ExecuteBatch(ctx, db, s.Statements, true, false)
	} else {
		return ExecuteAll(ctx, db, s.Statements...)
	}
}
func (s *DefaultStatements) Add(sql string, args []interface{}) Statements {
	var stm = Statement{Query: sql, Params: args}
	s.Statements = append(s.Statements, stm)
	return s
}
func (s *DefaultStatements) Clear() Statements {
	s.Statements = s.Statements[:0]
	return s
}

type FieldDB struct {
	JSON   string
	Column string
	Field  string
	Index  int
	Key    bool
	Update bool
	True   *string
	False  *string
}

func MakeSchema(modelType reflect.Type) ([]string, []string, map[string]FieldDB) {
	numField := modelType.NumField()
	columns := make([]string, 0)
	keys := make([]string, 0)
	schema := make(map[string]FieldDB, 0)
	for idx := 0; idx < numField; idx++ {
		field := modelType.Field(idx)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			update := !strings.Contains(tag, "update:false")
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							isKey := strings.Contains(tag, "primary_key")
							col = str2[j+1]
							columns = append(columns, col)
							if isKey {
								keys = append(keys, col)
							}

							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := FieldDB{
								JSON:   json,
								Column: col,
								Index:  idx,
								Key:    isKey,
								Update: update,
							}
							tTag, tOk := field.Tag.Lookup("true")
							if tOk {
								f.True = &tTag
								fTag, fOk := field.Tag.Lookup("false")
								if fOk {
									f.False = &fTag
								}
							}
							schema[col] = f
						}
					}
				}
			}
		}
	}
	return columns, keys, schema
}
func BuildToUpdateBatch(table string, models interface{}, buildParam func(int) string, options ...*sql.DB) ([]Statement, error) {
	driver := ""
	if len(options) > 0 {
		driver = GetDriver(options[0])
	}
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		mv := reflect.ValueOf(model)
		values := make([]string, 0)
		where := make([]string, 0)
		args := make([]interface{}, 0)
		i := 1
		for _, col := range cols {
			fdb := schema[col]
			if !fdb.Key && fdb.Update {
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					values = append(values, col+"=null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, col+"="+v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if driver == DriverPostgres {
								if boolValue {
									values = append(values, col+"=true")
								} else {
									values = append(values, col+"=false")
								}
							} else {
								if boolValue {
									if fdb.True != nil {
										values = append(values, col+"="+buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, col+"=1")
									}
								} else {
									if fdb.False != nil {
										values = append(values, col+"="+buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, col+"=0")
									}
								}
							}
						} else {
							values = append(values, col+"="+buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
		}
		for _, col := range keys {
			fdb := schema[col]
			f := mv.Field(fdb.Index)
			fieldValue := f.Interface()
			if f.Kind() == reflect.Ptr {
				if !reflect.ValueOf(fieldValue).IsNil() {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			v, ok := GetDBValue(fieldValue)
			if ok {
				where = append(where, col+"="+v)
			} else {
				where = append(where, col+"="+buildParam(i))
				i = i + 1
				args = append(args, fieldValue)
			}
		}
		query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, ","))
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToInsertBatch(db *sql.DB, table string, models interface{}, options ...func(int) string) (string, []interface{}, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}

	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return "", nil, nil
	}
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, _, schema := MakeSchema(modelType)
	driver := GetDriver(db)
	slen := s.Len()
	if driver != DriverOracle {
		i := 1
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			values := make([]string, 0)
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if driver == DriverPostgres {
								if boolValue {
									values = append(values, "true")
								} else {
									values = append(values, "false")
								}
							} else {
								if boolValue {
									if fdb.True != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, "1")
									}
								} else {
									if fdb.False != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, "0")
									}
								}
							}
						} else {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			x := "(" + strings.Join(values, ",") + ")"
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
			table,
			strings.Join(cols, ","),
			strings.Join(placeholders, ","),
		))
		return query, args, nil
	} else {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			i := 1
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if !isNil {
					iCols = append(iCols, col)
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.True)
								} else {
									values = append(values, "'1'")
								}
							} else {
								if fdb.False != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.False)
								} else {
									values = append(values, "'0'")
								}
							}
						} else {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			x := fmt.Sprintf("into %s(%s)values(%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf("insert all %s select * from dual", strings.Join(placeholders, " "))
		return query, args, nil
	}
}
func BuildToSaveBatch(db *sql.DB, table string, models interface{}) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return nil, nil
	}
	buildParam := GetBuild(db)
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
	driver := GetDriver(db)
	if driver == DriverPostgres || driver == DriverMysql {
		i := 1
		for j := 0; j < slen; j++ {
			i = 1
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			setColumns := make([]string, 0)
			args := make([]interface{}, 0)
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if !isNil {
					iCols = append(iCols, col)
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if driver == DriverPostgres {
								if boolValue {
									values = append(values, "true")
								} else {
									values = append(values, "false")
								}
							} else {
								if boolValue {
									if fdb.True != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.True)
									} else {
										values = append(values, "1")
									}
								} else {
									if fdb.False != nil {
										values = append(values, buildParam(i))
										i = i + 1
										args = append(args, *fdb.False)
									} else {
										values = append(values, "0")
									}
								}
							}
						} else {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			for _, col := range cols {
				fdb := schema[col]
				if !fdb.Key && fdb.Update {
					f := mv.Field(fdb.Index)
					fieldValue := f.Interface()
					isNil := false
					if f.Kind() == reflect.Ptr {
						if reflect.ValueOf(fieldValue).IsNil() {
							isNil = true
						} else {
							fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
						}
					}
					if isNil {
						setColumns = append(setColumns, col+"=null")
					} else {
						v, ok := GetDBValue(fieldValue)
						if ok {
							setColumns = append(setColumns, col+"="+v)
						} else {
							if boolValue, ok := fieldValue.(bool); ok {
								if driver == DriverPostgres {
									if boolValue {
										setColumns = append(setColumns, col+"=true")
									} else {
										setColumns = append(setColumns, col+"=false")
									}
								} else {
									if boolValue {
										if fdb.True != nil {
											setColumns = append(setColumns, col+"="+buildParam(i))
											i = i + 1
											args = append(args, *fdb.True)
										} else {
											values = append(values, "1")
										}
									} else {
										if fdb.False != nil {
											setColumns = append(setColumns, col+"="+buildParam(i))
											i = i + 1
											args = append(args, *fdb.False)
										} else {
											values = append(values, "0")
										}
									}
								}
							} else {
								setColumns = append(setColumns, col+"="+buildParam(i))
								i = i + 1
								args = append(args, fieldValue)
							}
						}
					}
				}
			}
			var query string
			if len(setColumns) > 0 {
				if driver == DriverPostgres {
					query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do update set %s",
						table,
						strings.Join(iCols, ","),
						strings.Join(values, ","),
						strings.Join(keys, ","),
						strings.Join(setColumns, ","),
					)
				} else {
					query = fmt.Sprintf("insert into %s(%s) values (%s) on duplicate key update %s",
						table,
						strings.Join(iCols, ","),
						strings.Join(values, ","),
						strings.Join(setColumns, ","),
					)
				}
			} else {
				if driver == DriverPostgres {
					query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do nothing",
						table,
						strings.Join(iCols, ","),
						strings.Join(values, ","),
						strings.Join(keys, ","),
					)
				} else {
					query = fmt.Sprintf("insert ignore into %s(%s) values (%s)",
						table,
						strings.Join(iCols, ","),
						strings.Join(values, ","),
					)
				}
			}
			s := Statement{Query: query, Params: args}
			stmts = append(stmts, s)
		}
	} else if driver == DriverSqlite3 {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			args := make([]interface{}, 0)
			i := 1
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				iCols = append(iCols, col)
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.True)
								} else {
									values = append(values, "1")
								}
							} else {
								if fdb.False != nil {
									values = append(values, buildParam(i))
									i = i + 1
									args = append(args, *fdb.False)
								} else {
									values = append(values, "0")
								}
							}
						} else {
							values = append(values, buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			query := fmt.Sprintf("insert or replace into %s(%s) values (%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			s := Statement{Query: query, Params: args}
			stmts = append(stmts, s)
		}
	} else if driver == DriverOracle {
		for j := 0; j < slen; j++ {
			_, _, schema := MakeSchema(modelType)
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			uniqueCols := make([]string, 0)
			inColumns := make([]string, 0)
			variables := make([]string, 0)
			setColumns := make([]string, 0)
			values := make([]interface{}, 0)
			insertCols := make([]string, 0)
			i := 0
			attrs, unique, _, err := ExtractBySchema(model, cols, schema)
			sorted := SortedKeys(attrs)
			if err != nil {
				return nil, fmt.Errorf("cannot extract object's values: %w", err)
			}
			for _, key := range sorted {
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				setColumns = append(setColumns, "a."+tkey+" = temp."+tkey)
				inColumns = append(inColumns, "temp."+key)
				fdb := schema[key]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						attrs[key] = reflect.Indirect(reflect.ValueOf(attrs[key])).Interface()
					}
				}
				if isNil {
					variables = append(variables, "null "+tkey)
				} else {
					v, ok := GetDBValue(attrs[key])
					if ok {
						variables = append(variables, v+" "+tkey)
					} else {
						if boolValue, ok := attrs[key].(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, fmt.Sprintf(":%d "+tkey, i))
									values = append(values, attrs[key])
									i++
								} else {
									variables = append(variables, "1 "+tkey)
								}
							} else {
								if fdb.False != nil {
									variables = append(variables, fmt.Sprintf(":%d "+tkey, i))
									values = append(values, attrs[key])
									i++
								} else {
									variables = append(variables, "0 "+tkey)
								}
							}
						} else {
							variables = append(variables, fmt.Sprintf(":%d "+tkey, i))
							values = append(values, attrs[key])
							i++
						}
					}
				}
				insertCols = append(insertCols, tkey)
			}
			for key, val := range unique {
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				onDupe := "a." + tkey + " = " + "temp." + tkey
				uniqueCols = append(uniqueCols, onDupe)
				variables = append(variables, fmt.Sprintf(":%s "+tkey, key))
				inColumns = append(inColumns, "temp."+key)
				values = append(values, val)
				insertCols = append(insertCols, tkey)
			}
			query := fmt.Sprintf("MERGE INTO %s a USING (SELECT %s FROM dual) temp ON  (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
				table,
				strings.Join(variables, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(insertCols, ", "),
				strings.Join(inColumns, ", "),
			)
			s := Statement{Query: query, Params: values}
			stmts = append(stmts, s)
		}
	} else if driver == DriverMssql {
		for j := 0; j < slen; j++ {
			_, _, schema := MakeSchema(modelType)
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			uniqueCols := make([]string, 0)
			dbColumns := make([]string, 0)
			variables := make([]string, 0)
			setColumns := make([]string, 0)
			values := make([]interface{}, 0)
			inColumns := make([]string, 0)
			attrs, unique, _, err := ExtractBySchema(model, cols, schema)
			sorted := SortedKeys(attrs)
			if err != nil {
				return nil, fmt.Errorf("cannot extract object's values: %w", err)
			}
			for _, key := range sorted {
				//mainScope.AddToVars(attrs[key])
				fdb := schema[key]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := strings.Replace(key, `"`, `""`, -1)
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						attrs[key] = reflect.Indirect(reflect.ValueOf(attrs[key])).Interface()
					}
				}
				if isNil {
					variables = append(variables, "null")
				} else {
					v, ok := GetDBValue(attrs[key])
					if ok {
						variables = append(variables, v)
					} else {
						if boolValue, ok := attrs[key].(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, "?")
									values = append(values, attrs[key])
								} else {
									variables = append(variables, "1 "+tkey)
								}
							} else {
								if fdb.False != nil {
									variables = append(variables, "?")
									values = append(values, attrs[key])
								} else {
									variables = append(variables, "0 "+tkey)
								}
							}
						} else {
							variables = append(variables, "?")
							values = append(values, attrs[key])
						}
					}
				}
				dbColumns = append(dbColumns, tkey)
				setColumns = append(setColumns, tkey+" = temp."+tkey)
				inColumns = append(inColumns, "temp."+key)

			}
			for i, val := range unique {
				tkey := strings.Replace(i, `"`, `""`, -1)
				dbColumns = append(dbColumns, strings.Replace(tkey, `"`, `""`, -1))
				variables = append(variables, "?")
				values = append(values, val)
				onDupe := table + "." + tkey + " = " + "temp." + tkey
				uniqueCols = append(uniqueCols, onDupe)
				inColumns = append(inColumns, "temp."+i)
			}
			query := fmt.Sprintf("MERGE INTO %s USING (SELECT %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
				table,
				strings.Join(variables, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(inColumns, ", "),
			)
			s := Statement{Query: query, Params: values}
			stmts = append(stmts, s)
		}
	}
	return stmts, nil
}

func InsertBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...func(int) string) (int64, error) {
	query, args, er1 := BuildToInsertBatch(db, tableName, models, options...)
	if er1 != nil {
		return 0, er1
	}
	x, er2 := db.ExecContext(ctx, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return x.RowsAffected()
}
func UpdateBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	stmts, er1 := BuildToUpdateBatch(tableName, models, buildParam, db)
	if er1 != nil {
		return 0, er1
	}
	return ExecuteAll(ctx, db, stmts...)
}
func SaveBatch(ctx context.Context, db *sql.DB, tableName string, models interface{}) (int64, error) {
	stmts, er1 := BuildToSaveBatch(db, tableName, models)
	if er1 != nil {
		return 0, er1
	}
	_, err := ExecuteAll(ctx, db, stmts...)
	total := int64(len(stmts))
	return total, err
}
