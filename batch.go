package sql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
)

func BuildToUpdateBatch(table string, models interface{}, buildParam func(int) string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...bool) ([]Statement, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
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
	strt := Schema{Columns: cols, Keys: keys, Fields: schema}
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		// mv := reflect.ValueOf(model)
		query, args := BuildToUpdateWithSchema(table, model, buildParam, toArray, boolSupport, strt)
		s := Statement{Query: query, Params: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildToInsertBatch(table string, models interface{}, driver string, toArray func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}, options ...func(int) string) (string, []interface{}, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuildByDriver(driver)
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
							if driver == DriverPostgres || driver == DriverCassandra {
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
							}
						} else {
							if toArray != nil && reflect.TypeOf(fieldValue).Kind() == reflect.Slice {
								values = append(values, buildParam(i))
								i = i + 1
								args = append(args, toArray(fieldValue))
							} else {
								values = append(values, buildParam(i))
								i = i + 1
								args = append(args, fieldValue)
							}
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
func BuildToSaveBatch(table string, models interface{}, driver string, options ...func(i int) string) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return nil, nil
	}
	var buildParam func(i int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuildByDriver(driver)
	}

	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
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
							if driver == DriverPostgres || driver == DriverCassandra {
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
								if driver == DriverPostgres || driver == DriverCassandra {
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
											values = append(values, "'1'")
										}
									} else {
										if fdb.False != nil {
											setColumns = append(setColumns, col+"="+buildParam(i))
											i = i + 1
											args = append(args, *fdb.False)
										} else {
											values = append(values, "'0'")
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
