package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// raw query
func Save(ctx context.Context, db *sql.DB, table string, model interface{}) (int64, error) {
	driver := GetDriver(db)
	queryString, value, err := BuildToSave(table, model, driver)
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, queryString, value...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func SaveTx(ctx context.Context, db *sql.DB, tx *sql.Tx, table string, model interface{}) (int64, error) {
	driver := GetDriver(db)
	query, values, err0 := BuildToSave(table, model, driver)
	if err0 != nil {
		return -1, err0
	}
	r, err1 := tx.ExecContext(ctx, query, values...)
	if err1 != nil {
		return -1, err1
	}
	return r.RowsAffected()
}

func BuildToSave(table string, model interface{}, driver string, options...func(i int) string) (string, []interface{}, error) {
	// driver := GetDriver(db)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	var buildParam func(i int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuildByDriver(driver)
	}
	mv := reflect.Indirect(reflect.ValueOf(model))
	cols, keys, schema := MakeSchema(modelType)
	if driver == DriverPostgres || driver == DriverMysql {
		iCols := make([]string, 0)
		values := make([]string, 0)
		setColumns := make([]string, 0)
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
		return query, args, nil
	} else if driver == DriverSqlite3 {
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
		return query, args, nil
	} else {
		dbColumns := make([]string, 0)
		variables := make([]string, 0)
		uniqueCols := make([]string, 0)
		inColumns := make([]string, 0)
		values := make([]interface{}, 0)
		insertCols := make([]string, 0)
		var setColumns []string
		i := 0
		switch driver {
		case DriverOracle:
			for _, key := range cols {
				fdb := schema[key]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				inColumns = append(inColumns, "temp."+key)
				if fdb.Key {
					onDupe := "a." + tkey + "=" + "temp." + tkey
					uniqueCols = append(uniqueCols, onDupe)
				} else {
					setColumns = append(setColumns, "a."+tkey+" = temp."+tkey)
				}
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					variables = append(variables,"null "+tkey)
				}else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						variables = append(variables, v+" "+tkey)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, buildParam(i)+" "+tkey)
									values = append(values, *fdb.True)
									i++
								} else {
									variables = append(variables,"1 "+tkey)
								}
							}else {
								if fdb.False != nil {
									variables = append(variables, buildParam(i)+" "+tkey)
									values = append(values, *fdb.False)
									i++
								} else {
									variables = append(variables,"0 "+tkey)
								}
							}
						}else {
							variables = append(variables, buildParam(i)+" "+tkey)
							values = append(values, fieldValue)
							i++
						}
					}
				}
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
			return query, values, nil
		case DriverMssql:
			for _, key := range cols {
				fdb := schema[key]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := strings.Replace(key, `"`, `""`, -1)
				isNil := false
				if fdb.Key {
					onDupe := table + "." + tkey + "=" + "temp." + tkey
					uniqueCols = append(uniqueCols, onDupe)
				}
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					variables = append(variables, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						variables = append(variables, v)
					} else {
						if boolValue, ok := fieldValue.(bool); ok {
							if boolValue {
								if fdb.True != nil {
									variables = append(variables, "?")
									values = append(values, *fdb.True)
								} else {
									variables = append(variables, "1 "+tkey)
								}
							} else {
								if fdb.False != nil {
									variables = append(variables, "?")
									values = append(values, *fdb.False)
								} else {
									variables = append(variables, "0 "+tkey)
								}
							}
						} else {
							variables = append(variables, "?")
							values = append(values, fieldValue)
						}
					}
				}
				dbColumns = append(dbColumns, tkey)
				setColumns = append(setColumns, table+"."+tkey+"=temp."+tkey)
				inColumns = append(inColumns, "temp."+key)
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
			return query, values, nil
		default:
			return "", nil, fmt.Errorf("unsupported db vendor")
		}
	}
}
