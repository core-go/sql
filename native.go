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
	queryString, value, err := BuildToSave(db, table, model)
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
	query, values, err0 := BuildToSave(db, table, model)
	if err0 != nil {
		return -1, err0
	}
	r, err1 := tx.ExecContext(ctx, query, values...)
	if err1 != nil {
		return -1, err1
	}
	return r.RowsAffected()
}

func BuildToSave(db *sql.DB, table string, model interface{}) (string, []interface{}, error) {
	driver := GetDriver(db)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	numField := modelType.NumField()
	buildParam := GetBuild(db)
	mv := reflect.Indirect(reflect.ValueOf(model))
	if driver == DriverPostgres || driver == DriverMysql {
		cols, keys, schema := MakeSchema(modelType)
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
		return query, args, nil
	} else if driver == DriverSqlite3 {
		cols, _, schema := MakeSchema(modelType)
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
		return query, args, nil
	} else {
		exclude := make([]string, 0)
		for j := 0; j < numField; j++ {
			field := modelType.Field(j)
			ormTag := field.Tag.Get("gorm")
			tags := strings.Split(ormTag, ";")
			for _, tag := range tags {
				if strings.TrimSpace(tag) == "-" {
					exclude = append(exclude, field.Name)
				}
			}
		}

		attrs, unique, _, err := ExtractMapValue(model, &exclude, false)
		if err != nil {
			return "0", nil, fmt.Errorf("cannot extract object's values: %w", err)
		}
		//mainScope := db.NewScope(model)
		//pkey := FindIdFields(modelType)
		size := len(attrs)
		dbColumns := make([]string, 0, size)
		variables := make([]string, 0, size)
		sorted := SortedKeys(attrs)

		// Also append variables to mainScope
		var setColumns []string
		switch driver {
		case DriverOracle:
			_, _, schema := MakeSchema(modelType)
			uniqueCols := make([]string, 0)
			inColumns := make([]string, 0)
			values := make([]interface{}, 0, len(attrs)*2)
			insertCols := make([]string, 0)
			i := 0
			for _, key := range sorted {
				fdb := schema[key]
				f := mv.Field(fdb.Index)
				fieldValue := f.Interface()
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				setColumns = append(setColumns, "a."+tkey+" = temp."+tkey)
				inColumns = append(inColumns, "temp."+key)
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
			//for _, key := range sorted {
			//	value = append(value, attrs[key])
			//}
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
			_, _, schema := MakeSchema(modelType)
			uniqueCols := make([]string, 0)
			values := make([]interface{}, 0, len(attrs)*2)
			inColumns := make([]string, 0)
			for _, key := range sorted {
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
				setColumns = append(setColumns, table+"."+tkey+"=temp."+tkey)
				inColumns = append(inColumns, "temp."+key)
			}
			for i, val := range unique {
				tkey := strings.Replace(i, `"`, `""`, -1)
				fmt.Println(tkey)
				dbColumns = append(dbColumns, tkey)
				//dbColumns = append(dbColumns, `"`+strings.Replace(tkey, `"`, `""`, -1)+`"`)
				variables = append(variables, "?")
				values = append(values, val)
				inColumns = append(inColumns, "temp."+i)
				onDupe := table + "." + tkey + "=" + "temp." + tkey
				uniqueCols = append(uniqueCols, onDupe)
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
