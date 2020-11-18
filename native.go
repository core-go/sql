package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// raw query
func Save(db *sql.DB, table string, model interface{}) (int64, error) {
	placeholders := make([]string, 0)
	exclude := make([]string, 0)
	unique := make([]string, 0)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "-" {
				exclude = append(exclude, field.Name)
			} else if strings.TrimSpace(tag) == "primary_key" {
				unique = append(exclude, field.Name)
			}
		}
	}

	attrs, _, err := ExtractMapValue(model, exclude)
	if err != nil {
		return 0, fmt.Errorf("cannot extract object's values: %w", err)
	}
	//mainScope := db.NewScope(model)
	//pkey := FindIdFields(modelType)
	size := len(attrs)
	dbColumns := make([]string, 0, size)
	variables := make([]string, 0, size)
	sorted := SortedKeys(attrs)

	// Also append variables to mainScope

	var setColumns []string
	dialect := GetDriverName(db)
	switch dialect {
	case "mysql":
		for _, key := range sorted {
			setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = ?")
		}
		for _, key := range sorted {
			//mainScope.AddToVars(attrs[key])
			dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
			variables = append(variables, "?")
		}

		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)
		queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			"`"+strings.Replace(table, "`", "``", -1)+"`",
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(setColumns, ", "),
		)
		value := make([]interface{}, 0, len(attrs)*2)
		for _, s := range sorted {
			value = append(value, attrs[s])
		}
		for _, s := range sorted {
			value = append(value, attrs[s])
		}
		res, err := db.Exec(queryString, value...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()

	case "postgres":
		uniqueCols := make([]string, 0)
		for i := 0; i < len(sorted); i++ {
			setColumns = append(setColumns, `"`+strings.Replace(sorted[i], `"`, `""`, -1)+`"`+" = excluded."+strings.Replace(sorted[i], `"`, `""`, -1))
			dbColumns = append(dbColumns, "`"+strings.Replace(sorted[i], "`", "``", -1)+"`")
			variables = append(variables, "$"+strconv.Itoa(i+1))
		}
		for _, i2 := range unique {
			uniqueCols = append(uniqueCols, `"`+strings.Replace(i2, `"`, `""`, -1)+`"`)
		}
		queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(uniqueCols, ", "),
			strings.Join(setColumns, ", "),
		)
		value := make([]interface{}, 0, len(attrs)*2)
		for _, s := range sorted {
			value = append(value, attrs[s])
		}
		res, err := db.Exec(queryString, value...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	case "mssql":
		uniqueCols := make([]string, 0)
		value := make([]interface{}, 0, len(attrs)*2)

		for _, key := range sorted {
			for _, i2 := range unique {
				if strings.ToLower(i2) == strings.ToLower(key) {
					onDupe := table + "." + key + " = " + "temp." + key
					uniqueCols = append(uniqueCols, onDupe)
				}
			}
			value = append(value, attrs[key])
			setColumns = append(setColumns, `"`+strings.Replace(key, `"`, `""`, -1)+`"`+" = temp."+key)
		}
		for _, key := range sorted {
			//mainScope.AddToVars(attrs[key])
			dbColumns = append(dbColumns, `"`+strings.Replace(key, `"`, `""`, -1)+`"`)
			variables = append(variables, "?")
		}
		queryString := fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(variables, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(variables, ", "),
		)
		res, err := db.Exec(queryString, value...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	case "oracle":
		uniqueCols := make([]string, 0)
		value := make([]interface{}, 0, len(attrs)*2)

		for _, key := range sorted {
			for _, i2 := range unique {
				if strings.ToLower(i2) == strings.ToLower(key) {
					onDupe := table + "." + key + " = " + "temp." + key
					uniqueCols = append(uniqueCols, onDupe)
				}
			}
			value = append(value, attrs[key])
			setColumns = append(setColumns, `"`+strings.Replace(key, `"`, `""`, -1)+`"`+" = temp."+key)
		}
		count := 1
		for _, key := range sorted {

			//mainScope.AddToVars(attrs[key])
			dbColumns = append(dbColumns, `"`+strings.Replace(key, `"`, `""`, -1)+`"`)
			variables = append(variables, fmt.Sprintf(":%d", count))
			count++
		}
		queryString := fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(variables, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(variables, ", "),
		)
		res, err := db.Exec(queryString, value...)
		if err != nil {
			return 0, err
		}
		return res.RowsAffected()
	default:
		return 0, fmt.Errorf("unsupported db vendor")
	}

}
