package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// raw query
func Save(db *sql.DB, table string, model interface{}) (int64, error) {
	placeholders := make([]string, 0)
	exclude := make([]string, 0)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "-" {
				exclude = append(exclude, field.Name)
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
	for _, key := range sorted {
		//mainScope.AddToVars(attrs[key])
		dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
		variables = append(variables, "?")
	}

	valueQuery := "(" + strings.Join(variables, ", ") + ")"
	placeholders = append(placeholders, valueQuery)

	// Also append variables to mainScope

	var setColumns []string

	for _, key := range sorted {
		setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = ?")
	}
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
}
