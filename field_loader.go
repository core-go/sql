package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type FieldLoader struct {
	DB            *sql.DB
	TableName     string
	Name          string
	QuestionParam bool
}

func NewFieldLoader(db *sql.DB, tableName string, name string, questionParam bool) *FieldLoader {
	return &FieldLoader{
		DB:            db,
		TableName:     tableName,
		Name:          name,
		QuestionParam: questionParam,
	}
}

func (l *FieldLoader) Values(ctx context.Context, ids []string) ([]string, error) {
	ss := make([]string, 0)
	vs := make([]string, 0)
	params := make([]interface{}, 0)
	for i, s := range ids {
		if l.QuestionParam {
			ss = append(ss, "?")
		} else {
			ss = append(ss, "$"+strconv.Itoa(i))
		}
		params = append(params, s)
	}

	sql := fmt.Sprintf("select distinct %s from %s where %s in (%s)", l.Name, l.TableName, l.Name, strings.Join(ss, ","))
	rows, er1 := l.DB.Query(sql, params...)
	if er1 != nil {
		return vs, er1
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if er2 := rows.Scan(&id); er2 == nil {
			vs = append(vs, id)
		} else {
			return vs, er2
		}
	}
	return vs, nil
}
