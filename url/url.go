package text

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type URL struct {
	Id   string  `yaml:"id" mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	Name *string `yaml:"name" mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Url  *string `yaml:"url" mapstructure:"url" json:"url,omitempty" gorm:"column:url" bson:"url,omitempty" dynamodbav:"url,omitempty" firestore:"url,omitempty"`
}

type URLAdapter struct {
	db         *sql.DB
	Table      string
	Id         string
	Name       string
	Url        string
	BuildParam func(int) string
}

func NewURLAdapter(db *sql.DB, table string, id string, text string, url string, opts ...func(i int) string) (*URLAdapter, error) {
	var buildParam func(i int) string
	if len(opts) > 0 && opts[0] != nil {
		buildParam = opts[0]
	} else {
		buildParam = getBuild(db)
	}
	return &URLAdapter{
		db:         db,
		Table:      table,
		Id:         id,
		Name:       text,
		Url:        url,
		BuildParam: buildParam,
	}, nil
}

func (r *URLAdapter) Load(ctx context.Context, ids []string) ([]URL, error) {
	var values []URL
	if len(ids) == 0 {
		return values, nil
	}
	le := len(ids)
	p := make([]interface{}, 0)
	for _, str := range ids {
		p = append(p, str)
	}
	var arrValue []string
	for i := 1; i <= le; i++ {
		param := r.BuildParam(i)
		arrValue = append(arrValue, param)
	}
	query := fmt.Sprintf("select %s, %s, %s from %s where %s in (%s)", r.Id, r.Name, r.Url, r.Table, r.Id, strings.Join(arrValue, ","))
	rows, err := r.db.QueryContext(ctx, query, p...)
	defer rows.Close()

	for rows.Next() {
		var row URL
		if err := rows.Scan(&row.Id, &row.Name, &row.Url); err != nil {
			return values, err
		}
		values = append(values, row)
	}
	if err = rows.Err(); err != nil {
		return values, err
	}
	return values, nil
}

func ToMap(rows []URL) map[string]*URL {
	rs := make(map[string]*URL, 0)
	for _, row := range rows {
		rs[row.Id] = &row
	}
	return rs
}

func Unique(s []string) []string {
	inResult := make(map[string]bool)
	var result []string
	for _, str := range s {
		if _, ok := inResult[str]; !ok {
			inResult[str] = true
			result = append(result, str)
		}
	}
	return result
}

func getBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return buildDollarParam
	case "*godror.drv":
		return buildOracleParam
	case "*mssql.Driver":
		return buildMsSqlParam
	default:
		return buildParam
	}
}
func buildParam(i int) string {
	return "?"
}
func buildOracleParam(i int) string {
	return ":" + strconv.Itoa(i)
}
func buildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func buildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
