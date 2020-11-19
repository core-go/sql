package sql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type StringService struct {
	DB            *sql.DB
	Table         string
	Field         string
	QuestionParam bool
}

func NewStringService(db *sql.DB, table string, field string, questionParam bool) *StringService {
	return &StringService{DB: db, Table: table, Field: field, QuestionParam: questionParam}
}

func (s *StringService) Load(ctx context.Context, key string, max int64) ([]string, error) {
	re := regexp.MustCompile(`\%|\?`)
	key = re.ReplaceAllString(key, "")
	key = key + "%"
	vs := make([]string, 0)
	var sql string
	if s.QuestionParam {
		sql = fmt.Sprintf("select %s from %s where %s like ? limit %d", s.Field, s.Table, s.Field, max)
	} else {
		sql = fmt.Sprintf("select %s from %s where %s like $1 fetch next %d rows only", s.Field, s.Table, s.Field, max)
	}
	rows, er1 := s.DB.Query(sql, key)
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

func (s *StringService) Save(ctx context.Context, values []string) (int64, error) {
	mainScope := BatchStatement{}
	var placeholder []string
	driverName := GetDriverName(s.DB)
	for _, e := range values {
		placeholder = append(placeholder, "(?)")
		mainScope.Values = append(mainScope.Values, e)
	}
	query := ""
	if driverName == DriverPostgres {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
		)
	} else if driverName == "sqlite3" {
		query = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES %s",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
		)
	} else if driverName == DriverMysql {
		qKey := s.Field + " = " + s.Field
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
			qKey,
		)
	} else if driverName == "mssql" {
		onDupe := s.Table + "." + s.Field + " = " + "temp." + s.Field
		value := "temp." + s.Field
		query = fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
			s.Table,
			strings.Join(placeholder, ", "),
			s.Field,
			onDupe,
			s.Field,
			value,
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", driverName)
	}

	query = ReplaceQueryparam(driverName, query, len(mainScope.Values))
	mainScope.Query = query
	x, err := s.DB.Exec(mainScope.Query, mainScope.Values...)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func (s *StringService) Delete(ctx context.Context, values []string) (int64, error) {
	strSQL := ""
	for i := 0; i < len(values); i++ {
		strSQL += `'` + values[i] + `',`
	}
	strSQL = strings.TrimRight(strSQL, ",")
	query := `DELETE FROM ` + s.Table + ` WHERE ` + s.Field + ` IN (` + strSQL + `)`
	x, err := s.DB.Exec(query)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}
