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
	Driver        string
}

func NewStringService(db *sql.DB, table string, field string, questionParam bool) *StringService {
	driver := GetDriver(db)
	return &StringService{DB: db, Table: table, Field: field, Driver: driver}
}

func (s *StringService) Load(ctx context.Context, key string, max int64) ([]string, error) {
	re := regexp.MustCompile(`\%|\?`)
	key = re.ReplaceAllString(key, "")
	key = key + "%"
	vs := make([]string, 0)
	var sql string
	if s.Driver == DriverPostgres {
		sql = fmt.Sprintf("select %s from %s where %s ilike $1 fetch next %d rows only", s.Field, s.Table, s.Field, max)
	} else if s.Driver == DriverOracle {
		sql = fmt.Sprintf("select %s from %s where %s ilike :val1 fetch next %d rows only", s.Field, s.Table, s.Field, max)
	} else {
		sql = fmt.Sprintf("select %s from %s where %s like ? limit %d", s.Field, s.Table, s.Field, max)
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
	driver := s.Driver
	for _, e := range values {
		placeholder = append(placeholder, "(?)")
		mainScope.Values = append(mainScope.Values, e)
	}
	query := ""
	if driver == DriverPostgres {
		query = fmt.Sprintf("insert into %s (%s) values %s on conflict do nothing",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
		)
	} else if driver == "sqlite3" {
		query = fmt.Sprintf("insert or ignore into %s (%s) values %s",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
		)
	} else if driver == DriverMysql {
		qKey := s.Field + " = " + s.Field
		query = fmt.Sprintf("insert into %s (%s) values %s on duplicate key update %s",
			s.Table,
			s.Field,
			strings.Join(placeholder, ", "),
			qKey,
		)
	} else if driver == "mssql" {
		onDupe := s.Table + "." + s.Field + " = " + "temp." + s.Field
		value := "temp." + s.Field
		query = fmt.Sprintf("merge into %s using (values %s) as temp (%s) on %s when not matched then insert (%s) values (%s);",
			s.Table,
			strings.Join(placeholder, ", "),
			s.Field,
			onDupe,
			s.Field,
			value,
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", driver)
	}

	query = ReplaceParameters(driver, query, len(mainScope.Values))
	mainScope.Query = query
	x, err := s.DB.Exec(mainScope.Query, mainScope.Values...)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}

func (s *StringService) Delete(ctx context.Context, values []string) (int64, error) {
	var arrValue []string
	le := len(values)
	for i := 1; i <= le; i++ {
		param := BuildParam(i, s.Driver)
		arrValue = append(arrValue, param)
	}
	query := `delete from ` + s.Table + ` where ` + s.Field + ` in (` + strings.Join(arrValue, ",") + `)`
	x, err := s.DB.Exec(query)
	if err != nil {
		return 0, err
	}
	return x.RowsAffected()
}
