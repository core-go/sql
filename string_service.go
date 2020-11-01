package sql
/*
import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jinzhu/gorm"
	"regexp"
	"strings"
)

type StringService struct {
	DB    *sql.DB
	Table string
	Field string
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
		sql = fmt.Sprintf("select %s from %s where %s like ? fetch next %d rows only", s.Field, s.Table, s.Field, max)
	} else {
		sql = fmt.Sprintf("select %s from %s where %s ilike $1 fetch next %d rows only", s.Field, s.Table, s.Field, max)
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
	//err := s.Database.Table(s.Table).columnWhere(s.Field+" LIKE ?", key).Limit(max).Pluck(s.Field, &urlIdArr).Error
	mainScope := s.Database.NewScope(values)
	var placeholder []string
	for _, e := range values {
		placeholder = append(placeholder, "(?)")
		mainScope.AddToVars(e)
	}
	query := ""
	if s.Database.Dialect().GetName() == "postgres" {
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT DO NOTHING",
			mainScope.Quote(s.Table),
			mainScope.Quote(s.Field),
			strings.Join(placeholder, ", "),
		)
	} else if s.Database.Dialect().GetName() == "sqlite3" {
		query = fmt.Sprintf("INSERT OR IGNORE INTO %s (%s) VALUES %s",
			mainScope.Quote(s.Table),
			mainScope.Quote(s.Field),
			strings.Join(placeholder, ", "),
		)
	} else if s.Database.Dialect().GetName() == "mysql" {
		qKey := mainScope.Quote(s.Field) + " = " + mainScope.Quote(s.Field)
		query = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			mainScope.Quote(s.Table),
			mainScope.Quote(s.Field),
			strings.Join(placeholder, ", "),
			qKey,
		)

	} else if s.Database.Dialect().GetName() == "mssql" {
		onDupe := mainScope.Quote(s.Table) + "." + mainScope.Quote(s.Field) + " = " + "temp." + mainScope.Quote(s.Field)
		value := "temp." + mainScope.Quote(s.Field)
		query = fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s);",
			mainScope.Quote(s.Table),
			strings.Join(placeholder, ", "),
			mainScope.Quote(s.Field),
			onDupe,
			mainScope.Quote(s.Field),
			value,
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", s.Database.Dialect().GetName())
	}
	mainScope.Raw(query)

	x := s.Database.Exec(mainScope.SQL, mainScope.SQLVars...)
	return x.RowsAffected, x.Error
}

func (s *StringService) Delete(ctx context.Context, values []string) (int64, error) {
	var result map[string]interface{}
	rows := s.Database.Table(s.Table).Set("gorm:auto_preload", true).Where(s.Field+" IN (?)", values).Delete(&result)
	if rows.Error != nil {
		return 0, rows.Error
	}
	return rows.RowsAffected, nil
}
*/