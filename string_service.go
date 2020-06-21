package sql

import (
	"context"
	"fmt"
	"github.com/jinzhu/gorm"
	"regexp"
	"strings"
)

type StringService struct {
	Database *gorm.DB
	Table    string
	Field    string
}

func NewStringService(db *gorm.DB, table string, field string) *StringService {
	return &StringService{db, table, field}
}

func (s *StringService) Load(ctx context.Context, key string, max int64) ([]string, error) {
	var nilArr []string
	var urlIdArr []string
	re := regexp.MustCompile(`\%|\?`)
	key = re.ReplaceAllString(key, "")
	key = key + "%"
	err := s.Database.Table(s.Table).Set("gorm:auto_preload", true).Where(s.Field+" LIKE ?", key).Limit(max).Pluck(s.Field, &urlIdArr).Error
	if err != nil {
		return nilArr, err
	}
	return urlIdArr, nil
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
