package orm

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"time"
)

type SqlHealthChecker struct {
	db       *gorm.DB
	name     string
	sql      string
	timeout  time.Duration
	provider bool
}

func NewSqlHealthChecker(db *gorm.DB, name string, timeout time.Duration, provider bool, sql string) *SqlHealthChecker {
	return &SqlHealthChecker{db, name, sql, timeout, provider}
}

func NewDefaultSqlHealthChecker(db *gorm.DB, provider bool, sql string) *SqlHealthChecker {
	return &SqlHealthChecker{db, "sql", sql, 5 * time.Second, provider}
}

func (s *SqlHealthChecker) Name() string {
	return s.name
}

func (s *SqlHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	cancel := func() {}
	if s.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	defer cancel()

	res := make(map[string]interface{})
	if s.provider {
		res["provider"] = s.db.Dialector.Name()
	}
	checkerChan := make(chan error)
	go func() {
		_, err := s.db.Raw(s.sql).Rows()
		checkerChan <- err
	}()
	select {
	case err := <-checkerChan:
		return res, err
	case <-ctx.Done():
		return res, fmt.Errorf("timeout")
	}
}

func (s *SqlHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
