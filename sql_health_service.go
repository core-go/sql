package sql

import (
	"context"
	"fmt"
	"github.com/jinzhu/gorm"
	"time"
)

type SqlHealthService struct {
	db       *gorm.DB
	name     string
	sql      string
	timeout  time.Duration
	provider bool
}

func NewSqlHealthService(db *gorm.DB, name string, timeout time.Duration, provider bool, sql string) *SqlHealthService {
	return &SqlHealthService{db, name, sql, timeout, provider}
}

func NewDefaultSqlHealthService(db *gorm.DB, provider bool, sql string) *SqlHealthService {
	return &SqlHealthService{db, "mongo", sql, 5 * time.Second, provider}
}

func (s *SqlHealthService) Name() string {
	return s.name
}

func (s *SqlHealthService) Check(ctx context.Context) (map[string]interface{}, error) {
	cancel := func() {}
	if s.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	defer cancel()

	res := make(map[string]interface{})
	if s.provider {
		res["provider"] = s.db.Dialect().GetName()
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

func (s *SqlHealthService) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
