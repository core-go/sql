package sql

import (
	"context"
	"database/sql"
	"time"
)

type SqlHealthChecker struct {
	db      *sql.DB
	name    string
	timeout time.Duration
}

func NewSqlHealthChecker(db *sql.DB, name string, timeout time.Duration) *SqlHealthChecker {
	return &SqlHealthChecker{db, name, timeout}
}

func NewDefaultSqlHealthChecker(db *sql.DB) *SqlHealthChecker {
	return &SqlHealthChecker{db, "sql", 5 * time.Second}
}

func (s *SqlHealthChecker) Name() string {
	return s.name
}

func (s *SqlHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	err := s.db.Ping()
	if err != nil {
		return nil, err
	}
	res["status"] = "success"
	return res, nil
}

func (s *SqlHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
