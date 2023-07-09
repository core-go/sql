package sequence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SequenceAdapter struct {
	DB         *sql.DB
	Tables     string
	Table      string
	Sequence   string
	BuildParam func(i int) string
}
func NewSequenceRepository(db *sql.DB, buildParam func(int) string, options ...string) *SequenceAdapter {
	return NewSequenceAdapter(db, buildParam, options...)
}
func NewSequenceAdapter(db *sql.DB, buildParam func(int) string, options ...string) *SequenceAdapter {
	var tables, table, sequence string
	if len(options) > 0 && len(options[0]) > 0 {
		tables = options[0]
	} else {
		tables = "sequences"
	}
	if len(options) > 1 && len(options[1]) > 0 {
		table = options[1]
	} else {
		table = "table"
	}
	if len(options) > 2 && len(options[2]) > 0 {
		sequence = options[2]
	} else {
		sequence = "sequence"
	}
	return &SequenceAdapter{
		DB:         db,
		Tables:     strings.ToLower(tables),
		Table:      strings.ToLower(table),
		Sequence:   strings.ToLower(sequence),
		BuildParam: buildParam,
	}
}
func (s *SequenceAdapter) Next(ctx context.Context, id string) (int64, error) {
	query := fmt.Sprintf(`select %s from %s where %s = %s`, s.Sequence, s.Tables, s.Table, s.BuildParam(1))
	tx, err := s.DB.Begin()
	if err != nil {
		return -1, err
	}
	rows, err := tx.QueryContext(ctx, query, id)
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	if rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return -1, err
		}
		updateSql := fmt.Sprintf(`update %s set %s = %s where %s = %s`, s.Tables, s.Sequence, s.BuildParam(1), s.Table, s.BuildParam(2))
		_, err = tx.ExecContext(ctx, updateSql, seq+1, id)
		if err != nil {
			er := tx.Rollback()
			if er != nil {
				return -1, er
			}
			return -1, err
		}
		err = tx.Commit()
		if err != nil {
			return -1, err
		}
		return seq, nil
	} else {
		insertSql := fmt.Sprintf(`insert into %s (%s, %s) values (%s, 2)`, s.Tables, s.Table, s.Sequence, s.BuildParam(1))
		_, err = tx.ExecContext(ctx, insertSql, id)
		if err != nil {
			er := tx.Rollback()
			if er != nil {
				return -1, er
			}
			return -1, err
		}
		err = tx.Commit()
		if err != nil {
			return -1, err
		}
		return 1, nil
	}
}
func (s *SequenceAdapter) Reset(ctx context.Context, id string) (int64, error) {
	updateSql := fmt.Sprintf(`update %s set %s = 1 where %s = %s`, s.Tables, s.Sequence, s.Table, s.BuildParam(1))
	res, err := s.DB.ExecContext(ctx, updateSql, id)
	if err != nil {
		return -1, err
	}
	return res.RowsAffected()
}
