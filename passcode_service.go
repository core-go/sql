package orm

import (
	"context"
	"fmt"
	"gorm.io/gorm"
	"strings"
	"time"
)

type PasscodeService struct {
	db            *gorm.DB
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeService(db *gorm.DB, tableName string, idName string, passcodeName string, expiredAtName string) *PasscodeService {
	return &PasscodeService{
		db:            db,
		tableName:     strings.ToLower(tableName),
		idName:        strings.ToLower(idName),
		passcodeName:  strings.ToLower(passcodeName),
		expiredAtName: strings.ToLower(expiredAtName),
	}
}

func NewDefaultPasscodeService(db *gorm.DB, tableName string) *PasscodeService {
	return NewPasscodeService(db, tableName, "id", "passcode", "expiredat")
}

func (s *PasscodeService) Save(ctx context.Context, id string, passcode string, expireAt time.Time) (int64, error) {
	mainScope := s.db.Session(&gorm.Session{DryRun: true}).Statement
	var placeholder []string
	columns := []string{s.idName, s.passcodeName, s.expiredAtName}
	for i := 0; i < 3; i++ {
		placeholder = append(placeholder, "?")
	}
	mainScope.AddVar(mainScope, id)
	mainScope.AddVar(mainScope,passcode)
	mainScope.AddVar(mainScope,expireAt)
	mainScope.AddVar(mainScope,id)
	mainScope.AddVar(mainScope,passcode)
	mainScope.AddVar(mainScope,expireAt)
	var queryString string
	if a := s.db.Dialector.Name(); a == "postgres" || a == "sqlite3" {
		setColumns := make([]string, 0)
		for _, key := range columns {
			setColumns = append(setColumns, mainScope.Quote(key)+" = ?")
		}
		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			mainScope.Quote(s.tableName),
			strings.Join(columns, ", "),
			strings.Join(placeholder, ", "),
			mainScope.Quote(s.idName),
			strings.Join(setColumns, ", "),
		)
	} else if s.db.Dialector.Name() == "mysql" {
		setColumns := make([]string, 0)
		for _, key := range columns {
			setColumns = append(setColumns, mainScope.Quote(key)+" = ?")
		}

		queryString = fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			mainScope.Quote(s.tableName),
			strings.Join(columns, ", "),
			"("+strings.Join(placeholder, ", ")+")",
			strings.Join(setColumns, ", "),
		)
	} else if s.db.Dialector.Name() == "mssql" {
		setColumns := make([]string, 0)
		onDupe := mainScope.Quote(s.tableName) + "." + mainScope.Quote(s.idName) + " = " + "temp." + mainScope.Quote(s.idName)
		for _, key := range columns {
			setColumns = append(setColumns, mainScope.Quote(key)+" = temp."+key)
		}
		queryString = fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			mainScope.Quote(s.tableName),
			strings.Join(placeholder, ", "),
			strings.Join(columns, ", "),
			onDupe,
			strings.Join(setColumns, ", "),
			strings.Join(columns, ", "),
			strings.Join(placeholder, ", "),
		)
	} else {
		return 0, fmt.Errorf("unsupported db vendor, current vendor is %s", s.db.Dialector.Name())
	}
	mainScope.Raw(queryString)

	x := s.db.Exec(mainScope.SQL.String(), mainScope.Vars...)
	return x.RowsAffected, x.Error
}

func (s *PasscodeService) Load(ctx context.Context, id string) (string, time.Time, error) {
	arr := make(map[string]interface{})
	rows, err := s.db.Table(s.tableName).Set("gorm:auto_preload", true).Where(s.idName+"= ?", id).Rows()
	if err != nil {
		return "", time.Now().Add(-24 * time.Hour), err
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i, _ := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return "", time.Now().Add(-24 * time.Hour), err
		}

		for i, colName := range cols {
			val := columnPointers[i].(*interface{})
			arr[colName] = *val
		}
	}

	err2 := rows.Err()
	if err2 != nil {
		return "", time.Now().Add(-24 * time.Hour), err2
	}

	if len(arr) == 0 {
		return "", time.Now().Add(-24 * time.Hour), nil
	}

	code := string(arr[s.passcodeName].([]byte))
	expiredAt := arr[s.expiredAtName].(time.Time)
	return code, expiredAt, nil
}

func (s *PasscodeService) Delete(ctx context.Context, id string) (int64, error) {
	var result map[string]interface{}
	rows := s.db.Table(s.tableName).Where(s.idName+"= ?", id).Delete(&result)
	if rows.Error != nil {
		return 0, rows.Error
	}
	return rows.RowsAffected, nil
}
