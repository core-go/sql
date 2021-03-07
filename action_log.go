package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type ActionLogSchema struct {
	Id        string    `mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	User      string    `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip        string    `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	Resource  string    `mapstructure:"resource" json:"resource,omitempty" gorm:"column:resource" bson:"resource,omitempty" dynamodbav:"resource,omitempty" firestore:"resource,omitempty"`
	Action    string    `mapstructure:"action" json:"action,omitempty" gorm:"column:action" bson:"action,omitempty" dynamodbav:"action,omitempty" firestore:"action,omitempty"`
	Timestamp string    `mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Status    string    `mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Desc      string    `mapstructure:"desc" json:"desc,omitempty" gorm:"column:desc" bson:"desc,omitempty" dynamodbav:"desc,omitempty" firestore:"desc,omitempty"`
	Ext       *[]string `mapstructure:"ext" json:"ext,omitempty" gorm:"column:ext" bson:"ext,omitempty" dynamodbav:"ext,omitempty" firestore:"ext,omitempty"`
}
type ActionLogConfig struct {
	User       string `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip         string `mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	True       string `mapstructure:"true" json:"true,omitempty" gorm:"column:true" bson:"true,omitempty" dynamodbav:"true,omitempty" firestore:"true,omitempty"`
	False      string `mapstructure:"false" json:"false,omitempty" gorm:"column:false" bson:"false,omitempty" dynamodbav:"false,omitempty" firestore:"false,omitempty"`
	Goroutines bool   `mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}

type ActionLogWriter struct {
	Database *sql.DB
	Table    string
	Config   ActionLogConfig
	Schema   ActionLogSchema
	Generate func(ctx context.Context) (string, error)
	Driver   string
}

func NewActionLogWriter(database *sql.DB, tableName string, config ActionLogConfig, s ActionLogSchema, generate func(ctx context.Context) (string, error)) *ActionLogWriter {
	s.Id = strings.ToLower(s.Id)
	s.User = strings.ToLower(s.User)
	s.Resource = strings.ToLower(s.Resource)
	s.Action = strings.ToLower(s.Action)
	s.Timestamp = strings.ToLower(s.Timestamp)
	s.Status = strings.ToLower(s.Status)
	s.Desc = strings.ToLower(s.Desc)
	driver := GetDriver(database)
	if len(s.Id) == 0 {
		s.Id = "id"
	}
	if len(s.User) == 0 {
		s.User = "username"
	}
	if len(s.Resource) == 0 {
		s.Resource = "resource"
	}
	if len(s.Action) == 0 {
		s.Action = "action"
	}
	if len(s.Timestamp) == 0 {
		s.Timestamp = "timestamp"
	}
	if len(s.Status) == 0 {
		s.Status = "status"
	}
	if len(s.Desc) == 0 {
		s.Desc = "desc"
	}
	writer := ActionLogWriter{Database: database, Table: tableName, Config: config, Schema: s, Generate: generate, Driver: driver}
	return &writer
}

func (s *ActionLogWriter) Write(ctx context.Context, resource string, action string, success bool, desc string) error {
	log := make(map[string]interface{})
	now := time.Now()
	ch := s.Schema
	log[ch.Timestamp] = &now
	log[ch.Resource] = resource
	log[ch.Action] = action
	log[ch.Desc] = desc

	if success {
		log[ch.Status] = s.Config.True
	} else {
		log[ch.Status] = s.Config.False
	}
	log[ch.User] = GetString(ctx, s.Config.User)
	if len(ch.Ip) > 0 {
		log[ch.Ip] = GetString(ctx, s.Config.Ip)
	}
	if s.Generate != nil {
		id, er0 := s.Generate(ctx)
		if er0 == nil && len(id) > 0 {
			log[ch.Id] = id
		}
	}
	ext := BuildExt(ctx, ch.Ext)
	if len(ext) > 0 {
		for k, v := range ext {
			log[k] = v
		}
	}
	query, vars := BuildInsertSQL(s.Database, s.Table, log, s.Driver)
	_, err := s.Database.Exec(query, vars...)
	return err
}

func BuildExt(ctx context.Context, keys *[]string) map[string]interface{} {
	headers := make(map[string]interface{})
	if keys != nil {
		hs := *keys
		for _, header := range hs {
			v := ctx.Value(header)
			if v != nil {
				headers[header] = v
			}
		}
	}
	return headers
}
func GetString(ctx context.Context, key string) string {
	if len(key) > 0 {
		u := ctx.Value(key)
		if u != nil {
			s, ok := u.(string)
			if ok {
				return s
			} else {
				return ""
			}
		}
	}
	return ""
}
func BuildInsertSQL(db *sql.DB, tableName string, model map[string]interface{}, driver string) (string, []interface{}) {
	var cols []string
	var values []interface{}
	for col, v := range model {
		cols = append(cols, QuoteString(col, driver))
		values = append(values, v)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	var arrValue []string
	for i := 1; i <= numCol; i++ {
		param := BuildParam(i, driver)
		arrValue = append(arrValue, param)
	}
	value := fmt.Sprintf("(%v)", strings.Join(arrValue, ","))
	strSQL := fmt.Sprintf("insert into %v %v values %v", QuoteString(tableName, driver), column, value)
	return strSQL, values
}

func QuoteString( name string, driver string) string {
	if driver == DriverPostgres {
		name =	"`"+strings.Replace(name, "`", "``", -1)+"`"
	}
	return name
}
func ReplaceParameters(driver string, query string, n int) string {
	if driver == DriverOracle || driver == DriverPostgres {
		var x string
		if driver == DriverOracle {
			x = ":val"
		} else {
			x = "$"
		}
		for i := 0; i < n; i++ {
			count := i + 1
			query = strings.Replace(query, "?", x+fmt.Sprintf("%v", count), 1)
		}
	}
	return query
}
