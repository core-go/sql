package action

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func BuildParam(i int) string {
	return "?"
}
func BuildOracleParam(i int) string {
	return ":" + strconv.Itoa(i)
}
func BuildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func BuildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func GetBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return BuildDollarParam
	case "*godror.drv":
		return BuildOracleParam
	case "*mssql.Driver":
		return BuildMsSqlParam
	default:
		return BuildParam
	}
}

type DBConfig struct {
	DataSourceName string `yaml:"data_source_name" mapstructure:"data_source_name" json:"dataSourceName,omitempty" gorm:"column:datasourcename" bson:"dataSourceName,omitempty" dynamodbav:"dataSourceName,omitempty" firestore:"dataSourceName,omitempty"`
	Driver         string `yaml:"driver" mapstructure:"driver" json:"driver,omitempty" gorm:"column:driver" bson:"driver,omitempty" dynamodbav:"driver,omitempty" firestore:"driver,omitempty"`
}
type ActionLogConf struct {
	Log    bool            `yaml:"log" mapstructure:"log" json:"log,omitempty" gorm:"column:log" bson:"log,omitempty" dynamodbav:"log,omitempty" firestore:"log,omitempty"`
	DB     DBConfig        `yaml:"db" mapstructure:"db" json:"db,omitempty" gorm:"column:db" bson:"db,omitempty" dynamodbav:"db,omitempty" firestore:"db,omitempty"`
	Schema ActionLogSchema `yaml:"schema" mapstructure:"schema" json:"schema,omitempty" gorm:"column:schema" bson:"schema,omitempty" dynamodbav:"schema,omitempty" firestore:"schema,omitempty"`
	Config ActionLogConfig `yaml:"config" mapstructure:"config" json:"config,omitempty" gorm:"column:config" bson:"config,omitempty" dynamodbav:"config,omitempty" firestore:"config,omitempty"`
}
type ActionLogSchema struct {
	Id        string    `yaml:"id" mapstructure:"id" json:"id,omitempty" gorm:"column:id" bson:"_id,omitempty" dynamodbav:"id,omitempty" firestore:"id,omitempty"`
	User      string    `yaml:"user" mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip        string    `yaml:"ip" mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	Resource  string    `yaml:"resource" mapstructure:"resource" json:"resource,omitempty" gorm:"column:resource" bson:"resource,omitempty" dynamodbav:"resource,omitempty" firestore:"resource,omitempty"`
	Action    string    `yaml:"action" mapstructure:"action" json:"action,omitempty" gorm:"column:action" bson:"action,omitempty" dynamodbav:"action,omitempty" firestore:"action,omitempty"`
	Timestamp string    `yaml:"timestamp" mapstructure:"timestamp" json:"timestamp,omitempty" gorm:"column:timestamp" bson:"timestamp,omitempty" dynamodbav:"timestamp,omitempty" firestore:"timestamp,omitempty"`
	Status    string    `yaml:"status" mapstructure:"status" json:"status,omitempty" gorm:"column:status" bson:"status,omitempty" dynamodbav:"status,omitempty" firestore:"status,omitempty"`
	Desc      string    `yaml:"desc" mapstructure:"desc" json:"desc,omitempty" gorm:"column:desc" bson:"desc,omitempty" dynamodbav:"desc,omitempty" firestore:"desc,omitempty"`
	Ext       *[]string `yaml:"ext" mapstructure:"ext" json:"ext,omitempty" gorm:"column:ext" bson:"ext,omitempty" dynamodbav:"ext,omitempty" firestore:"ext,omitempty"`
}
type ActionLogConfig struct {
	User       string `yaml:"user" mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Ip         string `yaml:"ip" mapstructure:"ip" json:"ip,omitempty" gorm:"column:ip" bson:"ip,omitempty" dynamodbav:"ip,omitempty" firestore:"ip,omitempty"`
	True       string `yaml:"true" mapstructure:"true" json:"true,omitempty" gorm:"column:true" bson:"true,omitempty" dynamodbav:"true,omitempty" firestore:"true,omitempty"`
	False      string `yaml:"false" mapstructure:"false" json:"false,omitempty" gorm:"column:false" bson:"false,omitempty" dynamodbav:"false,omitempty" firestore:"false,omitempty"`
	Goroutines bool   `yaml:"goroutines" mapstructure:"goroutines" json:"goroutines,omitempty" gorm:"column:goroutines" bson:"goroutines,omitempty" dynamodbav:"goroutines,omitempty" firestore:"goroutines,omitempty"`
}

type ActionLogWriter struct {
	Database   *sql.DB
	Table      string
	Config     ActionLogConfig
	Schema     ActionLogSchema
	Generate   func(ctx context.Context) (string, error)
	BuildParam func(i int) string
}

func NewSqlActionLogWriter(database *sql.DB, tableName string, config ActionLogConfig, s ActionLogSchema, options ...func(context.Context) (string, error)) *ActionLogWriter {
	var generate func(context.Context) (string, error)
	if len(options) > 0 && options[0] != nil {
		generate = options[0]
	}
	return NewActionLogWriter(database, tableName, config, s, generate)
}
func NewActionLogWriter(database *sql.DB, tableName string, config ActionLogConfig, s ActionLogSchema, generate func(context.Context) (string, error), options ...func(i int) string) *ActionLogWriter {
	s.Id = strings.ToLower(s.Id)
	s.User = strings.ToLower(s.User)
	s.Resource = strings.ToLower(s.Resource)
	s.Action = strings.ToLower(s.Action)
	s.Timestamp = strings.ToLower(s.Timestamp)
	s.Status = strings.ToLower(s.Status)
	s.Desc = strings.ToLower(s.Desc)
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
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(database)
	}
	writer := ActionLogWriter{Database: database, Table: tableName, Config: config, Schema: s, Generate: generate, BuildParam: buildParam}
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
	query, vars := BuildInsertSQL(s.Table, log, s.BuildParam)
	_, err := s.Database.ExecContext(ctx, query, vars...)
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
func BuildInsertSQL(tableName string, model map[string]interface{}, buildParam func(i int) string) (string, []interface{}) {
	var cols []string
	var values []interface{}
	for col, v := range model {
		cols = append(cols, col)
		values = append(values, v)
	}
	column := fmt.Sprintf("(%v)", strings.Join(cols, ","))
	numCol := len(cols)
	var arrValue []string
	for i := 1; i <= numCol; i++ {
		param := buildParam(i)
		arrValue = append(arrValue, param)
	}
	value := fmt.Sprintf("(%v)", strings.Join(arrValue, ","))
	strSQL := fmt.Sprintf("insert into %v %v values %v", tableName, column, value)
	return strSQL, values
}
