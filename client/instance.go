package client

import (
	"context"
	"net/http"
	"strconv"

	"github.com/core-go/sql"
)

type ProxyClient struct {
	Client *http.Client
	Url    string
	Log    func(context.Context, string, map[string]interface{})
}

func NewProxyClient(client *http.Client, url string, options...func(context.Context, string, map[string]interface{})) *ProxyClient {
	var log func(context.Context, string, map[string]interface{})
	if len(options) > 0 {
		log = options[0]
	}
	return &ProxyClient{client, url, log}
}
func (c *ProxyClient) BeginTransaction(ctx context.Context, timeout int64) (string, error) {
	var s string
	st := ""
	if timeout > 0 {
		st = "?timeout=" + strconv.FormatInt(timeout, 10)
	}
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/begin"+st, "", &s, c.Log)
	return s, err
}
func (c *ProxyClient) CommitTransaction(ctx context.Context, tx string) error {
	var s string
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/end?tx="+tx, "", &s, c.Log)
	return err
}
func (c *ProxyClient) RollbackTransaction(ctx context.Context, tx string) error {
	var s string
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/end?rollback=true&tx="+tx, "", &s, )
	return err
}
func (c *ProxyClient) Exec(ctx context.Context, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/exec", stm, &r, c.Log)
	return r, err
}
func (c *ProxyClient) ExecBatch(ctx context.Context, master bool, stm ...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	sm := ""
	if master {
		sm = "?master=true"
	}
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/exec-batch"+sm, stmts, &r, c.Log)
	return r, err
}
func (c *ProxyClient) Query(ctx context.Context, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/query", stm, result, c.Log)
	return err
}

func (c *ProxyClient) ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/exec?tx="+tx+sc, stm, &r, c.Log)
	return r, err
}
func (c *ProxyClient) ExecBatchWithTx(ctx context.Context, tx string, commit bool, master bool, stm ...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	sm := ""
	if master {
		sm = "&master=true"
	}
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/exec-batch?tx="+tx+sc+sm, stmts, &r, c.Log)
	return r, err
}
func (c *ProxyClient) QueryWithTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	err := PostWithClientAndDecode(ctx, c.Client, c.Url+"/query?tx="+tx+sc, stm, &result, c.Log)
	return err
}

func (c *ProxyClient) Insert(ctx context.Context, table string, model interface{}, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.BuildDollarParam
	}
	s, values := sql.BuildToInsert(table, model, buildParam)
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) Update(ctx context.Context, table string, model interface{}, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.BuildDollarParam
	}
	s, values := sql.BuildToUpdate(table, model, buildParam)
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) Save(ctx context.Context, table string, model interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, values, err := sql.BuildToSave(table, model, driver, buildParam)
	if err != nil {
		return -1, err
	}
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) InsertBatch(ctx context.Context, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, values, err := sql.BuildToInsertBatch(table, models, driver, buildParam)
	if err != nil {
		return -1, err
	}
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) UpdateBatch(ctx context.Context, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, err := sql.BuildToUpdateBatch(table, models, buildParam, driver)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatch(ctx, false, s...)
	} else {
		return 0, nil
	}
}
func (c *ProxyClient) SaveBatch(ctx context.Context, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, err := sql.BuildToSaveBatch(table, models, driver, buildParam)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatch(ctx, false, s...)
	} else {
		return 0, nil
	}
}
func (c *ProxyClient) InsertWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.BuildDollarParam
	}
	s, values := sql.BuildToInsert(table, model, buildParam)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) UpdateWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.BuildDollarParam
	}
	s, values := sql.BuildToUpdate(table, model, buildParam)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) SaveWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, values, err := sql.BuildToSave(table, model, driver, buildParam)
	if err != nil {
		return -1, err
	}
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) InsertBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, values, err := sql.BuildToInsertBatch(table, models, driver, buildParam)
	if err != nil {
		return -1, err
	}
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) UpdateBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, err := sql.BuildToUpdateBatch(table, models, buildParam, driver)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatchWithTx(ctx, tx,  commit, false, s...)
	} else {
		return 0, nil
	}
}
func (c *ProxyClient) SaveBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, options...string) (int64, error) {
	driver := sql.DriverPostgres
	var buildParam func(int) string
	if len(options) > 0 {
		driver = options[0]
	}
	buildParam = sql.GetBuildByDriver(driver)
	s, err := sql.BuildToSaveBatch(table, models, driver, buildParam)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatchWithTx(ctx, tx,  commit, false, s...)
	} else {
		return 0, nil
	}
}
