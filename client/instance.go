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

func (c *ProxyClient) Insert(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, values := sql.BuildToInsertWithVersion(table, model, -1, buildParam, boolSupport, nil, options...)
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) Update(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, values := sql.BuildToUpdateWithVersion(table, model, -1, buildParam, boolSupport, nil, options...)
	return c.Exec(ctx, s, values...)
}
func (c *ProxyClient) Save(ctx context.Context, table string, model interface{}, driver string, options...*sql.Schema) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverCassandra {
		s, values := sql.BuildToInsertWithSchema(table, model, -1, buildParam, true, true, nil, options...)
		return c.Exec(ctx, s, values...)
	} else {
		s, values, err := sql.BuildToSaveWithSchema(table, model, driver, buildParam, nil, options...)
		if err != nil {
			return -1, err
		}
		return c.Exec(ctx, s, values...)
	}
}
func (c *ProxyClient) InsertBatch(ctx context.Context, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverPostgres || driver == sql.DriverOracle || driver == sql.DriverMysql || driver == sql.DriverMssql || driver == sql.DriverSqlite3 {
		s, values, err := sql.BuildToInsertBatchWithSchema(table, models, driver, nil, buildParam, options...)
		if err != nil {
			return -1, err
		}
		return c.Exec(ctx, s, values...)
	} else {
		boolSupport := driver == sql.DriverCassandra
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, buildParam, boolSupport, nil, true, options...)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatch(ctx, false, s...)
		} else {
			return 0, nil
		}
	}
}
func (c *ProxyClient) UpdateBatch(ctx context.Context, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, err := sql.BuildToUpdateBatchWithVersion(table, models, -1, buildParam, boolSupport, nil, options...)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatch(ctx, false, s...)
	} else {
		return 0, nil
	}
}
func (c *ProxyClient) SaveBatch(ctx context.Context, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	if driver == sql.DriverCassandra {
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, sql.BuildParam, true, nil, true, options...)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatch(ctx, false, s...)
		} else {
			return 0, nil
		}
	} else {
		s, er1 := sql.BuildToSaveBatchWithArray(table, models, driver, nil, options...)
		if er1 != nil {
			return -1, er1
		}
		if len(s) > 0 {
			return c.ExecBatch(ctx, false, s...)
		} else {
			return 0, nil
		}
	}
}
func (c *ProxyClient) InsertWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, values := sql.BuildToInsertWithVersion(table, model, -1, buildParam, boolSupport, nil, options...)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) UpdateWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, values := sql.BuildToUpdateWithVersion(table, model, -1, buildParam, boolSupport, nil, options...)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *ProxyClient) SaveWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, driver string, options...*sql.Schema) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverCassandra {
		s, values := sql.BuildToInsertWithSchema(table, model, -1, buildParam, true, true, nil, options...)
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	} else {
		s, values, err := sql.BuildToSaveWithSchema(table, model, driver, buildParam, nil, options...)
		if err != nil {
			return -1, err
		}
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	}
}
func (c *ProxyClient) InsertBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverPostgres || driver == sql.DriverOracle || driver == sql.DriverMysql || driver == sql.DriverMssql || driver == sql.DriverSqlite3 {
		s, values, err := sql.BuildToInsertBatchWithSchema(table, models, driver, nil, buildParam, options...)
		if err != nil {
			return -1, err
		}
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	} else {
		boolSupport := driver == sql.DriverCassandra
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, buildParam, boolSupport, nil, true, options...)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
		} else {
			return 0, nil
		}
	}
}
func (c *ProxyClient) UpdateBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	s, err := sql.BuildToUpdateBatchWithVersion(table, models, -1, buildParam, boolSupport, nil, options...)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
	} else {
		return 0, nil
	}
}
func (c *ProxyClient) SaveBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	if driver == sql.DriverCassandra {
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, sql.BuildParam, true, nil, true, options...)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
		} else {
			return 0, nil
		}
	} else {
		s, er1 := sql.BuildToSaveBatchWithArray(table, models, driver, nil, options...)
		if er1 != nil {
			return -1, er1
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
		} else {
			return 0, nil
		}
	}
}

func (c *ProxyClient) InsertAndCommit(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	return c.InsertWithTx(ctx, tx, true, table, model, buildParam, boolSupport, options...)
}
func (c *ProxyClient) UpdateAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	return c.UpdateWithTx(ctx, tx, true, table, model, buildParam, boolSupport, options...)
}
func (c *ProxyClient) SaveAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, options...*sql.Schema) (int64, error) {
	return c.SaveWithTx(ctx, tx, true, table, model, driver, options...)
}
func (c *ProxyClient) InsertBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	return c.InsertBatchWithTx(ctx, tx, true, table, models, driver, options...)
}
func (c *ProxyClient) UpdateBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, buildParam func(int) string, boolSupport bool, options...*sql.Schema) (int64, error) {
	return c.UpdateBatchWithTx(ctx, tx, true, table, models, buildParam, boolSupport, options...)
}
func (c *ProxyClient) SaveBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options...*sql.Schema) (int64, error) {
	return c.SaveBatchWithTx(ctx, tx, true, table, models, driver, options...)
}
