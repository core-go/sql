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
