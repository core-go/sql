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
}

func NewProxyClient(client *http.Client, url string) *ProxyClient {
	return &ProxyClient{client, url}
}
func (c *ProxyClient) BeginTransaction(ctx context.Context, timeout int64) (string, error) {
	var s string
	st := ""
	if timeout > 0 {
		st = "?timeout=" + strconv.FormatInt(timeout, 10)
	}
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/begin" + st, "", &s, )
	return s, err
}
func (c *ProxyClient) CommitTransaction(ctx context.Context, tx string) error {
	var s string
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/end?tx=" + tx, "", &s, )
	return err
}
func (c *ProxyClient) RollbackTransaction(ctx context.Context, tx string) error {
	var s string
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/end?rollback=true&tx=" + tx, "", &s, )
	return err
}
func (c *ProxyClient) Exec(ctx context.Context, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/exec", stm, &r)
	return r, err
}
func (c *ProxyClient) ExecBatch(ctx context.Context, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/exec-batch", stmts, &r)
	return r, err
}
func (c *ProxyClient) Query(ctx context.Context, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/query", stm, result)
	return err
}

func (c *ProxyClient) ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/exec?tx=" + tx +sc, stm, &r)
	return r, err
}
func (c *ProxyClient) ExecBatchWithTx(ctx context.Context, tx string, commit bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	var r int64
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/exec-batch?tx=" + tx +sc, stmts, &r)
	return r, err
}
func (c *ProxyClient) QueryWithTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	sc := ""
	if commit {
		sc = "&commit=true"
	}
	err := PostWithClientAndDecode(ctx, c.Client, c.Url + "/query?tx=" + tx + sc, stm, &result)
	return err
}
