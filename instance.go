package client

import (
	"context"
	"net/http"

	"github.com/core-go/sql"
)

type ProxyClient struct {
	Client *http.Client
	Url    string
}

func NewProxyClient(client *http.Client, url string) *ProxyClient {
	return &ProxyClient{client, url}
}
func (c *ProxyClient) BeginTransaction(ctx context.Context) (string, error) {
	var s string
	err := PostAndDecode(ctx, c.Url + "/begin", "", &s, )
	return s, err
}
func (c *ProxyClient) CommitTransaction(ctx context.Context, tx string) (string, error) {
	var s string
	err := PostAndDecode(ctx, c.Url + "/end?tx=" + tx, "", &s, )
	return s, err
}
func (c *ProxyClient) RollbackTransaction(ctx context.Context, tx string) (string, error) {
	var s string
	err := PostAndDecode(ctx, c.Url + "/end?roleback=true&tx=" + tx, "", &s, )
	return s, err
}
func (c *ProxyClient) Exec(ctx context.Context, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	var r int64
	err := PostAndDecode(ctx, c.Url + "/exec", stm, &r)
	return r, err
}
func (c *ProxyClient) ExecBatch(ctx context.Context, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	var r int64
	err := PostAndDecode(ctx, c.Url + "/exec-batch", stmts, &r)
	return r, err
}
func (c *ProxyClient) Query(ctx context.Context, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	err := PostAndDecode(ctx, c.Url + "/query", stm, &result)
	return err
}

func (c *ProxyClient) ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	scommit := ""
	if commit {
		scommit = "&commit=true"
	}
	var r int64
	err := PostAndDecode(ctx, c.Url + "/exec?tx=" + tx + scommit, stm, &r)
	return r, err
}
func (c *ProxyClient) ExecBatchWithTx(ctx context.Context, tx string, commit bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	scommit := ""
	if commit {
		scommit = "&commit=true"
	}
	var r int64
	err := PostAndDecode(ctx, c.Url + "/exec-batch?tx=" + tx + scommit, stmts, &r)
	return r, err
}
func (c *ProxyClient) QueryWithTx(ctx context.Context, result interface{}, tx string, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	err := PostAndDecode(ctx, c.Url + "/query?tx=" + tx, stm, &result)
	return err
}
