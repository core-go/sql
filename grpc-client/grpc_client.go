package grpc_client

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/core-go/sql"
	pb "github.com/core-go/sql/grpc"
	"google.golang.org/grpc"
)

type GRPCClient struct {
	Url    string
	Client pb.GoDbProxyClient
	Conn   *grpc.ClientConn
}

func NewGRPCClient(url string) (*GRPCClient, error) {
	conn, err := grpc.Dial(url, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	c := pb.NewGoDbProxyClient(conn)
	return &GRPCClient{url, c, conn}, nil
}
func (c *GRPCClient) BeginTransaction(ctx context.Context, timeout int64) (string, error) {
	rq := &pb.BeginTransactionRequest{Timeout: timeout}
	rs, err := c.Client.BeginTransaction(ctx, rq)
	if err != nil {
		return "", err
	}
	return rs.Id, err
}
func (c *GRPCClient) CommitTransaction(ctx context.Context, tx string) error {
	rq := &pb.EndTransactionRequest{Tx: tx}
	_, err := c.Client.EndTransaction(ctx, rq)
	return err
}
func (c *GRPCClient) RollbackTransaction(ctx context.Context, tx string) error {
	rq := &pb.EndTransactionRequest{Tx: tx, Rollback: "true"}
	_, err := c.Client.EndTransaction(ctx, rq)
	return err
}
func (c *GRPCClient) Exec(ctx context.Context, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	argsData := new(bytes.Buffer)
	er1 := json.NewEncoder(argsData).Encode(&stm.Params)
	if er1 != nil {
		return -1, er1
	}
	var dates []int32
	if stm.Dates != nil && len(stm.Dates) > 0 {
		for _, v := range stm.Dates {
			dates = append(dates, int32(v))
		}
	}
	rq := &pb.JStatementRequest{Query: stm.Query, Params: argsData.Bytes(), Dates: dates}
	rs, er2 := c.Client.Execute(ctx, rq)
	if er2 != nil {
		return -1, er2
	}
	return rs.Details, er2
}
func (c *GRPCClient) ExecBatch(ctx context.Context, master bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	batch := make([]*pb.JStatementRequest, 0)
	for _, s := range stmts {
		argsData := new(bytes.Buffer)
		er1 := json.NewEncoder(argsData).Encode(&s.Params)
		if er1 != nil {
			return -1, er1
		}
		d := sql.ToDates(s.Params)
		var dates []int32
		if d != nil && len(d) > 0 {
			for _, v := range d {
				dates = append(dates, int32(v))
			}
		}
		js := &pb.JStatementRequest{Query: s.Query, Params: argsData.Bytes(), Dates: dates}
		batch = append(batch, js)
	}
	sm := ""
	if master {
		sm = "true"
	}
	rq := &pb.JStatementBatchRequest{Batch: batch, Master: sm}
	rs, err := c.Client.ExecBatch(ctx, rq)
	if err != nil {
		return 0, err
	}
	return rs.Result, err
}
func (c *GRPCClient) Query(ctx context.Context, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	argsData := new(bytes.Buffer)
	er1 := json.NewEncoder(argsData).Encode(&stm.Params)
	if er1 != nil {
		return er1
	}
	var dates []int32
	if stm.Dates != nil && len(stm.Dates) > 0 {
		for _, v := range stm.Dates {
			dates = append(dates, int32(v))
		}
	}
	rq := &pb.JStatementRequest{Query: stm.Query, Params: argsData.Bytes(), Dates: dates}
	rs, er2 := c.Client.Query(ctx, rq)
	if er2 != nil {
		return er2
	}
	x := json.NewDecoder(bytes.NewBuffer([]byte(rs.Details)))
	er3 := x.Decode(result)
	return er3
}

func (c *GRPCClient) ExecWithTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error) {
	stm := sql.BuildStatement(query, values...)
	argsData := new(bytes.Buffer)
	er1 := json.NewEncoder(argsData).Encode(&stm.Params)
	if er1 != nil {
		return -1, er1
	}
	var dates []int32
	if stm.Dates != nil && len(stm.Dates) > 0 {
		for _, v := range stm.Dates {
			dates = append(dates, int32(v))
		}
	}
	sc := ""
	if commit {
		sc = "true"
	}
	rq := &pb.JStatementRequest{Query: stm.Query, Params: argsData.Bytes(), Dates: dates, Tx: tx, Commit: sc}
	rs, er2 := c.Client.Execute(ctx, rq)
	if er2 != nil {
		return -1, er2
	}
	return rs.Details, er2
}
func (c *GRPCClient) ExecBatchWithTx(ctx context.Context, tx string, commit bool, master bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	batch := make([]*pb.JStatementRequest, 0)
	for _, s := range stmts {
		argsData := new(bytes.Buffer)
		er1 := json.NewEncoder(argsData).Encode(&s.Params)
		if er1 != nil {
			return -1, er1
		}
		d := sql.ToDates(s.Params)
		var dates []int32
		if d != nil && len(d) > 0 {
			for _, v := range d {
				dates = append(dates, int32(v))
			}
		}
		js := &pb.JStatementRequest{Query: s.Query, Params: argsData.Bytes(), Dates: dates}
		batch = append(batch, js)
	}
	sc := ""
	if commit {
		sc = "true"
	}
	sm := ""
	if master {
		sm = "true"
	}
	rq := &pb.JStatementBatchRequest{Batch: batch, Tx: tx, Commit: sc, Master: sm}
	rs, err := c.Client.ExecBatch(ctx, rq)
	if err != nil {
		return 0, err
	}
	return rs.Result, err
}
func (c *GRPCClient) QueryWithTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error {
	stm := sql.BuildStatement(query, values...)
	argsData := new(bytes.Buffer)
	er1 := json.NewEncoder(argsData).Encode(&stm.Params)
	if er1 != nil {
		return er1
	}
	var dates []int32
	if stm.Dates != nil && len(stm.Dates) > 0 {
		for _, v := range stm.Dates {
			dates = append(dates, int32(v))
		}
	}
	sc := ""
	if commit {
		sc = "true"
	}
	rq := &pb.JStatementRequest{Query: stm.Query, Params: argsData.Bytes(), Dates: dates, Tx: tx, Commit: sc}
	rs, er2 := c.Client.Query(ctx, rq)
	if er2 != nil {
		return er2
	}
	x := json.NewDecoder(bytes.NewBuffer([]byte(rs.Details)))
	er3 := x.Decode(result)
	return er3
}
