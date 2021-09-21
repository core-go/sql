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
	Client pb.DbProxyClient
	Conn   *grpc.ClientConn
}

func NewGRPCClient(url string) (*GRPCClient, error) {
	conn, err := grpc.Dial(url, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	c := pb.NewDbProxyClient(conn)
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
	rq := &pb.Request{Query: stm.Query, Params: argsData.Bytes(), Dates: dates}
	rs, er2 := c.Client.Execute(ctx, rq)
	if er2 != nil {
		return -1, er2
	}
	return rs.Result, er2
}
func (c *GRPCClient) ExecBatch(ctx context.Context, master bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	batch := make([]*pb.Request, 0)
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
		js := &pb.Request{Query: s.Query, Params: argsData.Bytes(), Dates: dates}
		batch = append(batch, js)
	}
	sm := ""
	if master {
		sm = "true"
	}
	rq := &pb.BatchRequest{Batch: batch, Master: sm}
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
	rq := &pb.Request{Query: stm.Query, Params: argsData.Bytes(), Dates: dates}
	rs, er2 := c.Client.Query(ctx, rq)
	if er2 != nil {
		return er2
	}
	x := json.NewDecoder(bytes.NewBuffer([]byte(rs.Message)))
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
	rq := &pb.Request{Query: stm.Query, Params: argsData.Bytes(), Dates: dates, Tx: tx, Commit: sc}
	rs, er2 := c.Client.Execute(ctx, rq)
	if er2 != nil {
		return -1, er2
	}
	return rs.Result, er2
}
func (c *GRPCClient) ExecBatchWithTx(ctx context.Context, tx string, commit bool, master bool, stm...sql.Statement) (int64, error) {
	stmts := sql.BuildJStatements(stm...)
	if len(stmts) == 0 {
		return 0, nil
	}
	batch := make([]*pb.Request, 0)
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
		js := &pb.Request{Query: s.Query, Params: argsData.Bytes(), Dates: dates}
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
	rq := &pb.BatchRequest{Batch: batch, Tx: tx, Commit: sc, Master: sm}
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
	rq := &pb.Request{Query: stm.Query, Params: argsData.Bytes(), Dates: dates, Tx: tx, Commit: sc}
	rs, er2 := c.Client.Query(ctx, rq)
	if er2 != nil {
		return er2
	}
	x := json.NewDecoder(bytes.NewBuffer([]byte(rs.Message)))
	er3 := x.Decode(result)
	return er3
}

func (c *GRPCClient) Insert(ctx context.Context, table string, model interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	s, values := sql.BuildToInsert(table, model, buildParam, nil, boolSupport)
	return c.Exec(ctx, s, values...)
}
func (c *GRPCClient) Update(ctx context.Context, table string, model interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	s, values := sql.BuildToUpdate(table, model, buildParam, nil, boolSupport)
	return c.Exec(ctx, s, values...)
}
func (c *GRPCClient) Save(ctx context.Context, table string, model interface{}, driver string, options...sql.Schema) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverPostgres || driver == sql.DriverOracle || driver == sql.DriverMysql || driver == sql.DriverMssql || driver == sql.DriverSqlite3 {
		s, values, err := sql.BuildToSaveWithSchema(table, model, driver, buildParam, nil, options...)
		if err != nil {
			return -1, err
		}
		return c.Exec(ctx, s, values...)
	} else {
		s, values := sql.BuildToInsertWithSchema(table, model, -1, buildParam, true, true, nil, options...)
		return c.Exec(ctx, s, values...)
	}
}
func (c *GRPCClient) InsertBatch(ctx context.Context, table string, models interface{}, driver string) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	if driver == sql.DriverPostgres || driver == sql.DriverOracle || driver == sql.DriverMysql || driver == sql.DriverMssql || driver == sql.DriverSqlite3 {
		s, values, err := sql.BuildToInsertBatch(table, models, driver, nil, buildParam)
		if err != nil {
			return -1, err
		}
		return c.Exec(ctx, s, values...)
	} else {
		boolSupport := driver == sql.DriverCassandra
		s, er0 := sql.BuildInsertStatements(table, models, buildParam, nil, boolSupport)
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
func (c *GRPCClient) UpdateBatch(ctx context.Context, table string, models interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	s, err := sql.BuildToUpdateBatch(table, models, buildParam, nil, boolSupport)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatch(ctx, false, s...)
	} else {
		return 0, nil
	}
}
func (c *GRPCClient) SaveBatch(ctx context.Context, table string, models interface{}, driver string) (int64, error) {
	if driver == sql.DriverCassandra {
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, sql.BuildParam, nil, true, true)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatch(ctx, false, s...)
		} else {
			return 0, nil
		}
	} else {
		s, er1 := sql.BuildToSaveBatch(table, models, driver, nil)
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
func (c *GRPCClient) InsertWithTx(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	commit := true
	s, values := sql.BuildToInsert(table, model, buildParam, nil, boolSupport)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *GRPCClient) UpdateWithTx(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	commit := true
	s, values := sql.BuildToUpdate(table, model, buildParam, nil, boolSupport)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *GRPCClient) SaveWithTx(ctx context.Context, tx string, table string, model interface{}, driver string, options...bool) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	commit := false
	if len(options) > 0 {
		commit = options[0]
	}
	if driver == sql.DriverCassandra {
		s, values := sql.BuildToInsertWithSchema(table, model, -1, buildParam, true, true, nil)
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	} else {
		s, values, err := sql.BuildToSaveWithSchema(table, model, driver, buildParam, nil)
		if err != nil {
			return -1, err
		}
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	}
}
func (c *GRPCClient) InsertBatchWithTx(ctx context.Context, tx string, table string, models interface{}, driver string, options...bool) (int64, error) {
	buildParam := sql.GetBuildByDriver(driver)
	commit := false
	if len(options) > 0 {
		commit = options[0]
	}
	if driver == sql.DriverCassandra {
		s, er0 := sql.BuildInsertStatements(table, models, buildParam, nil, true)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
		} else {
			return 0, nil
		}
	} else {
		s, values, err := sql.BuildToInsertBatch(table, models, driver, nil, buildParam)
		if err != nil {
			return -1, err
		}
		return c.ExecWithTx(ctx, tx, commit, s, values...)
	}
}
func (c *GRPCClient) UpdateBatchWithTx(ctx context.Context, tx string, table string, models interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	commit := false
	if len(options) > 1 {
		commit = options[1]
	}
	s, err := sql.BuildToUpdateBatch(table, models, buildParam, nil, boolSupport)
	if err != nil {
		return -1, err
	}
	if len(s) > 0 {
		return c.ExecBatchWithTx(ctx, tx,  commit, false, s...)
	} else {
		return 0, nil
	}
}
func (c *GRPCClient) SaveBatchWithTx(ctx context.Context, tx string, table string, models interface{}, driver string, options...bool) (int64, error) {
	commit := false
	if len(options) > 0 {
		commit = options[0]
	}
	if driver == sql.DriverCassandra {
		s, er0 := sql.BuildInsertStatementsWithVersion(table, models, -1, sql.BuildParam, nil, true, true)
		if er0 != nil {
			return -1, er0
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx, commit, false, s...)
		} else {
			return 0, nil
		}
	} else {
		s, er1 := sql.BuildToSaveBatch(table, models, driver, nil)
		if er1 != nil {
			return -1, er1
		}
		if len(s) > 0 {
			return c.ExecBatchWithTx(ctx, tx,  commit, false, s...)
		} else {
			return 0, nil
		}
	}
}

func (c *GRPCClient) InsertAndCommit(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	return c.InsertWithTx(ctx, tx, table, model, buildParam, boolSupport, true)
}
func (c *GRPCClient) UpdateAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	return c.UpdateWithTx(ctx, tx, table, model, buildParam, boolSupport, true)
}
func (c *GRPCClient) SaveAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string) (int64, error) {
	return c.SaveWithTx(ctx, tx, table, model, driver, true)
}
func (c *GRPCClient) InsertBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string) (int64, error) {
	return c.InsertBatchWithTx(ctx, tx, table, models, driver, true)
}
func (c *GRPCClient) UpdateBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, buildParam func(int) string, options...bool) (int64, error) {
	boolSupport := false
	if len(options) > 0 {
		boolSupport = options[0]
	}
	return c.UpdateBatchWithTx(ctx, tx, table, models, buildParam, boolSupport, true)
}
func (c *GRPCClient) SaveBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string) (int64, error) {
	return c.SaveBatchWithTx(ctx, tx, table, models, driver, true)
}
