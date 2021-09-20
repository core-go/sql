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

func (c *GRPCClient) Insert(ctx context.Context, table string, model interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values := sql.BuildToInsert(table, model, buildParam, nil, boolSupport)
	return c.Exec(ctx, s, values...)
}
func (c *GRPCClient) Update(ctx context.Context, table string, model interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values := sql.BuildToUpdate(table, model, buildParam, nil, boolSupport)
	return c.Exec(ctx, s, values...)
}
func (c *GRPCClient) Save(ctx context.Context, table string, model interface{}, options...string) (int64, error) {
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
func (c *GRPCClient) InsertBatch(ctx context.Context, table string, models interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	// boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values, err := sql.BuildToInsertBatch(table, models, driver, nil, buildParam)
	if err != nil {
		return -1, err
	}
	return c.Exec(ctx, s, values...)
}
func (c *GRPCClient) UpdateBatch(ctx context.Context, table string, models interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
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
func (c *GRPCClient) SaveBatch(ctx context.Context, table string, models interface{}, options...string) (int64, error) {
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
func (c *GRPCClient) InsertWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values := sql.BuildToInsert(table, model, buildParam, nil, boolSupport)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *GRPCClient) UpdateWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values := sql.BuildToUpdate(table, model, buildParam, nil, boolSupport)
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *GRPCClient) SaveWithTx(ctx context.Context, tx string, commit bool, table string, model interface{}, options...string) (int64, error) {
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
func (c *GRPCClient) InsertBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	// boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
	s, values, err := sql.BuildToInsertBatch(table, models, driver, nil, buildParam)
	if err != nil {
		return -1, err
	}
	return c.ExecWithTx(ctx, tx, commit, s, values...)
}
func (c *GRPCClient) UpdateBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = sql.GetBuildByDriver(driver)
	}
	boolSupport := driver == sql.DriverPostgres || driver == sql.DriverCassandra
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
func (c *GRPCClient) SaveBatchWithTx(ctx context.Context, tx string, commit bool, table string, models interface{}, options...string) (int64, error) {
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
