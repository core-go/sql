package grpc_server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	q "github.com/core-go/sql"
	"github.com/core-go/sql/grpc"
	"time"
)

const d = 120 * time.Second

type GRPCHandler struct {
	grpc.UnimplementedGoDbProxyServer
	DB        *sql.DB
	Transform func(s string) string
	Cache     q.TxCache
	Generate  func(ctx context.Context) (string, error)
	Error     func(context.Context, string)
}

func NewHandler(db *sql.DB, transform func(s string) string, cache q.TxCache, generate func(ctx context.Context) (string, error), err func(context.Context, string)) *GRPCHandler {
	g := GRPCHandler{
		Transform: transform,
		Cache:     cache,
		DB:        db,
		Generate:  generate,
		Error:     err,
	}
	return &g
}

func CreateStatements(in *grpc.JStatementBatchRequest) ([]q.JStatement, error) {
	var (
		statements []q.JStatement
		err        error
	)
	for _, batch := range in.Batch {
		st := q.JStatement{
			Query: batch.Query,
		}
		err = json.NewDecoder(bytes.NewBuffer(batch.Params)).Decode(&st.Params)
		if err != nil {
			return nil, err
		}
		for _, date := range batch.Dates {
			st.Dates = append(st.Dates, int(date))
		}
		statements = append(statements, st)
	}
	return statements, err
}

func (s *GRPCHandler) Query(ctx context.Context, in *grpc.JStatementRequest) (*grpc.JStatementReply, error) {
	statement := q.JStatement{}
	err := json.NewDecoder(bytes.NewBuffer(in.Params)).Decode(&statement.Params)
	if err != nil {
		return &grpc.JStatementReply{Message: "Error: " + err.Error()}, err
	}
	statement.Query = in.Query
	for _, v := range in.Dates {
		statement.Dates = append(statement.Dates, int(v))
	}
	statement.Params = q.ParseDates(statement.Params, statement.Dates)
	stx := in.Tx
	if len(stx) == 0 {
		res, err := q.QueryMap(ctx, s.DB, s.Transform, statement.Query, statement.Params...)
		return &grpc.JStatementReply{
			Message: "success",
			Details: int64(len(res)),
		}, err
	} else {
		tx, er0 := s.Cache.Get(stx)
		if er0 != nil {
			return &grpc.JStatementReply{
				Message: "",
				Details: 0,
			}, er0
		}
		if tx == nil {
			return &grpc.JStatementReply{
				Message: "cannot get tx from cache. Maybe tx got timeout",
				Details: 0,
			}, err
		}
		res, er1 := q.QueryMapWithTx(ctx, tx, s.Transform, statement.Query, statement.Params...)
		if er1 != nil {
			return &grpc.JStatementReply{
				Message: "",
				Details: 0,
			}, er1
		}
		return &grpc.JStatementReply{
			Message: "success",
			Details: int64(len(res)),
		}, err
	}
}

func (s *GRPCHandler) Execute(ctx context.Context, in *grpc.JStatementRequest) (*grpc.JStatementReply, error) {
	statement := q.JStatement{}
	err := json.NewDecoder(bytes.NewBuffer(in.Params)).Decode(&statement.Params)
	if err != nil {
		return &grpc.JStatementReply{Message: "Error: " + err.Error()}, err
	}
	statement.Query = in.Query
	for _, v := range in.Dates {
		statement.Dates = append(statement.Dates, int(v))
	}
	statement.Params = q.ParseDates(statement.Params, statement.Dates)
	result, err := s.DB.Exec(statement.Query, statement.Params...)
	if err != nil {
		return &grpc.JStatementReply{Message: "Error: " + err.Error()}, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	return &grpc.JStatementReply{Message: "Execute Successfully", Details: affected}, err
}

func (s *GRPCHandler) ExecBatch(ctx context.Context, in *grpc.JStatementBatchRequest) (*grpc.JStatementBatchReply, error) {
	statements, err := CreateStatements(in)
	if err != nil {
		return &grpc.JStatementBatchReply{Message: "Error: " + err.Error(), Result: 0}, err
	}
	b := make([]q.Statement, 0)
	l := len(statements)
	for i := 0; i < l; i++ {
		st := q.Statement{}
		st.Query = statements[i].Query
		st.Params = q.ParseDates(statements[i].Params, statements[i].Dates)
		b = append(b, st)
	}
	stx := in.Tx
	var er1 error
	var res int64
	if len(stx) == 0 {
		master := in.Master
		if master == "true" {
			res, er1 = q.ExecuteBatch(ctx, s.DB, b, true, true)
		} else {
			res, er1 = q.ExecuteAll(ctx, s.DB, b...)
		}
	} else {
		tx, er0 := s.Cache.Get(stx)
		if er0 != nil {
			return &grpc.JStatementBatchReply{Message: "", Result: 0}, er0
		}
		if tx == nil {
			return &grpc.JStatementBatchReply{Message: "", Result: 0}, errors.New("cannot get tx from cache. Maybe tx got timeout")
		}
		tc := false
		commit := in.Commit
		if commit == "true" {
			tc = true
		}
		res, er1 = q.ExecuteStatements(ctx, tx, tc, b...)
		if tc && er1 == nil {
			s.Cache.Remove(stx)
		}
	}
	return &grpc.JStatementBatchReply{Message: "success", Result: res}, err
}

func (s *GRPCHandler) BeginTransaction(ctx context.Context, in *grpc.BeginTransactionRequest) (*grpc.BeginTransactionReply, error) {
	id, er0 := s.Generate(ctx)
	if er0 != nil {
		return &grpc.BeginTransactionReply{Id: ""}, er0
	}
	tx, er1 := s.DB.Begin()
	if er1 != nil {
		return &grpc.BeginTransactionReply{Id: ""}, er1
	}
	t := d
	st := in.Timeout
	if st > 0 {
		t = time.Duration(st) * time.Second
	}
	err := s.Cache.Put(id, tx, t)
	if err != nil {
		return nil, err
	}
	return &grpc.BeginTransactionReply{Id: id}, err
}

func (s *GRPCHandler) EndTransaction(ctx context.Context, in *grpc.EndTransactionRequest) (*grpc.EndTransactionReply, error) {
	stx := in.Tx
	if len(stx) == 0 {
		return nil, errors.New("tx is required")
	}
	tx, er0 := s.Cache.Get(stx)
	if er0 != nil {
		return nil, er0
	}
	if tx == nil {
		return nil, errors.New("cannot get tx from cache. Maybe tx got timeout")
	}
	rollback := in.Rollback
	if rollback == "true" {
		er1 := tx.Rollback()
		if er1 != nil {
			return &grpc.EndTransactionReply{Response: "false"}, er1
		} else {
			_, err := s.Cache.Remove(stx)
			if err != nil {
				return &grpc.EndTransactionReply{Response: "true"}, err
			}
			return &grpc.EndTransactionReply{Response: "false"}, err
		}
	} else {
		er1 := tx.Commit()
		if er1 != nil {
			return &grpc.EndTransactionReply{Response: ""}, er1
		} else {
			_, err := s.Cache.Remove(stx)
			if err != nil {
				return &grpc.EndTransactionReply{Response: "true"}, err
			}
			return &grpc.EndTransactionReply{Response: "false"}, err
		}
	}
}
