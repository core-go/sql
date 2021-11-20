package proxy

import (
	"context"
	"fmt"
	q "github.com/core-go/sql"
	"reflect"
	"strings"
)

const txs = "txId"

type Proxy interface {
	BeginTransaction(ctx context.Context, timeout int64) (string, error)
	CommitTransaction(ctx context.Context, tx string) error
	RollbackTransaction(ctx context.Context, tx string) error
	Exec(ctx context.Context, query string, values ...interface{}) (int64, error)
	ExecBatch(ctx context.Context, master bool, stm ...q.Statement) (int64, error)
	Query(ctx context.Context, result interface{}, query string, values ...interface{}) error
	QueryOne(ctx context.Context, result interface{}, query string, values ...interface{}) error
	ExecTx(ctx context.Context, tx string, commit bool, query string, values ...interface{}) (int64, error)
	ExecBatchTx(ctx context.Context, tx string, commit bool, master bool, stm ...q.Statement) (int64, error)
	QueryTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error
	QueryOneTx(ctx context.Context, tx string, commit bool, result interface{}, query string, values ...interface{}) error

	Insert(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	Update(ctx context.Context, table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	Save(ctx context.Context, table string, model interface{}, driver string, options ...*q.Schema) (int64, error)
	InsertBatch(ctx context.Context, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)
	UpdateBatch(ctx context.Context, table string, models interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	SaveBatch(ctx context.Context, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)

	InsertTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	UpdateTx(ctx context.Context, tx string, commit bool, table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	SaveTx(ctx context.Context, tx string, commit bool, table string, model interface{}, driver string, options ...*q.Schema) (int64, error)
	InsertBatchTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)
	UpdateBatchTx(ctx context.Context, tx string, commit bool, table string, models interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	SaveBatchTx(ctx context.Context, tx string, commit bool, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)

	InsertAndCommit(ctx context.Context, tx string, table string, model interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	UpdateAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	SaveAndCommit(ctx context.Context, tx string, table string, model interface{}, driver string, options ...*q.Schema) (int64, error)
	InsertBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)
	UpdateBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, buildParam func(int) string, boolSupport bool, options ...*q.Schema) (int64, error)
	SaveBatchAndCommit(ctx context.Context, tx string, table string, models interface{}, driver string, options ...*q.Schema) (int64, error)
}

type Loader struct {
	Proxy             Proxy
	BuildParam        func(i int) string
	Map               func(ctx context.Context, model interface{}) (interface{}, error)
	modelType         reflect.Type
	modelsType        reflect.Type
	keys              []string
	mapJsonColumnKeys map[string]string
	fieldsIndex       map[string]int
	table             string
	IsRollback        bool
}

func NewLoader(proxy Proxy, tableName string, modelType reflect.Type, buildParam func(i int) string, options ...func(context.Context, interface{}) (interface{}, error)) (*Loader, error) {
	_, idNames := q.FindPrimaryKeys(modelType)
	mapJsonColumnKeys := q.MapJsonColumn(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()

	fieldsIndex, er0 := q.GetColumnIndexes(modelType)
	if er0 != nil {
		return nil, er0
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Loader{Proxy: proxy, IsRollback: true, BuildParam: buildParam, Map: mp, modelType: modelType, modelsType: modelsType, keys: idNames, mapJsonColumnKeys: mapJsonColumnKeys, fieldsIndex: fieldsIndex, table: tableName}, nil
}

func (s *Loader) Keys() []string {
	return s.keys
}

func (s *Loader) All(ctx context.Context) (interface{}, error) {
	query := q.BuildSelectAllQuery(s.table)
	result := reflect.New(s.modelsType).Interface()
	var err error
	tx := q.GetTxId(ctx)
	if tx == nil {
		err = s.Proxy.Query(ctx, result, query)
	} else {
		err = s.Proxy.QueryTx(ctx, *tx, false, result, query)
		if err != nil {
			if s.IsRollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return result, err
		}
	}
	if err == nil {
		if s.Map != nil {
			return q.MapModels(ctx, result, s.Map)
		}
		return result, err
	}
	return result, err
}

func (s *Loader) Load(ctx context.Context, id interface{}) (interface{}, error) {
	queryFindById, values := q.BuildFindById(s.table, s.BuildParam, id, s.mapJsonColumnKeys, s.keys)
	tx := q.GetTxId(ctx)
	result := reflect.New(s.modelsType).Interface()
	var r interface{}
	var er1 error
	if tx == nil {
		er1 = s.Proxy.Query(ctx, result, queryFindById, values...)
	} else {
		er1 = s.Proxy.QueryTx(ctx, *tx, false, result, queryFindById, values...)
	}
	if er1 != nil {
		if s.IsRollback && tx != nil {
			s.Proxy.RollbackTransaction(ctx, *tx)
		}
		return r, er1
	}
	if s.Map != nil {
		_, er2 := s.Map(ctx, &r)
		if er2 != nil {
			return nil, er2
		}
		vo := reflect.Indirect(reflect.ValueOf(r))
		if vo.Kind() == reflect.Slice {
			if vo.Len() > 0 {
				return vo.Index(0).Addr(), nil
			}
			return nil, nil
		}
		return r, er2
	}
	vo := reflect.Indirect(reflect.ValueOf(r))
	if vo.Kind() == reflect.Slice {
		if vo.Len() > 0 {
			return vo.Index(0).Addr(), nil
		}
		return nil, nil
	}
	return r, er1
}

func (s *Loader) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	queryFindById, values := q.BuildFindById(s.table, s.BuildParam, id, s.mapJsonColumnKeys, s.keys)
	tx := q.GetTxId(ctx)
	var er1 error
	if tx == nil {
		er1 = s.Proxy.QueryOne(ctx, result, queryFindById, values...)
	} else {
		er1 = s.Proxy.QueryOneTx(ctx, *tx, false, result, queryFindById, values...)
	}
	if er1 != nil {
		if s.IsRollback && tx != nil {
			s.Proxy.RollbackTransaction(ctx, *tx)
		}
		return false, er1
	}
	if s.Map != nil {
		_, er2 := s.Map(ctx, result)
		if er2 != nil {
			return true, er2
		}
	}
	return true, er1
}

func (s *Loader) Exist(ctx context.Context, id interface{}) (bool, error) {
	var count map[string]int
	var where string
	var values []interface{}
	colNumber := 1
	if len(s.keys) == 1 {
		where = fmt.Sprintf("where %s = %s", s.mapJsonColumnKeys[s.keys[0]], s.BuildParam(colNumber))
		values = append(values, id)
		colNumber++
	} else {
		conditions := make([]string, 0)
		var ids = id.(map[string]interface{})
		for k, idk := range ids {
			columnName := s.mapJsonColumnKeys[k]
			conditions = append(conditions, fmt.Sprintf("%s = %s", columnName, s.BuildParam(colNumber)))
			values = append(values, idk)
			colNumber++
		}
		where = "where " + strings.Join(conditions, " and ")
	}
	var er1 error
	tx := q.GetTxId(ctx)
	if tx == nil {
		er1 = s.Proxy.QueryOne(ctx, &count, fmt.Sprintf("select count(*) as count from %s %s", s.table, where), values...)
	} else {
		er1 = s.Proxy.QueryOneTx(ctx, *tx, false, &count, fmt.Sprintf("select count(*) as count from %s %s", s.table, where), values...)
	}
	if er1 != nil {
		if s.IsRollback && tx != nil {
			s.Proxy.RollbackTransaction(ctx, *tx)
		}
		return false, er1
	} else {
		if count["count"] >= 1 {
			return true, nil
		}
		return false, nil
	}
}
