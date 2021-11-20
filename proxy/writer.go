package proxy

import (
	"context"
	"database/sql"
	q "github.com/core-go/sql"
	"reflect"
	"strconv"
	"strings"
)

func Begin(ctx context.Context, db *sql.DB, opts ...*sql.TxOptions) (context.Context, *sql.Tx, error) {
	var tx *sql.Tx
	var err error
	if len(opts) > 0 && opts[0] != nil {
		tx, err = db.BeginTx(ctx, opts[0])
	} else {
		tx, err = db.Begin()
	}
	if err != nil {
		return ctx, tx, err
	} else {
		c2 := context.WithValue(ctx, txs, tx)
		return c2, tx, nil
	}
}
func Commit(tx *sql.Tx, err error, options ...bool) error {
	if err != nil {
		if len(options) > 0 && options[0] {
			tx.Rollback()
		}
		return err
	}
	return tx.Commit()
}
func Rollback(tx *sql.Tx, err error, options ...int64) (int64, error) {
	tx.Rollback()
	if len(options) > 0 {
		return options[0], err
	}
	return -1, err
}
func End(tx *sql.Tx, res int64, err error, options ...bool) (int64, error) {
	er := Commit(tx, err, options...)
	return res, er
}

type Writer struct {
	*Loader
	jsonColumnMap  map[string]string
	Mapper         q.Mapper
	versionField   string
	versionIndex   int
	versionDBField string
	schema         *q.Schema
	BoolSupport    bool
	Rollback       bool
	Driver         string
}

func NewWriter(proxy Proxy, tableName string, modelType reflect.Type, buildParam func(i int) string, options ...q.Mapper) (*Writer, error) {
	return NewWriterWithVersion(proxy, tableName, modelType, buildParam, "", options...)
}
func NewWriterWithVersion(proxy Proxy, tableName string, modelType reflect.Type, buildParam func(i int) string, versionField string, options ...q.Mapper) (*Writer, error) {
	var mapper q.Mapper
	if len(options) > 0 {
		mapper = options[0]
	}
	var loader *Loader
	var err error
	if mapper != nil {
		loader, err = NewLoader(proxy, tableName, modelType, buildParam, mapper.DbToModel)
	} else {
		loader, err = NewLoader(proxy, tableName, modelType, buildParam, nil)
	}
	if err != nil {
		return nil, err
	}
	schema := q.CreateSchema(modelType)
	jsonColumnMap := q.MakeJsonColumnMap(modelType)
	if len(versionField) > 0 {
		index := q.FindFieldIndex(modelType, versionField)
		if index >= 0 {
			_, dbFieldName, exist := q.GetFieldByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &Writer{Loader: loader, Rollback: true, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}, nil
		}
	}
	return &Writer{Loader: loader, Rollback: true, schema: schema, Mapper: mapper, jsonColumnMap: jsonColumnMap, versionField: versionField, versionIndex: -1}, nil
}
func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	tx := q.GetTxId(ctx)
	queryInsert, values := q.BuildToInsertWithVersion(s.table, m, s.versionIndex, s.BuildParam, s.BoolSupport, nil, s.schema)
	if tx == nil {
		return s.Proxy.Exec(ctx, queryInsert, values...)
	} else {
		result, err := s.Proxy.ExecTx(ctx, *tx, false, queryInsert, values...)
		if err != nil {
			if s.Rollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return -1, err
		}
		return result, err
	}
}
func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	tx := q.GetTxId(ctx)
	query, values := q.BuildToUpdateWithVersion(s.table, m, s.versionIndex, s.BuildParam, s.BoolSupport, nil, s.schema)
	if tx == nil {
		return s.Proxy.Exec(ctx, query, values...)
	} else {
		result, err := s.Proxy.ExecTx(ctx, *tx, false, query, values...)
		if err != nil {
			if s.Rollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return -1, err
		}
		return result, err
	}
}
func (s *Writer) Save(ctx context.Context, model interface{}) (int64, error) {
	var m interface{}
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		m = m2
	} else {
		m = model
	}
	tx := q.GetTxId(ctx)
	query, values, er0 := q.BuildToSave(s.table, m, s.Driver, s.schema)
	if er0 != nil {
		return -1, er0
	}
	if tx == nil {
		return s.Proxy.Exec(ctx, query, values...)
	} else {
		result, err := s.Proxy.ExecTx(ctx, *tx, false, query, values...)
		if err != nil {
			if s.Rollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return -1, err
		}
		return result, err
	}
}
func (s *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
	}
	MapToDB(&model, s.modelType)
	dbColumnMap := q.JSONToColumns(model, s.jsonColumnMap)
	query, values := q.BuildToPatchWithVersion(s.table, dbColumnMap, s.schema.SKeys, s.BuildParam, nil, s.versionDBField, s.schema.Fields)
	tx := q.GetTxId(ctx)
	if tx == nil {
		return s.Proxy.Exec(ctx, query, values...)
	} else {
		result, err := s.Proxy.ExecTx(ctx, *tx, false, query, values...)
		if err != nil {
			if s.Rollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return -1, err
		}
		return result, err
	}
}
func MapToDB(model *map[string]interface{}, modelType reflect.Type) {
	for colName, value := range *model {
		if boolValue, boolOk := value.(bool); boolOk {
			index := q.GetIndexByTag("json", colName, modelType)
			if index > -1 {
				valueS := modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				valueInt, err := strconv.Atoi(valueS)
				if err != nil {
					(*model)[colName] = valueS
				} else {
					(*model)[colName] = valueInt
				}
				continue
			}
		}
		(*model)[colName] = value
	}
}
func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	tx := q.GetTxId(ctx)
	// l := len(s.keys)
	query := q.BuildQueryById(id, s.modelType, s.keys[0])
	sql, values := q.BuildToDelete(s.table, query, s.BuildParam)
	if tx == nil {
		return s.Proxy.Exec(ctx, sql, values...)
	} else {
		result, err := s.Proxy.ExecTx(ctx, *tx, false, sql, values...)
		if err != nil {
			if s.Rollback {
				s.Proxy.RollbackTransaction(ctx, *tx)
			}
			return -1, err
		}
		return result, err
	}
}
