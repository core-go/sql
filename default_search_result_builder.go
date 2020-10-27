package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"

	s "github.com/common-go/search"
)

type DefaultSearchResultBuilder struct {
	Database     *sql.DB
	QueryBuilder QueryBuilder
	Mapper       Mapper
}

func NewSearchResultBuilder(db *sql.DB, queryBuilder QueryBuilder, mapper Mapper) *DefaultSearchResultBuilder {
	builder := &DefaultSearchResultBuilder{db, queryBuilder, mapper}
	return builder
}

func (b *DefaultSearchResultBuilder) BuildSearchResult(ctx context.Context, m interface{}, modelType reflect.Type, tableName string) (*s.SearchResult, error) {
	query, params := b.QueryBuilder.BuildQuery(m, modelType, tableName, "")
	var searchModel *s.SearchModel
	if sModel, ok := m.(*s.SearchModel); ok {
		searchModel = sModel
	} else {
		value := reflect.Indirect(reflect.ValueOf(m))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*s.SearchModel); ok {
				searchModel = sModel1
			}
		}
	}
	return b.buildFromDynamicQuery(ctx, b.Database, tableName, modelType, query, params, searchModel.Page, searchModel.Limit, searchModel.FirstLimit)
}

func (b *DefaultSearchResultBuilder) buildFromDynamicQuery(ctx context.Context, db *sql.DB, tableName string, modelType reflect.Type, query string, params []interface{}, pageIndex int64, pageSize int64, initpageSize int64) (*s.SearchResult, error) {
	var countSelect struct {
		Total int
	}
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	models := reflect.New(modelsType).Interface()
	queryPaging, paramsPaging := b.createPagingSql(query, params, pageIndex, pageSize, initpageSize)
	queryCount, paramsCount := b.buildCountQuery(query, params)
	er1 := Query(b.Database, models, queryPaging, paramsPaging...)
	if er1 != nil {
		return nil, er1
	}
	er2 := Query(b.Database, &countSelect, queryCount, paramsCount...)
	if er2 != nil {
		countSelect.Total = 0
	}
	return b.buildSearchResult(ctx, models, int64(countSelect.Total), pageIndex, pageSize, initpageSize)
}

func (b *DefaultSearchResultBuilder) createPagingSql(sql string, params []interface{}, pageIndex int64, pageSize int64, initPageSize int64) (string, []interface{}) {
	if pageSize > 0 {
		sql = sql + ` LIMIT ? OFFSET ? `
		if initPageSize > 0 {
			if pageIndex == 1 {
				params = append(params, initPageSize, 0)
			} else {
				params = append(params, pageSize, pageSize*(pageIndex-2)+initPageSize)
			}
		} else {
			params = append(params, pageSize, pageSize*(pageIndex-1))
		}
	}
	return sql, params
}

func (b *DefaultSearchResultBuilder) buildCountQuery(sql string, params []interface{}) (string, []interface{}) {
	i := strings.Index(sql, "SELECT ")
	if i < 0 {
		return sql, params
	}
	j := strings.Index(sql, " FROM ")
	if j < 0 {
		return sql, params
	}
	k := strings.Index(sql, " ORDER BY ")
	h := strings.Index(sql, " DISTINCT ")
	if h > 0 {
		sql3 := `SELECT count(*) as total FROM (` + sql[i:] + `) as main`
		return sql3, params
	}
	if k > 0 {
		sql3 := `SELECT count(*) as total ` + sql[j:k]
		return sql3, params
	} else {
		sql3 := `SELECT count(*) as total ` + sql[j:]
		return sql3, params
	}
}

func (b *DefaultSearchResultBuilder) buildSearchResult(ctx context.Context, models interface{}, count int64, pageIndex int64, pageSize int64, initpageSize int64) (*s.SearchResult, error) {
	searchResult := s.SearchResult{}
	searchResult.Total = count

	searchResult.Last = false
	lengthModels := int64(reflect.Indirect(reflect.ValueOf(models)).Len())
	var receivedItems int64

	if initpageSize > 0 {
		if pageIndex == 1 {
			receivedItems = initpageSize
		} else if pageIndex > 1 {
			receivedItems = pageSize*(pageIndex-2) + initpageSize + lengthModels
		}
	} else {
		receivedItems = pageSize*(pageIndex-1) + lengthModels
	}
	searchResult.Last = receivedItems >= count

	if b.Mapper == nil {
		searchResult.Results = models
		return &searchResult, nil
	}
	r2, er3 := b.Mapper.DbToModels(ctx, models)
	if er3 != nil {
		searchResult.Results = models
		return &searchResult, nil
	}
	searchResult.Results = r2
	return &searchResult, er3
}
