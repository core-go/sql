package orm

import (
	"context"
	"reflect"
	"strings"

	s "github.com/common-go/search"
	"gorm.io/gorm"
)

type DefaultSearchResultDynamicBuilder struct {
	Database     *gorm.DB
	QueryBuilder DynamicQueryBuilder
	SortBuilder  SortBuilder
	Mapper       Mapper
}

func NewSearchResultDynamicBuilder(db *gorm.DB, queryBuilder DynamicQueryBuilder, sortBuilder SortBuilder, mapper Mapper) *DefaultSearchResultDynamicBuilder {
	builder := &DefaultSearchResultDynamicBuilder{db, queryBuilder, sortBuilder, mapper}
	return builder
}

func (b *DefaultSearchResultDynamicBuilder) BuildSearchResult(ctx context.Context, m interface{}, modelType reflect.Type, tableName string) (*s.SearchResult, error) {
	query := b.QueryBuilder.BuildQuery(m, modelType)

	var sort = ""
	var searchModel *s.SearchModel

	if sModel, ok := m.(*s.SearchModel); ok {
		searchModel = sModel
		sort = b.SortBuilder.BuildSort(*sModel, modelType)
	} else {
		value := reflect.Indirect(reflect.ValueOf(m))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*s.SearchModel); ok {
				searchModel = sModel1
				sort = b.SortBuilder.BuildSort(*sModel1, modelType)
			}
		}
	}
	return b.buildFromDynamicQuery(ctx, b.Database, tableName, modelType, query, sort, searchModel.Page, searchModel.Limit)
}

func (b *DefaultSearchResultDynamicBuilder) buildFromDynamicQuery(ctx context.Context, db *gorm.DB, tableName string, modelType reflect.Type, query DynamicQuery, sort string, pageIndex int64, pageSize int64) (*s.SearchResult, error) {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	models := reflect.New(modelsType).Interface()
	results, count := b.findAndCount(db, tableName, models, query, sort, pageIndex, pageSize)
	return b.buildSearchResult(ctx, results, count, pageIndex, pageSize)
}

func (b *DefaultSearchResultDynamicBuilder) findAndCount(db *gorm.DB, tableName string, models interface{}, query DynamicQuery, sort string, pageIndex int64, pageSize int64) (interface{}, int64) {
	var count int64
	var countSelect struct {
		count int64
	}
	countWithFind := false
	x := pageSize * (pageIndex - 1)
	offset := int(x)
	x3 := int(pageSize)
	queryDb := db.Table(tableName).Offset(offset).Order(sort).Limit(x3)
	countDb := db.Table(tableName)
	fields := make([]string, 0)

	queryDb, countDb, fields = b.handleQuery(queryDb, countDb, query)

	tagsSqlBuilder := GetSqlBuilderTags(reflect.Indirect(reflect.ValueOf(models)).Type().Elem())
	if len(tagsSqlBuilder) > 0 {
		queryDb, countDb, countWithFind = b.handleSqlBuilderTags(queryDb, countDb, tagsSqlBuilder)
	} else {
		if len(fields) > 0 {
			queryDb = queryDb.Select(strings.Join(fields, ","))
		}
	}

	queryDb = queryDb.Find(models)
	if countWithFind {
		countDb = countDb.Find(&countSelect)
		count = countSelect.count
	} else {
		countDb = countDb.Count(&count)
	}

	return models, count
}

func (b *DefaultSearchResultDynamicBuilder) buildSearchResult(ctx context.Context, models interface{}, count int64, pageIndex int64, pageSize int64) (*s.SearchResult, error) {
	searchResult := s.SearchResult{}
	searchResult.Total = count

	searchResult.Last = false
	lengthModels := int64(reflect.Indirect(reflect.ValueOf(models)).Len())
	if pageSize*pageIndex+lengthModels >= count {
		searchResult.Last = true
	}
	if b.Mapper == nil {
		searchResult.Results = models
		return &searchResult, nil
	}
	r2, er3 := b.Mapper.DbToModels(ctx, models)
	if er3 != nil {
		searchResult.Results = models
		return &searchResult, er3
	}
	searchResult.Results = r2
	return &searchResult, er3
}

func (b *DefaultSearchResultDynamicBuilder) handleQuery(queryDb *gorm.DB, countDb *gorm.DB, query DynamicQuery) (*gorm.DB, *gorm.DB, []string) {
	if query.RawQuery != "" {
		return queryDb.Where(query.RawQuery, query.Value...), countDb.Where(query.RawQuery, query.Value...), query.Fields
	} else {
		return queryDb, countDb, query.Fields
	}
	/*
		if dq, ok := query.(DynamicQuery); ok {
			if dq.RawQuery != "" {
				return queryDb.Where(dq.RawQuery, dq.Value...), countDb.Where(dq.RawQuery, dq.Value...), dq.Fields
			} else {
				return queryDb, countDb, dq.Fields
			}
		} else {
			return queryDb.Where(query), countDb.Where(query), dq.Fields
		}
	*/
}

func (b *DefaultSearchResultDynamicBuilder) handleSqlBuilderTags(queryDb *gorm.DB, countDb *gorm.DB, sqlBuilderQueries []QueryType) (*gorm.DB, *gorm.DB, bool) {
	selects := make([]string, 0)
	selectsCount := make([]string, 0)
	countWithFind := false
	for _, q := range sqlBuilderQueries {
		if q.Select != "" {
			selects = append(selects, q.Select)
		}
		if q.SelectCount != "" {
			selectsCount = append(selectsCount, q.SelectCount)
		}
		if q.Join != "" {
			queryDb = queryDb.Joins(q.Join)
			countDb = countDb.Joins(q.Join)
		}
	}
	if len(selects) > 0 {
		queryDb = queryDb.Select(strings.Join(selects, ","))
	}
	if len(selectsCount) > 0 {
		countDb = countDb.Select(strings.Join(selectsCount, ","))
		countWithFind = true
	}
	return queryDb, countDb, countWithFind
}
