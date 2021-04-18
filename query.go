package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	DefaultPagingFormat = " limit %s offset %s "
	OraclePagingFormat  = " offset %s rows fetch next %s rows only "
	desc                = "desc"
	asc                 = "asc"
)

func BuildFromQuery(ctx context.Context, db *sql.DB, models interface{}, query string, params []interface{}, pageIndex int64, pageSize int64, initPageSize int64, options...func(context.Context, interface{}) (interface{}, error)) (int64, error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	var total int64
	driver := getDriver(db)
	if pageSize <= 0 {
		er1 := Query(ctx, db, models, query, params...)
		if er1 != nil {
			return -1, er1
		}
		objectValues := reflect.Indirect(reflect.ValueOf(models))
		if objectValues.Kind() == reflect.Slice {
			i := objectValues.Len()
			total = int64(i)
		}
		er2 := BuildSearchResult(ctx, models, mp)
		return total, er2
	} else {
		if driver == DriverOracle {
			queryPaging := BuildPagingQueryByDriver(query, pageIndex, pageSize, initPageSize, driver)
			er1 := QueryAndCount(ctx, db, models, &total, queryPaging, params...)
			if er1 != nil {
				return -1, er1
			}
			er2 := BuildSearchResult(ctx, models, mp)
			return total, er2
		} else {
			queryPaging := BuildPagingQuery(query, pageIndex, pageSize, initPageSize, driver)
			queryCount, paramsCount := BuildCountQuery(query, params)
			er1 := Query(ctx, db, models, queryPaging, params...)
			if er1 != nil {
				return -1, er1
			}
			total, er2 := Count(ctx, db, queryCount, paramsCount...)
			if er2 != nil {
				total = 0
			}
			er3 := BuildSearchResult(ctx, models, mp)
			return total, er3
		}
	}
}
func BuildPagingQueryByDriver(sql string, pageIndex int64, pageSize int64, initPageSize int64, driver string) string {
	s2 := BuildPagingQuery(sql, pageIndex, pageSize, initPageSize, driver)
	if driver != DriverOracle {
		return s2
	} else {
		l := len(" distinct ")
		i := strings.Index(sql, " distinct ")
		if i < 0 {
			i = strings.Index(sql, " DISTINCT ")
		}
		if i < 0 {
			l = len("select") + 1
			i = strings.Index(s2, "select")
		}
		if i < 0 {
			i = strings.Index(s2, "SELECT")
		}
		if i >= 0 {
			return s2[0:l] + " count(*) over() as total, " + s2[l:]
		}
		return s2
	}
}
func BuildPagingQuery(sql string, pageIndex int64, pageSize int64, initPageSize int64, driver string) string {
	if pageSize > 0 {
		var limit, offset int64
		if initPageSize > 0 {
			if pageIndex == 1 {
				limit = initPageSize
				offset = 0
			} else {
				limit = pageSize
				offset = pageSize*(pageIndex-2) + initPageSize
			}
		} else {
			limit = pageSize
			offset = pageSize * (pageIndex - 1)
		}

		var pagingQuery string
		if driver == DriverOracle {
			pagingQuery = fmt.Sprintf(OraclePagingFormat, strconv.Itoa(int(offset)), strconv.Itoa(int(limit)))
		} else {
			pagingQuery = fmt.Sprintf(DefaultPagingFormat, strconv.Itoa(int(limit)), strconv.Itoa(int(offset)))
		}
		sql += pagingQuery
	}

	return sql
}

func BuildCountQuery(sql string, params []interface{}) (string, []interface{}) {
	i := strings.Index(sql, "select ")
	if i < 0 {
		return sql, params
	}
	j := strings.Index(sql, " from ")
	if j < 0 {
		return sql, params
	}
	k := strings.Index(sql, " order by ")
	h := strings.Index(sql, " distinct ")
	if h > 0 {
		sql3 := `select count(*) as total from (` + sql[i:] + `) as main`
		return sql3, params
	}
	if k > 0 {
		sql3 := `select count(*) as total ` + sql[j:k]
		return sql3, params
	} else {
		sql3 := `select count(*) as total ` + sql[j:]
		return sql3, params
	}
}

func BuildSearchResult(ctx context.Context, models interface{}, mp func(context.Context, interface{}) (interface{}, error)) error {
	if mp == nil {
		return nil
	}
	_, err := MapModels(ctx, models, mp)
	return err
}

func getDriver(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*godror.drv":
		return DriverOracle
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*sqlite3.SQLiteDriver":
		return DriverSqlite3
	default:
		return DriverNotSupport
	}
}
func buildParam(i int) string {
	return "?"
}
func buildOracleParam(i int) string {
	return ":val" + strconv.Itoa(i)
}
func buildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func buildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func getBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return buildDollarParam
	case "*godror.drv":
		return buildOracleParam
	case "*mssql.Driver":
		return buildMsSqlParam
	default:
		return buildParam
	}
}

func BuildSort(sortString string, modelType reflect.Type) string {
	var sort = make([]string, 0)
	sorts := strings.Split(sortString, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField
		c := sortField[0:1]
		if c == "-" || c == "+" {
			fieldName = sortField[1:]
		}
		columnName := GetColumnNameForSearch(modelType, fieldName)
		sortType := GetSortType(c)
		sort = append(sort, columnName+" "+sortType)
	}
	return ` order by ` + strings.Join(sort, ",")
}
