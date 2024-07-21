# sql
- It is the library to work with "database/sql" of GO SDK to offer some advantages:
#### Simplified Database Operations
- Simplify common database operations, such as CRUD (Create, Read, Update, Delete) operations, transactions, and batch processing, by providing high-level abstractions and utilities.
#### Compatibility and Flexibility
- It is compatible with various SQL databases such as [Postgres](github.com/lib/pq), [My SQL](github.com/go-sql-driver/mysql), [MS SQL](https://github.com/denisenkom/go-mssqldb), [Oracle](https://github.com/godror/godror), [SQLite](https://github.com/mattn/go-sqlite3), offer flexibility in terms of database driver selection, and query building capabilities.
#### Performance Optimizations
Some use cases that we optimize the performance:
- When insert many rows, we build a single SQL statement and execute once. The syntax of Oracle is different from Postgres, My SQL, MS SQL, SQLite.
  - Source code to build dynamic SQL statement is [here](https://github.com/core-go/sql/blob/main/batch.go), and the sample is at [project-samples/go-import](https://github.com/project-samples/go-import)
- When you want to insert or update, we build a single SQL statement.
  - Cons: The syntax is specified for Oracle, MS SQL, Postgres, My SQL, SQLite. So, source code is long and complicated.
  - Props: Because it interacts with DB once, it has a better performance.
#### Reduced Boilerplate Code
- Reduce boilerplate code associated with database interactions, allowing developers to focus more on application logic rather than low-level database handling
  - In this [layer architecture sample](https://github.com/source-code-template/go-sql-sample), you can see we can reduce a lot of source code at [data access layer](https://github.com/source-code-template/go-sql-sample/blob/main/internal/user/repository/adapter/adapter.go), or you use [generic repository](https://github.com/core-go/sql/blob/main/adapter/adapter.go) to replace all repository source code.

## Some advantage features
#### Decimal
- Support decimal, which is useful for currency
#### Query Template (SQL Mapper)
- My batis for GOLANG.
  - Project sample is at [go-admin](https://github.com/project-samples/go-admin). Mybatis file is here [query.xml](https://github.com/project-samples/go-admin/blob/main/configs/query.xml)

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE mapper PUBLIC "-//mybatis.org//DTD Mapper 3.0//EN" 
        "http://mybatis.org/dtd/mybatis-3-mapper.dtd">

<mapper namespace="mappers">
  <select id="user">
    select *
    from users
    where
    <if test="username != null">
      username like #{username} and
    </if>
    <if test="displayName != null">
      displayName like #{displayName} and
    </if>
    <if test="status != null">
      status in (#{status}) and
    </if>
    <if test="q != null">
      (username like #{q} or displayName like #{q} or email like #{q}) and
    </if>
    1 = 1
    <if test="sort != null">
      order by {sort}
    </if>
    <if test="sort == null">
      order by userId
    </if>
  </select>
</mapper>
```

#### Generic Repository (CRUD repository)
- It is like [CrudRepository](https://docs.spring.io/spring-data/commons/docs/current/api/org/springframework/data/repository/CrudRepository.html) of Spring, which promotes rapid development and consistency across applications.
- While it provides many advantages, such as reducing boilerplate code and ensuring transactional integrity, it also offers flexibility and control over complex queries, because it uses "database/sql" at GO SDK level.
- Especially, it also supports composite primary key.
  - You can look at the sample at [go-sql-composite-key](https://github.com/go-tutorials/go-sql-composite-key).
  - In this sample, the company_users has 2 primary keys: company_id and user_id
  - You can define a GO struct, which contains 2 fields: CompanyId and UserId
    ```go
    package model
    
    type UserId struct {
      CompanyId string `json:"companyId" gorm:"column:company_id;primary_key"`
      UserId    string `json:"userId" gorm:"column:user_id;primary_key"`
    }
    ```
#### Search Repository
The flow for search/paging:
- Build the dynamic query
- Build the paging query from dynamic query (it specified for Oracle, Postgres, My SQL, MS SQL, SQLite)
  - Query data and map to array of struct 
- Build the count query
  - Count the total records for paging
#### Dynamic query builder
- Look at this sample [user](https://github.com/source-code-template/go-sql-sample/blob/main/internal/user/user.go), you can see it automatically build a dynamic query for serach.
<table><thead><tr><td>

```go
func BuildFilter(
    filter *model.UserFilter) (string, []interface{}) {
  buildParam := s.BuildDollarParam
  var where []string
  var params []interface{}
  i := 1
  if len(filter.Id) > 0 {
    params = append(params, filter.Id)
    where = append(where,
      fmt.Sprintf(`id = %s`, buildParam(i)))
    i++
  }
  if filter.DateOfBirth != nil {
    if filter.DateOfBirth.Min != nil {
      params = append(params, filter.DateOfBirth.Min)
      where = append(where, 
        fmt.Sprintf(`date_of_birth >= %s`, buildParam(i)))
      i++
    }
    if filter.DateOfBirth.Max != nil {
      params = append(params, filter.DateOfBirth.Max)
      where = append(where,
        fmt.Sprintf(`date_of_birth <= %s`, buildParam(i)))
      i++
    }
  }
  if len(filter.Username) > 0 {
    q := filter.Username + "%"
    params = append(params, q)
    where = append(where,
      fmt.Sprintf(`username like %s`, buildParam(i)))
    i++
  }
  if len(filter.Email) > 0 {
    q := filter.Email + "%"
    params = append(params, q)
    where = append(where,
      fmt.Sprintf(`email like %s`, buildParam(i)))
    i++
  }
  if len(filter.Phone) > 0 {
    q := "%" + filter.Phone + "%"
    params = append(params, q)
    where = append(where,
      fmt.Sprintf(`phone like %s`, buildParam(i)))
    i++
  }
  if len(where) > 0 {
    return strings.Join(where, " and "), params
  }
  return "", params
}
```

</td>
<td>

```go
buildQuery := query.UseQuery[
    model.User,
    *model.UserFilter](db, "users")
query, args := buildQuery(filter)
```

</td></tr></tbody></table>

#### Utilities to simplified Database Operations
##### Simplify data querying: support to map *sql.Rows to array of struct
- For example in this file [adapter.go](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/adapter.go), you can see it reduces a lot of source code.

<table><thead><tr><td>

[GO SDK Only](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/user_adapter.go)
</td><td>

[GO SDK with utilities](https://github.com/source-code-template/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/user_adapter.go)
</td></tr></thead><tbody><tr><td>

```go
func (r *UserAdapter) Load(
    ctx context.Context,
    id string) (*User, error) {
  query := `
    select
      id, 
      username,
      email,
      phone,
      date_of_birth
    from users where id = ?`
  rows, err := r.DB.QueryContext(ctx, query, id)
  if err != nil {
    return nil, err
  }
  defer rows.Close()
  for rows.Next() {
    var user User
    err = rows.Scan(
      &user.Id,
      &user.Username,
      &user.Phone,
      &user.Email,
      &user.DateOfBirth)
    return &user, nil
  }
  return nil, nil
}
```
</td>
<td>

```go
import q "github.com/core-go/sql"

func (r *UserAdapter) Load(
    ctx context.Context,
    id string) (*User, error) {
  var users []User
  query := fmt.Sprintf(`
    select
      id,
      username,
      email,
      phone,
      date_of_birth
    from users where id = %s limit 1`,
    q.BuildParam(1))
  err := q.Select(ctx, r.DB, &users, query, id)
  if err != nil {
    return nil, err
  }
  if len(users) > 0 {
    return &users[0], nil
  }
  return nil, nil
}
```
</td></tr></tbody></table>

##### Utility functions to build sql statement to insert, update, insert or update
- For example in this file [adapter.go](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/adapter.go), you can see it reduces a lot of source code.
<table><thead><tr><td>

[GO SDK Only](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/user_adapter.go)
</td><td>

[GO SDK with utilities](https://github.com/source-code-template/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/user_adapter.go)
</td></tr></thead><tbody><tr><td>

```go
func (r *UserAdapter) Create(
    ctx context.Context,
    user *User) (int64, error) {
  query := `
    insert into users (
      id,
      username,
      email,
      phone,
      date_of_birth)
    values (
      ?,
      ?,
      ?, 
      ?,
      ?)`
  tx := GetTx(ctx)
  stmt, err := tx.Prepare(query)
  if err != nil {
    return -1, err
  }
  res, err := stmt.ExecContext(ctx,
    user.Id,
    user.Username,
    user.Email,
    user.Phone,
    user.DateOfBirth)
  if err != nil {
    return -1, err
  }
  return res.RowsAffected()
}
```
</td>
<td>

```go
import q "github.com/core-go/sql"

func (r *UserAdapter) Create(
    ctx context.Context,
    user *User) (int64, error) {
  query, args := q.BuildToInsert("users", user, q.BuildParam)
  tx := q.GetTx(ctx)
  res, err := tx.ExecContext(ctx, query, args...)
  return q.RowsAffected(res, err)
}
```
</td></tr></tbody></table>

##### Simplify transaction handling
- For example, in [this service layer](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/service/service.go) and [this data access layer](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/adapter/repository/adapter.go), it simplifies transaction handling, at GO SDK level.

<table><thead><tr><td>

[GO SDK Only](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/service/user_service.go)
</td><td>

[GO SDK with utilities](https://github.com/source-code-template/go-sql-hexagonal-architecture-sample/blob/main/internal/user/service/user_service.go)
</td></tr></thead><tbody><tr><td>

```go
func (s *userService) Create(
    ctx context.Context,
    user *User) (int64, error) {
  tx, err := s.db.Begin()
  if err != nil {
    return -1, nil
  }
  ctx = context.WithValue(ctx, "tx", tx)
  res, err := s.repository.Create(ctx, user)
  if err != nil {
    er := tx.Rollback()
    if er != nil {
      return -1, er
    }
    return -1, err
  }
  err = tx.Commit()
  return res, err
}
```
</td>
<td>

```go
func (s *userService) Create(
    ctx context.Context,
	user *User) (int64, error) {
  ctx, tx, err := q.Begin(ctx, s.db)
  if err != nil {
    return  -1, err
  }
  res, err := s.repository.Create(ctx, user)
  return q.End(tx, res, err)
}
```
</td></tr></tbody></table>

- In another example, in [this service layer](https://github.com/source-code-template/go-sql-sample/blob/main/internal/user/service/usecase.go) and [this data access layer](https://github.com/source-code-template/go-sql-sample/blob/main/internal/user/repository/adapter/adapter.go), it simplifies transaction handling, at GO SDK level.
<table><thead><tr><td>

[GO SDK Only](https://github.com/go-tutorials/go-sql-hexagonal-architecture-sample/blob/main/internal/user/service/user_service.go)
</td><td>

[GO SDK with utilities](https://github.com/source-code-template/go-sql-hexagonal-architecture-sample/blob/main/internal/user/service/user_service.go)
</td></tr></thead><tbody><tr><td>

```go
func (s *userService) Create(
    ctx context.Context,
    user *User) (int64, error) {
  tx, err := s.db.Begin()
  if err != nil {
    return -1, nil
  }
  ctx = context.WithValue(ctx, "tx", tx)
  res, err := s.repository.Create(ctx, user)
  if err != nil {
    er := tx.Rollback()
    if er != nil {
      return -1, er
    }
    return -1, err
  }
  err = tx.Commit()
  return res, err
}
```
</td>
<td>

```go
func (s *UserUseCase) Create(
    ctx context.Context, user *model.User) (int64, error) {
  return tx.Execute(ctx, s.db, func(ctx context.Context) (int64, error) {
    return s.repository.Create(ctx, user)
  })
}
```
</td></tr></tbody></table>

#### For batch job
- Inserter
- Updater
- Writer
- StreamInserter
- StreamUpdater
- StreamWriter
- BatchInserter
- BatchUpdater
- BatchWriter
#### Health Check
#### Passcode Adapter
#### Action Log
- Save Action Log with dynamic database design
#### Field Loader
## Installation
Please make sure to initialize a Go module before installing core-go/sql:

```shell
go get -u github.com/core-go/sql
```

Import:
```go
import "github.com/core-go/sql"
```
