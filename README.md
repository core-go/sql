# SQL
- SQL Utilities

## Installation
Please make sure to initialize a Go module before installing core-go/sql:

```shell
go get -u github.com/core-go/sql
```

Import:
```go
import "github.com/core-go/sql"
```
## Features
### SQL builder
- Insert, Update, Delete, Find By ID.
- Insert or Update: support Oracle, PostgreSQL, My SQL, MS SQL Server, Sqlite
#### Decimal
- Support decimal, which is useful for currency
#### Dynamic Query
- Build dynamic query for search
### Batch
- Batch Insert
- Batch Batch Update
- Batch Insert or Update: support Oracle, PostgreSQL, My SQL, MS SQL Server, Sqlite
### Repository
- CRUD repository
- Handle transaction
### Query Template (SQL Mapper)
- Dynamic Query