package sql

type DynamicQuery struct {
	RawQuery string
	Value    []interface{}
	Fields   []string
}
