package orm

type DynamicQuery struct {
	RawQuery string
	Value    []interface{}
	Fields   []string
}
