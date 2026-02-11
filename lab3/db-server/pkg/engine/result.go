package engine

type ColumnarResult struct {
	RowCount uint64 `json:"rowCount"`
	Columns  []any  `json:"columns"`
}
