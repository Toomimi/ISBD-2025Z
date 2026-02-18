package types

type ColumnarResult struct {
	RowCount uint64 `json:"rowCount"`
	Columns  []any  `json:"columns"`
}
