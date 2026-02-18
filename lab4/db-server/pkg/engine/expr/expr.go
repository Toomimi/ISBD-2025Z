package expr

import (
	"isbd4/pkg/engine/types"
)

type Expression interface {
	Evaluate(batch *types.ChunkResult, colMapping map[string]int) (types.ChunkColumn, error)
	ResultType() types.ChunkColumnType
	GetUsedColumns() []string
}

func GetUsedColumnsFromExpressions(exprs []Expression) []string {
	uniqueCols := make(map[string]struct{})
	for _, e := range exprs {
		cols := e.GetUsedColumns()
		for _, col := range cols {
			uniqueCols[col] = struct{}{}
		}
	}
	res := make([]string, 0, len(uniqueCols))
	for col := range uniqueCols {
		res = append(res, col)
	}
	return res
}
