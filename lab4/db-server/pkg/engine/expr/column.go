package expr

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

type ColumnRefExpr struct {
	ColName string
	ColType types.ChunkColumnType
}

func (e *ColumnRefExpr) ResultType() types.ChunkColumnType { return e.ColType }

func (e *ColumnRefExpr) GetUsedColumns() []string {
	return []string{e.ColName}
}

func (e *ColumnRefExpr) Evaluate(batch *types.ChunkResult, colMapping map[string]int) (types.ChunkColumn, error) {
	idx, ok := colMapping[e.ColName]
	if !ok {
		return nil, fmt.Errorf("column %s not found in batch", e.ColName)
	}
	if idx >= len(batch.Columns) {
		return nil, fmt.Errorf("internal: Column index out of bounds")
	}
	return batch.Columns[idx], nil
}
