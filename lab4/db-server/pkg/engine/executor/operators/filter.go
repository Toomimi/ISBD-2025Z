package operators

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

type FilterOperator struct {
	Child Operator
}

func (op *FilterOperator) Close() {
	if op.Child != nil {
		op.Child.Close()
		op.Child = nil
	}
}

func (op *FilterOperator) NextBatch() (*types.ChunkResult, error) {
	for {
		batch, err := op.Child.NextBatch()
		if err != nil {
			return nil, err
		}
		if batch == nil {
			return nil, nil
		}

		if batch.FilterIdx < 0 || batch.FilterIdx >= len(batch.Columns) {
			return nil, fmt.Errorf("filter predicate index (FilterIdx) out of bounds: %d, cols: %d", batch.FilterIdx, len(batch.Columns))
		}

		predCol := batch.Columns[batch.FilterIdx].(*types.BooleanChunkColumn)

		rowsToPass := 0
		passIndices := make([]int, 0, batch.RowCount)
		for i, v := range predCol.Values {
			if v {
				rowsToPass++
				passIndices = append(passIndices, i)
			}
		}

		if rowsToPass == 0 {
			continue
		}

		if rowsToPass == int(batch.RowCount) {
			return batch, nil
		}

		newCols, err := filterBatchColumns(batch.Columns, passIndices)
		if err != nil {
			return nil, err
		}

		return &types.ChunkResult{
			RowCount: uint64(len(passIndices)),
			Columns:  newCols,
		}, nil
	}
}
