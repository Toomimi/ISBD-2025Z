package operators

import (
	"isbd4/pkg/engine/types"
)

type LimitOperator struct {
	Child Operator
	Limit uint64

	count uint64
}

func NewLimitOperator(child Operator, limit uint64) *LimitOperator {
	return &LimitOperator{
		Child: child,
		Limit: limit,
		count: 0,
	}
}

func (op *LimitOperator) Close() {
	if op.Child != nil {
		op.Child.Close()
	}
}

func (op *LimitOperator) NextBatch() (*types.ChunkResult, error) {
	if op.count >= op.Limit {
		op.Close()
		return nil, nil
	}

	batch, err := op.Child.NextBatch()
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}

	remaining := op.Limit - op.count
	if batch.RowCount <= remaining {
		op.count += batch.RowCount
		return batch, nil
	}

	slicedCols, err := sliceColumns(batch.Columns, 0, remaining)
	if err != nil {
		return nil, err
	}

	op.count += remaining

	return &types.ChunkResult{
		RowCount:  remaining,
		Columns:   slicedCols,
		SelectIdx: batch.SelectIdx,
		FilterIdx: batch.FilterIdx,
	}, nil
}
