package operators

import "isbd4/pkg/engine/types"

type DummyReaderOperator struct {
	returned bool
}

func (op *DummyReaderOperator) NextBatch() (*types.ChunkResult, error) {
	if op.returned {
		return nil, nil
	}
	op.returned = true

	return &types.ChunkResult{
		RowCount:  1,
		Columns:   nil,
		SelectIdx: nil,
		FilterIdx: -1,
	}, nil
}

func (op *DummyReaderOperator) Close() {}
