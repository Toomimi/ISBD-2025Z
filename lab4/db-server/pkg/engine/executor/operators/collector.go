package operators

import (
	"fmt"

	"isbd4/pkg/engine/types"
)

// Todo: implement storing results to disk
func CollectAllBatches(op Operator) (*types.ColumnarResult, error) {
	var allChunks []*types.ChunkResult

	for {
		batch, err := op.NextBatch()
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		if batch.RowCount == 0 {
			continue
		}
		allChunks = append(allChunks, batch)
	}

	if len(allChunks) == 0 {
		return &types.ColumnarResult{RowCount: 0, Columns: []any{}}, nil
	}
	finalChunk, err := mergeChunkResultsWithinOneSchema(allChunks)
	if err != nil {
		return nil, fmt.Errorf("failed to merge all batches: %w", err)
	}

	return finalChunk.ToColumnarResult(), nil
}
