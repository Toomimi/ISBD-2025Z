package operators

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

func FilterBatchColumns(columns []types.ChunkColumn, indices []int) ([]types.ChunkColumn, error) {
	result := make([]types.ChunkColumn, len(columns))
	for i, col := range columns {
		switch c := col.(type) {
		case *types.Int64ChunkColumn:
			newVals := keepOnlyIndices(c.Values, indices)
			result[i] = types.NewInt64Column(c.Name, newVals)
		case *types.BooleanChunkColumn:
			newVals := keepOnlyIndices(c.Values, indices)
			result[i] = types.NewBooleanColumn(c.Name, newVals)
		case *types.VarcharChunkColumn:
			result[i] = filterBatchVarcharColumn(c, indices)
		default:
			return nil, fmt.Errorf("unsupported column type in filterBatchColumns: %T", col)
		}
	}
	return result, nil
}

func keepOnlyIndices[T any](slice []T, indices []int) []T {
	newVals := make([]T, len(indices))
	for j, idx := range indices {
		newVals[j] = slice[idx]
	}
	return newVals
}

func filterBatchVarcharColumn(col *types.VarcharChunkColumn, indices []int) *types.VarcharChunkColumn {
	totalDataSize := 0
	for _, idx := range indices {
		totalDataSize += int(col.NextOffset(idx) - col.Offsets[idx])
	}

	newData := make([]byte, 0, totalDataSize)
	newOffsets := make([]uint64, len(indices))

	for j, idx := range indices {
		start := col.Offsets[idx]
		end := col.NextOffset(idx)

		chunk := col.Data[start:end]

		newOffsets[j] = uint64(len(newData))
		newData = append(newData, chunk...)
	}

	return &types.VarcharChunkColumn{Name: col.Name, Offsets: newOffsets, Data: newData}
}

func SliceColumns(cols []types.ChunkColumn, start, count uint64) ([]types.ChunkColumn, error) {
	newCols := make([]types.ChunkColumn, len(cols))

	if count == 0 {
		return nil, fmt.Errorf("Internal: sliceColumns shouldn't be called for empty batch")
	}

	for i, col := range cols {
		switch c := col.(type) {
		case *types.Int64ChunkColumn:
			newValues := make([]int64, count)
			copy(newValues, c.Values[start:start+count])
			newCols[i] = types.NewInt64Column(c.Name, newValues)

		case *types.BooleanChunkColumn:
			newValues := make([]bool, count)
			copy(newValues, c.Values[start:start+count])
			newCols[i] = types.NewBooleanColumn(c.Name, newValues)

		case *types.VarcharChunkColumn:
			firstIdx := int(start)
			lastIdx := int(start + count)

			startByte := c.Offsets[firstIdx]
			endByte := c.NextOffset(lastIdx - 1)

			newData := make([]byte, endByte-startByte)
			copy(newData, c.Data[startByte:endByte])

			newOffsets := make([]uint64, count)
			for j := 0; j < int(count); j++ {
				newOffsets[j] = c.Offsets[firstIdx+j] - startByte
			}

			newCols[i] = &types.VarcharChunkColumn{
				Name:    c.Name,
				Offsets: newOffsets,
				Data:    newData,
			}

		default:
			return nil, fmt.Errorf("sliceColumns: unknown column type %T", col)
		}
	}
	return newCols, nil
}

func MergeChunkResultsWithinOneSchema(chunks []*types.ChunkResult) (*types.ChunkResult, error) {
	if len(chunks) == 0 {
		return nil, nil
	}

	totalRows := 0
	numCols := len(chunks[0].Columns)

	totalDataSizes := make([]int, numCols)

	for _, chunk := range chunks {
		totalRows += int(chunk.RowCount)
		for i, col := range chunk.Columns {
			if vCol, ok := col.(*types.VarcharChunkColumn); ok {
				totalDataSizes[i] += len(vCol.Data)
			}
		}
	}

	mergedCols := make([]types.ChunkColumn, numCols)
	for i := 0; i < numCols; i++ {
		c := types.CloneEmpty(chunks[0].Columns[i], totalRows, totalDataSizes[i])
		switch typedCol := c.(type) {
		case *types.Int64ChunkColumn:
			typedCol.Values = typedCol.Values[:totalRows]
		case *types.BooleanChunkColumn:
			typedCol.Values = typedCol.Values[:totalRows]
		case *types.VarcharChunkColumn:
			typedCol.Offsets = typedCol.Offsets[:totalRows]
		}
		mergedCols[i] = c
	}

	currentRow := 0
	for _, chunk := range chunks {
		for i := 0; i < numCols; i++ {
			chunk.Columns[i].CopyTo(mergedCols[i], currentRow)
		}
		currentRow += int(chunk.RowCount)
	}

	return &types.ChunkResult{
		RowCount:  uint64(totalRows),
		Columns:   mergedCols,
		SelectIdx: chunks[0].SelectIdx,
		FilterIdx: chunks[0].FilterIdx,
	}, nil
}
