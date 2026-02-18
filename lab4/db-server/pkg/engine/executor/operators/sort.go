package operators

import (
	"bytes"
	"fmt"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
	"sort"
)

type SortOperator struct {
	Child      Operator
	SortFields []planner.OrderByColumnReference
	ChunkSize  uint64

	isSorted      bool
	sortedRows    *types.ChunkResult
	currentOffset uint64
}

func (op *SortOperator) Close() {
	if op.Child != nil {
		op.Child.Close()
		op.Child = nil
	}
	op.sortedRows = nil
}

func (op *SortOperator) NextBatch() (*types.ChunkResult, error) {
	if !op.isSorted {
		var allChunks []*types.ChunkResult

		for {
			batch, err := op.Child.NextBatch()
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
			return nil, nil
		}

		mergedBatch, err := mergeChunkResultsWithinOneSchema(allChunks)
		if err != nil {
			return nil, fmt.Errorf("failed to merge chunks for sorting: %w", err)
		}

		totalRows := mergedBatch.RowCount

		perm := make([]int, totalRows)
		for i := range perm {
			perm[i] = i
		}

		sorter := &batchPermutationSorter{
			batch:      mergedBatch,
			perm:       perm,
			sortFields: op.SortFields,
		}
		sort.Sort(sorter)

		reorderedCols, err := reorderRowsInColumns(mergedBatch.Columns, perm)
		if err != nil {
			return nil, err
		}

		op.sortedRows = &types.ChunkResult{
			RowCount:  totalRows,
			Columns:   reorderedCols,
			SelectIdx: mergedBatch.SelectIdx,
			FilterIdx: mergedBatch.FilterIdx,
		}
		op.isSorted = true
	}

	if op.sortedRows == nil || op.currentOffset >= op.sortedRows.RowCount {
		return nil, nil
	}

	end := op.currentOffset + op.ChunkSize
	if end > op.sortedRows.RowCount {
		end = op.sortedRows.RowCount
	}

	count := end - op.currentOffset

	if op.sortedRows == nil {
		return nil, nil
	}
	slicedCols, err := sliceColumns(op.sortedRows.Columns, op.currentOffset, count)
	if err != nil {
		return nil, err
	}

	op.currentOffset = end

	result := &types.ChunkResult{
		RowCount:  count,
		Columns:   slicedCols,
		SelectIdx: op.sortedRows.SelectIdx,
		FilterIdx: op.sortedRows.FilterIdx,
	}

	if op.currentOffset >= op.sortedRows.RowCount {
		op.sortedRows = nil
	}
	return result, nil
}

type batchPermutationSorter struct {
	batch      *types.ChunkResult
	perm       []int
	sortFields []planner.OrderByColumnReference
}

func (s *batchPermutationSorter) Len() int { return len(s.perm) }
func (s *batchPermutationSorter) Swap(i, j int) {
	s.perm[i], s.perm[j] = s.perm[j], s.perm[i]
}

func (s *batchPermutationSorter) Less(i, j int) bool {
	idxI := s.perm[i]
	idxJ := s.perm[j]

	for _, sf := range s.sortFields {
		res := compare(s.batch.Columns[sf.Index], idxI, idxJ)
		if res == 0 {
			continue
		}

		if sf.Ascending {
			return res < 0
		}
		return res > 0
	}
	return false
}

// -1 < 0 < 1
func compare(col types.ChunkColumn, testIdx, baseIdx int) int {
	switch c := col.(type) {
	case *types.Int64ChunkColumn:
		v1, v2 := c.Values[testIdx], c.Values[baseIdx]
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case *types.VarcharChunkColumn:
		s1, e1 := c.Offsets[testIdx], c.NextOffset(testIdx)
		s2, e2 := c.Offsets[baseIdx], c.NextOffset(baseIdx)
		return bytes.Compare(c.Data[s1:e1], c.Data[s2:e2])
	case *types.BooleanChunkColumn:
		v1, v2 := c.Values[testIdx], c.Values[baseIdx]
		if v1 == v2 {
			return 0
		}
		if !v1 {
			return -1
		}
		return 1
	}
	return 0
}

func reorderRowsInColumns(cols []types.ChunkColumn, perm []int) ([]types.ChunkColumn, error) {
	return filterBatchColumns(cols, perm)
}
