package sort

import (
	"bytes"
	"isbd4/pkg/engine/executor/operators"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
	"sort"
)

type batchSorter struct {
	batch      *types.ChunkResult
	perm       []int
	sortFields []planner.OrderByColumnReference
}

func newBatchSorter(batch *types.ChunkResult, sortFields []planner.OrderByColumnReference) *batchSorter {
	perm := make([]int, batch.RowCount)
	for i := range perm {
		perm[i] = i
	}
	return &batchSorter{
		batch:      batch,
		perm:       perm,
		sortFields: sortFields,
	}
}

func (s *batchSorter) Len() int { return len(s.perm) }

func (s *batchSorter) Swap(i, j int) {
	s.perm[i], s.perm[j] = s.perm[j], s.perm[i]
}

func (s *batchSorter) Less(i, j int) bool {
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

func (s *batchSorter) sort() (*types.ChunkResult, error) {
	sort.Sort(s)
	reorderedCols, err := reorderRowsInColumns(s.batch.Columns, s.perm)
	if err != nil {
		return nil, err
	}
	return &types.ChunkResult{
		RowCount:  s.batch.RowCount,
		Columns:   reorderedCols,
		SelectIdx: s.batch.SelectIdx,
		FilterIdx: s.batch.FilterIdx,
	}, nil
}

func reorderRowsInColumns(cols []types.ChunkColumn, perm []int) ([]types.ChunkColumn, error) {
	return operators.FilterBatchColumns(cols, perm)
}
