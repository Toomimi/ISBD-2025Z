package sort

import (
	"container/heap"
	"fmt"
	"isbd4/pkg/engine/executor/operators"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
)

type ExternalMergeSortOperator struct {
	Child            operators.Operator
	SortFields       []planner.OrderByColumnReference
	ChunkSize        uint64
	MemoryLimitBytes uint64

	receivedAllData bool

	runFilesManager *RunFilesManager
	mergeHeap       *MergeHeap
	savedSelectIdx  []int

	sortedRows    *types.ChunkResult
	currentOffset uint64
}

func NewExternalMergeSortOperator(child operators.Operator, sortFields []planner.OrderByColumnReference, chunkSize uint64, memoryLimitBytes uint64, baseDir string) *ExternalMergeSortOperator {

	return &ExternalMergeSortOperator{
		Child:            child,
		SortFields:       sortFields,
		ChunkSize:        chunkSize,
		MemoryLimitBytes: memoryLimitBytes,
		runFilesManager:  NewRunFilesManager(baseDir),
	}
}

func (op *ExternalMergeSortOperator) Close() {
	if op.Child != nil {
		op.Child.Close()
		op.Child = nil
	}
	op.sortedRows = nil

	if op.runFilesManager != nil {
		op.runFilesManager.close()
		op.runFilesManager = nil
	}
}

func (op *ExternalMergeSortOperator) NextBatch() (*types.ChunkResult, error) {
	if !op.receivedAllData {
		if err := op.readAndSpill(); err != nil {
			return nil, err
		}
	}

	if op.runFilesManager.used() {
		return op.nextBatchFromMerge()
	}

	return op.nextBatchFromMemory()
}

func (op *ExternalMergeSortOperator) readAndSpill() error {
	var currentChunks []*types.ChunkResult
	var currentBytes uint64

	for {
		batch, err := op.Child.NextBatch()
		if err != nil || batch == nil {
			if err != nil {
				return err
			}
			break
		}

		if op.savedSelectIdx == nil {
			op.savedSelectIdx = batch.SelectIdx
		}

		batchSize := batch.SizeInBytes()
		if currentBytes > 0 && currentBytes+batchSize > op.MemoryLimitBytes {
			if err := op.spillToDisk(currentChunks); err != nil {
				return err
			}
			currentChunks = nil
			currentBytes = 0
		}
		currentChunks = append(currentChunks, batch)
		currentBytes += batchSize
	}

	if op.runFilesManager.used() {
		if len(currentChunks) > 0 {
			op.spillToDisk(currentChunks)
		}
		if err := op.initMerge(); err != nil {
			return err
		}
	} else if len(currentChunks) > 0 {
		var err error
		op.sortedRows, err = op.sortInMemory(currentChunks)
		if err != nil {
			return err
		}
	}
	op.receivedAllData = true
	return nil
}

func (op *ExternalMergeSortOperator) spillToDisk(chunks []*types.ChunkResult) error {
	sorted, err := op.sortInMemory(chunks)
	if err != nil {
		return err
	}
	if err := op.runFilesManager.saveChunk(sorted); err != nil {
		return err
	}
	return nil
}

func (op *ExternalMergeSortOperator) sortInMemory(chunks []*types.ChunkResult) (*types.ChunkResult, error) {
	merged, _ := operators.MergeChunkResultsWithinOneSchema(chunks)
	sorter := newBatchSorter(merged, op.SortFields)
	return sorter.sort()
}

func (op *ExternalMergeSortOperator) nextBatchFromMemory() (*types.ChunkResult, error) {
	if op.sortedRows == nil || op.currentOffset >= op.sortedRows.RowCount {
		return nil, nil
	}

	end := min(op.currentOffset+op.ChunkSize, op.sortedRows.RowCount)
	count := end - op.currentOffset
	slicedCols, err := operators.SliceColumns(op.sortedRows.Columns, op.currentOffset, count)
	if err != nil {
		return nil, err
	}

	op.currentOffset = end
	return &types.ChunkResult{
		RowCount:  count,
		Columns:   slicedCols,
		SelectIdx: op.sortedRows.SelectIdx,
		FilterIdx: op.sortedRows.FilterIdx,
	}, nil
}

func (op *ExternalMergeSortOperator) initMerge() error {
	op.mergeHeap = &MergeHeap{sortFields: op.SortFields}
	heapNodes, err := op.runFilesManager.openRunReadersAndReadFirstBatch(op.ChunkSize)
	if err != nil {
		return err
	}

	for _, node := range heapNodes {
		heap.Push(op.mergeHeap, node)
	}
	return nil
}

func (op *ExternalMergeSortOperator) nextBatchFromMerge() (*types.ChunkResult, error) {
	if op.mergeHeap.Len() == 0 {
		return nil, nil
	}

	var outputRows = make([][]any, 0, op.ChunkSize)
	var rowCount uint64 = 0
	for rowCount < op.ChunkSize && op.mergeHeap.Len() > 0 {
		node := heap.Pop(op.mergeHeap).(*MergeNode)

		row := node.currentRow()
		outputRows = append(outputRows, row)
		rowCount++

		hasNext := node.advance()
		if hasNext {
			heap.Push(op.mergeHeap, node)
		} else {
			nextBatchRows, err := op.runFilesManager.readBatch(node.readerKIdx)
			if err != nil {
				return nil, fmt.Errorf("failed to refill buffer for runReader %d: %w", node.readerKIdx, err)
			}

			if len(nextBatchRows) > 0 {
				node.rows = nextBatchRows
				node.nextRow = 0
				heap.Push(op.mergeHeap, node)
			}
		}
	}

	if rowCount == 0 {
		return nil, nil
	}

	return op.anyRowsToChunkResult(outputRows)
}

func (op *ExternalMergeSortOperator) anyRowsToChunkResult(rows [][]any) (*types.ChunkResult, error) {
	firstRow := rows[0]
	outputCols := make([]types.ChunkColumn, len(firstRow))
	for i, val := range firstRow {
		switch val.(type) {
		case int64:
			outputCols[i] = &types.Int64ChunkColumn{Values: make([]int64, 0, op.ChunkSize)}
		case string:
			outputCols[i] = &types.VarcharChunkColumn{
				Data:    make([]byte, 0, op.ChunkSize*16),
				Offsets: make([]uint64, 0, op.ChunkSize),
			}
		case bool:
			outputCols[i] = &types.BooleanChunkColumn{Values: make([]bool, 0, op.ChunkSize)}
		}
	}

	for _, row := range rows {
		for i, val := range row {
			appendAnyToColumn(outputCols[i], val)
		}
	}

	return &types.ChunkResult{
		RowCount:  uint64(len(rows)),
		Columns:   outputCols,
		SelectIdx: op.savedSelectIdx,
		FilterIdx: -1,
	}, nil

}

func appendAnyToColumn(col types.ChunkColumn, val any) {
	switch c := col.(type) {
	case *types.Int64ChunkColumn:
		c.Values = append(c.Values, val.(int64))
	case *types.VarcharChunkColumn:
		str := val.(string)
		start := uint64(len(c.Data))
		c.Data = append(c.Data, str...)
		c.Offsets = append(c.Offsets, start)
	case *types.BooleanChunkColumn:
		c.Values = append(c.Values, val.(bool))
	}
}
