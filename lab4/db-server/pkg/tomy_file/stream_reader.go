package tomy_file

import (
	"fmt"
	"io"
)

type BatchReader struct {
	filePaths     []string
	columnsToRead []string

	currentFileIdx int
	currentTable   *ColumnarTable
	currentRow     uint64
}

func NewBatchReader(filePaths []string, columnsToRead []string) *BatchReader {
	return &BatchReader{
		filePaths:     filePaths,
		columnsToRead: columnsToRead,
	}
}

func (r *BatchReader) Close() error {
	r.currentTable = nil
	return nil
}

func (r *BatchReader) GetNextBatch(batchSize int) (*ColumnarTable, error) {
	if r.currentTable == nil && r.currentFileIdx >= len(r.filePaths) {
		return nil, io.EOF
	}

	if r.currentTable == nil {
		if err := r.loadNextFile(); err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}
			return nil, err
		}
	}

	remaining := r.currentTable.NumRows - r.currentRow

	if remaining == 0 {
		r.currentFileIdx++
		r.currentTable = nil
		r.currentRow = 0
		return r.GetNextBatch(batchSize)
	}

	toRead := uint64(batchSize)
	if toRead > remaining {
		toRead = remaining
	}

	batch := &ColumnarTable{
		NumRows: toRead,
		Columns: make([]AnyColumn, len(r.currentTable.Columns)),
	}

	for i, col := range r.currentTable.Columns {
		sliced, err := sliceColumn(col, r.currentRow, toRead)
		if err != nil {
			return nil, err
		}
		batch.Columns[i] = sliced
	}

	r.currentRow += toRead
	return batch, nil
}

func (r *BatchReader) loadNextFile() error {
	if r.currentFileIdx >= len(r.filePaths) {
		return io.EOF
	}

	filePath := r.filePaths[r.currentFileIdx]

	// Use the DeserializeColumns function
	table, err := DeserializeColumns(filePath, r.columnsToRead)
	if err != nil {
		return fmt.Errorf("failed to load file %s: %w", filePath, err)
	}
	r.currentTable = table
	r.currentRow = 0
	return nil
}

func sliceColumn(col AnyColumn, start, count uint64) (AnyColumn, error) {
	switch c := col.(type) {
	case *Int64Column:
		if start+count > uint64(len(c.Values)) {
			return nil, fmt.Errorf("slice out of bounds for Int64Column")
		}
		newValues := make([]int64, count)
		copy(newValues, c.Values[start:start+count])
		return &Int64Column{
			Name:   c.Name,
			Values: newValues,
		}, nil

	case *VarcharColumn:
		if start+count > uint64(len(c.Offsets)) {
			return nil, fmt.Errorf("slice out of bounds for VarcharColumn")
		}

		startIdx := int(start)
		countIdx := int(count)

		dataStart := c.Offsets[startIdx]
		var dataEnd uint64
		// If the last element requested is the last element in the column
		if startIdx+countIdx == len(c.Offsets) {
			dataEnd = uint64(len(c.Data))
		} else if startIdx+countIdx < len(c.Offsets) {
			dataEnd = c.Offsets[startIdx+countIdx]
		} else {
			// Should be covered by slice out of bounds check, but just in case
			return nil, fmt.Errorf("slice calculation error")
		}

		newData := make([]byte, dataEnd-dataStart)
		copy(newData, c.Data[dataStart:dataEnd])

		newOffsets := make([]uint64, countIdx)
		for i := 0; i < countIdx; i++ {
			newOffsets[i] = c.Offsets[startIdx+i] - dataStart
		}

		return &VarcharColumn{
			Name:    c.Name,
			Offsets: newOffsets,
			Data:    newData,
		}, nil

	default:
		return nil, fmt.Errorf("unknown column type: %T", col)
	}
}
