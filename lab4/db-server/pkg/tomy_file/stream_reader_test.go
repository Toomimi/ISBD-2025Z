package tomy_file

import (
	"fmt"
	"io"
	"path/filepath"
	"testing"
)

func TestBatchReader_Integration(t *testing.T) {
	tempDir := t.TempDir()

	table1 := newExampleTable(0, 15)
	table2 := newExampleTable(15, 5)

	file1Path := filepath.Join(tempDir, "file1.tomy")
	if err := table1.Serialize(file1Path); err != nil {
		t.Fatalf("Failed to serialize file1: %v", err)
	}

	file2Path := filepath.Join(tempDir, "file2.tomy")
	if err := table2.Serialize(file2Path); err != nil {
		t.Fatalf("Failed to serialize file2: %v", err)
	}

	tests := []struct {
		name            string
		files           []string
		columns         []string
		batchSize       int
		expectedBatches []expectedBatch
	}{
		{
			name:      "Read all with small batch",
			files:     []string{file1Path, file2Path},
			columns:   []string{"id", "name"},
			batchSize: 4,
			expectedBatches: []expectedBatch{
				{rowCount: 4, startId: 0},
				{rowCount: 4, startId: 4},
				{rowCount: 4, startId: 8},
				{rowCount: 3, startId: 12},
				{rowCount: 4, startId: 15},
				{rowCount: 1, startId: 19},
			},
		},
		{
			name:      "Read specific column with large batch",
			files:     []string{file1Path, file2Path},
			columns:   []string{"id"},
			batchSize: 10,
			expectedBatches: []expectedBatch{
				{rowCount: 10, startId: 0},
				{rowCount: 5, startId: 10},
				{rowCount: 5, startId: 15},
			},
		},
		{
			name:      "Read batch larger than file",
			files:     []string{file1Path},
			columns:   []string{"id"},
			batchSize: 100,
			expectedBatches: []expectedBatch{
				{rowCount: 15, startId: 0},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewBatchReader(tc.files, tc.columns)
			defer reader.Close()

			batchIdx := 0
			for {
				batch, err := reader.GetNextBatch(tc.batchSize)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("GetNextBatch failed: %v", err)
				}

				if batchIdx >= len(tc.expectedBatches) {
					t.Fatalf("Received more batches than expected")
				}
				exp := tc.expectedBatches[batchIdx]

				if batch.NumRows != uint64(exp.rowCount) {
					t.Errorf("Batch %d: expected %d rows, got %d", batchIdx, exp.rowCount, batch.NumRows)
				}

				if len(batch.Columns) != len(tc.columns) {
					t.Errorf("Batch %d: expected %d columns, got %d", batchIdx, len(tc.columns), len(batch.Columns))
				}

				intCol := batch.Columns[0].(*Int64Column)
				for i, val := range intCol.Values {
					expectedVal := int64(exp.startId) + int64(i)
					if val != expectedVal {
						t.Errorf("Batch %d: expected id %d, got %d", batchIdx, expectedVal, val)
					}
				}

				batchIdx++
			}

			if batchIdx != len(tc.expectedBatches) {
				t.Errorf("Expected %d batches, got %d", len(tc.expectedBatches), batchIdx)
			}
		})
	}
}

type expectedBatch struct {
	rowCount int
	startId  int
}

func newExampleTable(start, count int) *ColumnarTable {
	return &ColumnarTable{
		NumRows: uint64(count),
		Columns: []AnyColumn{
			makeInt64Column("id", start, count),
			makeVarcharColumn("name", "row", start, count), // row{start}..row{start+count-1}
		},
	}
}

func makeInt64Column(name string, start, count int) *Int64Column {
	values := make([]int64, count)
	for i := 0; i < count; i++ {
		values[i] = int64(start + i)
	}
	return &Int64Column{
		Name:   name,
		Values: values,
	}
}

func makeVarcharColumn(name, prefix string, start, count int) *VarcharColumn {
	var data []byte
	var offsets []uint64
	for i := 0; i < count; i++ {
		str := fmt.Sprintf("%s%d", prefix, start+i)
		offsets = append(offsets, uint64(len(data)))
		data = append(data, []byte(str)...)
	}
	return &VarcharColumn{
		Name:    name,
		Offsets: offsets,
		Data:    data,
	}
}
