package tomy_file

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_file.tomy")

	numRows := uint64(1000)

	col1Data := make([]int64, numRows)
	col2Data := make([]int64, numRows)
	var col3DataOffsets []uint64
	var col3DataBytes []byte

	for i := uint64(0); i < numRows; i++ {
		col1Data[i] = int64(100 + i)
		col2Data[i] = int64(rand.Intn(1000))

		strVal := fmt.Sprintf("val-%d", i)
		col3DataOffsets = append(col3DataOffsets, uint64(len(strVal)))
		col3DataBytes = append(col3DataBytes, []byte(strVal)...)
	}

	// Prepare table structure
	table := ColumnarTable{
		NumRows: numRows,
		Columns: []AnyColumn{
			Int64Column{Name: "id", Values: col1Data},
			Int64Column{Name: "random_val", Values: col2Data},
			VarcharColumn{Name: "description", Offsets: col3DataOffsets, Data: col3DataBytes},
		},
	}

	// Serialize
	if err := table.Serialize(filePath); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("File was not created: %v", err)
	}
	t.Logf("Created file size: %d bytes", info.Size())

	// Deserialize
	readTable, err := Deserialize(filePath)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Validate column and row count
	if readTable.NumRows != numRows {
		t.Errorf("Expected %d rows, got %d", numRows, readTable.NumRows)
	}

	if len(readTable.Columns) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(readTable.Columns))
	}

	int64Count := 0
	varcharCount := 0
	for _, col := range readTable.Columns {
		switch col.(type) {
		case *Int64Column:
			int64Count++
		case *VarcharColumn:
			varcharCount++
		}
	}

	if int64Count != 2 {
		t.Fatalf("Expected 2 Int64 columns, got %d", int64Count)
	}

	if varcharCount != 1 {
		t.Fatalf("Expected 1 Varchar column, got %d", varcharCount)
	}

	// Validate column content
	if !reflect.DeepEqual(readTable.Columns[0].(*Int64Column).Values, col1Data) {
		t.Errorf("Column 'id' data mismatch")
	}

	if !reflect.DeepEqual(readTable.Columns[1].(*Int64Column).Values, col2Data) {
		t.Errorf("Column 'random_val' data mismatch")
	}

	if !reflect.DeepEqual(readTable.Columns[2].(*VarcharColumn).Offsets, col3DataOffsets) {
		t.Errorf("Column 'description' offsets mismatch")
	}

	if !bytes.Equal(readTable.Columns[2].(*VarcharColumn).Data, col3DataBytes) {
		t.Errorf("Column 'description' bytes mismatch")
	}
}
