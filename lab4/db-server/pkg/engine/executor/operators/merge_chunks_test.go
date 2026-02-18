package operators

import (
	"reflect"
	"testing"

	"isbd4/pkg/engine/types"
)

func TestMergeChunkResultsWithinOneSchema(t *testing.T) {
	c1Row1Str := "Jacek"
	c1Row2Str := "Wrona"
	chunk1 := &types.ChunkResult{
		RowCount: 2,
		Columns: []types.ChunkColumn{
			types.NewInt64Column("id", []int64{1, 2}),
			types.VarcharChunkColumnFromStrings("text", []string{c1Row1Str, c1Row2Str}),
		},
		SelectIdx: []int{0, 1},
		FilterIdx: -1,
	}

	c2Row1Str := "foo"
	c2Row2Str := "bar_baz"
	chunk2 := &types.ChunkResult{
		RowCount: 2,
		Columns: []types.ChunkColumn{
			types.NewInt64Column("id", []int64{3, 4}),
			types.VarcharChunkColumnFromStrings("text", []string{c2Row1Str, c2Row2Str}),
		},
		SelectIdx: []int{0, 1},
		FilterIdx: -1,
	}

	chunks := []*types.ChunkResult{chunk1, chunk2}

	merged, err := mergeChunkResultsWithinOneSchema(chunks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if merged.RowCount != 4 {
		t.Errorf("invalid row count")
	}

	idCol := merged.Columns[0].(*types.Int64ChunkColumn)

	expectedIds := []int64{1, 2, 3, 4}
	if !reflect.DeepEqual(idCol.Values, expectedIds) {
		t.Errorf("not equal")
	}

	textCol := merged.Columns[1].(*types.VarcharChunkColumn)

	expectedStrings := []string{c1Row1Str, c1Row2Str, c2Row1Str, c2Row2Str}
	resultStrings := textCol.GetValuesAsString()

	if !reflect.DeepEqual(resultStrings, expectedStrings) {
		t.Errorf("not equal")
	}

	// 5+5+7+3
	if len(textCol.Data) != 20 {
		t.Errorf("expected data len 20, got %d", len(textCol.Data))
	}

	expectedOffsets := []uint64{0, 5, 10, 13}
	if !reflect.DeepEqual(textCol.Offsets, expectedOffsets) {
		t.Errorf("bad offsets")
	}
}
