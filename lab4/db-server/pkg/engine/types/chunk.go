package types

type ChunkResult struct {
	RowCount  uint64
	Columns   []ChunkColumn
	SelectIdx []int // indices of output columns
	FilterIdx int   // index of hidden columns, used for filtering
}

func (c *ChunkResult) ToColumnarResult() *ColumnarResult {
	columns := make([]any, len(c.SelectIdx))
	for i, id := range c.SelectIdx {
		columns[i] = c.Columns[id].GetAnyRepr()
	}
	return &ColumnarResult{
		RowCount: c.RowCount,
		Columns:  columns,
	}
}

func (chunk *ChunkResult) WriteToDisk() error {
	panic("not implemented")
	// return nil
}

func (chunk *ChunkResult) SizeInBytes() uint64 {
	var size uint64
	for _, col := range chunk.Columns {
		size += col.SizeInBytes()
	}
	return size
}
