package expr

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

type LiteralExpr struct {
	Value interface{}
	Type  types.ChunkColumnType
}

func (e *LiteralExpr) ResultType() types.ChunkColumnType { return e.Type }

func (e *LiteralExpr) GetUsedColumns() []string {
	return nil
}

func (e *LiteralExpr) Evaluate(batch *types.ChunkResult, _ map[string]int) (types.ChunkColumn, error) {
	switch v := e.Value.(type) {
	case int64:
		res := make([]int64, batch.RowCount)
		for i := range res {
			res[i] = v
		}
		return types.NewInt64Column("literal", res), nil
	case string:
		return varcharChunkColumnFromBytesLiteral("literal", []byte(v), batch.RowCount), nil
	case bool:
		res := make([]bool, batch.RowCount)
		for i := range res {
			res[i] = v
		}
		return types.NewBooleanColumn("literal", res), nil
	}

	return nil, fmt.Errorf("unsupported literal type")
}

func varcharChunkColumnFromBytesLiteral(name string, data []byte, count uint64) *types.VarcharChunkColumn {
	valLen := uint64(len(data))
	offsets := make([]uint64, count)
	allData := make([]byte, valLen*count)

	for i := range count {
		start := i * valLen
		offsets[i] = start
		copy(allData[start:start+valLen], data)
	}
	return &types.VarcharChunkColumn{Name: name, Offsets: offsets, Data: allData}
}
