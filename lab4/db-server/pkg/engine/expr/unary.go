package expr

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

type UnaryOperator int

const (
	Not UnaryOperator = iota
	Minus
)

func (o UnaryOperator) String() string {
	var toString = map[UnaryOperator]string{
		Not:   "NOT",
		Minus: "MINUS",
	}
	stringVal, ok := toString[o]
	if !ok {
		return "UNKNOWN"
	}
	return stringVal
}

func UnaryOpFromString(op string) (UnaryOperator, error) {
	var fromString = map[string]UnaryOperator{
		"NOT":   Not,
		"MINUS": Minus,
	}
	operator, ok := fromString[op]
	if !ok {
		return 0, fmt.Errorf("unknown unary operator: %s", op)
	}
	return operator, nil
}

type UnaryOpExpr struct {
	Operand  Expression
	Operator UnaryOperator
	resType  types.ChunkColumnType
}

func NewUnaryOp(operand Expression, op UnaryOperator) (*UnaryOpExpr, error) {
	ot := operand.ResultType()
	var resType types.ChunkColumnType

	switch op {
	case Not:
		if ot != types.ChunkColumnTypeBoolean {
			return nil, fmt.Errorf("NOT operator requires BOOLEAN, got %d", ot)
		}
		resType = types.ChunkColumnTypeBoolean
	case Minus:
		if ot != types.ChunkColumnTypeInt64 {
			return nil, fmt.Errorf("MINUS operator requires INT64, got %d", ot)
		}
		resType = types.ChunkColumnTypeInt64
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", op)
	}

	return &UnaryOpExpr{
		Operand:  operand,
		Operator: op,
		resType:  resType,
	}, nil
}

func (e *UnaryOpExpr) ResultType() types.ChunkColumnType { return e.resType }

func (e *UnaryOpExpr) GetUsedColumns() []string {
	return e.Operand.GetUsedColumns()
}

func (e *UnaryOpExpr) Evaluate(batch *types.ChunkResult, colMapping map[string]int) (types.ChunkColumn, error) {
	col, err := e.Operand.Evaluate(batch, colMapping)
	if err != nil {
		return nil, err
	}

	rowCount := batch.RowCount

	switch e.Operator {
	case Not:
		bCol, ok := col.(*types.BooleanChunkColumn)
		if !ok {
			return nil, fmt.Errorf("Operand is not BooleanChunkColumn")
		}
		res := make([]bool, rowCount)
		for i := 0; i < int(rowCount); i++ {
			res[i] = !bCol.Values[i]
		}
		return &types.BooleanChunkColumn{Name: "result", Values: res}, nil

	case Minus:
		iCol, ok := col.(*types.Int64ChunkColumn)
		if !ok {
			return nil, fmt.Errorf("Operand is not Int64ChunkColumn")
		}
		res := make([]int64, rowCount)
		for i := 0; i < int(rowCount); i++ {
			res[i] = -iCol.Values[i]
		}
		return &types.Int64ChunkColumn{Name: "result", Values: res}, nil
	}

	return nil, fmt.Errorf("execution for %s not implemented", e.Operator)
}
