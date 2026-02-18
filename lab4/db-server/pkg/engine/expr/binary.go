package expr

import (
	"fmt"
	"isbd4/pkg/engine/types"
)

type BinaryOperator int

const (
	Add BinaryOperator = iota
	Subtract
	Multiply
	Divide
	And
	Or
	Equal
	NotEqual
	LessThan
	LessEqual
	GreaterThan
	GreaterEqual
)

func (o BinaryOperator) String() string {
	var toString = map[BinaryOperator]string{
		Add:          "ADD",
		Subtract:     "SUBTRACT",
		Multiply:     "MULTIPLY",
		Divide:       "DIVIDE",
		And:          "AND",
		Or:           "OR",
		Equal:        "=",
		NotEqual:     "!=",
		LessThan:     "<",
		LessEqual:    "<=",
		GreaterThan:  ">",
		GreaterEqual: ">=",
	}
	stringVal, ok := toString[o]
	if !ok {
		return "UNKNOWN"
	}
	return stringVal
}

func BinaryOpFromString(op string) (BinaryOperator, error) {
	var fromString = map[string]BinaryOperator{
		"ADD":           Add,
		"SUBTRACT":      Subtract,
		"MULTIPLY":      Multiply,
		"DIVIDE":        Divide,
		"AND":           And,
		"OR":            Or,
		"EQUAL":         Equal,
		"NOT_EQUAL":     NotEqual,
		"LESS_THAN":     LessThan,
		"LESS_EQUAL":    LessEqual,
		"GREATER_THAN":  GreaterThan,
		"GREATER_EQUAL": GreaterEqual,
	}
	operator, ok := fromString[op]
	if !ok {
		return 0, fmt.Errorf("unknown binary operator: %s", op)
	}
	return operator, nil
}

type BinaryOpExpr struct {
	Left     Expression
	Right    Expression
	Operator BinaryOperator
	resType  types.ChunkColumnType
}

func NewBinaryOp(left, right Expression, op BinaryOperator) (*BinaryOpExpr, error) {
	lt := left.ResultType()
	rt := right.ResultType()

	var resType types.ChunkColumnType

	switch op {
	case Add, Subtract, Multiply, Divide:
		if lt != types.ChunkColumnTypeInt64 || rt != types.ChunkColumnTypeInt64 {
			return nil, fmt.Errorf("operator %s requires INT64, got %d and %d", op, lt, rt)
		}
		resType = types.ChunkColumnTypeInt64

	case Equal, NotEqual, LessThan, LessEqual, GreaterThan, GreaterEqual:
		if lt != rt {
			return nil, fmt.Errorf("comparison %s requires the same types, got %d and %d", op, lt, rt)
		}
		resType = types.ChunkColumnTypeBoolean

	case And, Or:
		if lt != types.ChunkColumnTypeBoolean || rt != types.ChunkColumnTypeBoolean {
			return nil, fmt.Errorf("logical operator %s requires BOOLEAN", op)
		}
		resType = types.ChunkColumnTypeBoolean
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}

	return &BinaryOpExpr{
		Left:     left,
		Right:    right,
		Operator: op,
		resType:  resType,
	}, nil
}

func (e *BinaryOpExpr) ResultType() types.ChunkColumnType { return e.resType }

func (e *BinaryOpExpr) GetUsedColumns() []string {
	leftCols := e.Left.GetUsedColumns()
	rightCols := e.Right.GetUsedColumns()
	return append(leftCols, rightCols...)
}

func (e *BinaryOpExpr) Evaluate(batch *types.ChunkResult, colMapping map[string]int) (types.ChunkColumn, error) {
	leftCol, err := e.Left.Evaluate(batch, colMapping)
	if err != nil {
		return nil, err
	}
	rightCol, err := e.Right.Evaluate(batch, colMapping)
	if err != nil {
		return nil, err
	}

	switch e.Operator {
	case Add:
		return e.evaluateAdd(leftCol, rightCol, batch.RowCount)
	case Subtract, Multiply, Divide:
		return e.evaluateArithmetic(leftCol, rightCol, batch.RowCount)
	case And, Or:
		return e.evaluateLogical(leftCol, rightCol, batch.RowCount)
	case Equal, NotEqual, LessThan, LessEqual, GreaterThan, GreaterEqual:
		return e.evaluateComparison(leftCol, rightCol, batch.RowCount)
	}

	return nil, fmt.Errorf("execution for %s not implemented", e.Operator)
}

func (e *BinaryOpExpr) evaluateAdd(leftCol, rightCol types.ChunkColumn, rowCount uint64) (types.ChunkColumn, error) {
	lCol, ok := leftCol.(*types.Int64ChunkColumn)
	if !ok {
		return nil, fmt.Errorf("Left operand is not Int64ChunkColumn")
	}
	rCol, ok := rightCol.(*types.Int64ChunkColumn)
	if !ok {
		return nil, fmt.Errorf("Right operand is not Int64ChunkColumn")
	}

	lData := lCol.Values
	rData := rCol.Values
	res := make([]int64, rowCount)
	for i := 0; i < int(rowCount); i++ {
		res[i] = lData[i] + rData[i]
	}
	return types.NewInt64Column("result", res), nil
}

func (e *BinaryOpExpr) evaluateArithmetic(leftCol, rightCol types.ChunkColumn, rowCount uint64) (types.ChunkColumn, error) {
	lCol, ok1 := leftCol.(*types.Int64ChunkColumn)
	rCol, ok2 := rightCol.(*types.Int64ChunkColumn)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("One of the operands is not Int64ChunkColumn")
	}

	lData := lCol.Values
	rData := rCol.Values
	res := make([]int64, rowCount)

	for i := 0; i < int(rowCount); i++ {
		switch e.Operator {
		case Subtract:
			res[i] = lData[i] - rData[i]
		case Multiply:
			res[i] = lData[i] * rData[i]
		case Divide:
			if rData[i] == 0 {
				return nil, fmt.Errorf("division by zero at row %d", i)
			}
			res[i] = lData[i] / rData[i]
		}
	}
	return types.NewInt64Column("result", res), nil
}

func (e *BinaryOpExpr) evaluateLogical(leftCol, rightCol types.ChunkColumn, rowCount uint64) (types.ChunkColumn, error) {
	lCol, ok1 := leftCol.(*types.BooleanChunkColumn)
	rCol, ok2 := rightCol.(*types.BooleanChunkColumn)
	if !ok1 || !ok2 {
		return nil, fmt.Errorf("One of the operands is not BooleanChunkColumn")
	}

	lData := lCol.Values
	rData := rCol.Values
	res := make([]bool, rowCount)

	for i := 0; i < int(rowCount); i++ {
		switch e.Operator {
		case And:
			res[i] = lData[i] && rData[i]
		case Or:
			res[i] = lData[i] || rData[i]
		}
	}
	return types.NewBooleanColumn("result", res), nil
}

func (e *BinaryOpExpr) evaluateComparison(leftCol, rightCol types.ChunkColumn, rowCount uint64) (types.ChunkColumn, error) {
	if lCol, ok := leftCol.(*types.Int64ChunkColumn); ok {
		rCol, ok := rightCol.(*types.Int64ChunkColumn)
		if !ok {
			return nil, fmt.Errorf("type mismatch in comparison")
		}
		return e.compareInt64(lCol.Values, rCol.Values, rowCount)
	}
	if lCol, ok := leftCol.(*types.VarcharChunkColumn); ok {
		rCol, ok := rightCol.(*types.VarcharChunkColumn)
		if !ok {
			return nil, fmt.Errorf("type mismatch in comparison")
		}
		return e.compareVarchar(lCol, rCol, rowCount)
	}
	return nil, fmt.Errorf("comparison not implemented for this type")
}

func (e *BinaryOpExpr) compareVarchar(lCol, rCol *types.VarcharChunkColumn, rowCount uint64) (types.ChunkColumn, error) {
	res := make([]bool, rowCount)
	for i := 0; i < int(rowCount); i++ {
		lStart := lCol.Offsets[i]
		lEnd := lCol.NextOffset(i)
		lVal := string(lCol.Data[lStart:lEnd])

		rStart := rCol.Offsets[i]
		rEnd := rCol.NextOffset(i)
		rVal := string(rCol.Data[rStart:rEnd])

		switch e.Operator {
		case Equal:
			res[i] = lVal == rVal
		case NotEqual:
			res[i] = lVal != rVal
		case LessThan:
			res[i] = lVal < rVal
		case LessEqual:
			res[i] = lVal <= rVal
		case GreaterThan:
			res[i] = lVal > rVal
		case GreaterEqual:
			res[i] = lVal >= rVal
		default:
			return nil, fmt.Errorf("unsupported operator for varchar comparison: %s", e.Operator)
		}
	}
	return types.NewBooleanColumn("result", res), nil
}

func (e *BinaryOpExpr) compareInt64(lData, rData []int64, rowCount uint64) (types.ChunkColumn, error) {
	res := make([]bool, rowCount)
	for i := 0; i < int(rowCount); i++ {
		switch e.Operator {
		case Equal:
			res[i] = lData[i] == rData[i]
		case NotEqual:
			res[i] = lData[i] != rData[i]
		case LessThan:
			res[i] = lData[i] < rData[i]
		case LessEqual:
			res[i] = lData[i] <= rData[i]
		case GreaterThan:
			res[i] = lData[i] > rData[i]
		case GreaterEqual:
			res[i] = lData[i] >= rData[i]
		}
	}
	return types.NewBooleanColumn("result", res), nil
}
