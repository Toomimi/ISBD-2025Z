package planner

import (
	"fmt"
	"isbd4/openapi"
	"isbd4/pkg/engine/expr"
	"isbd4/pkg/engine/types"
	"isbd4/pkg/metadata"
)

type Mapper struct {
	tableName  string
	nameToType map[string]types.ChunkColumnType
}

func NewMapper(snapshot *metadata.MetastoreSnapshot, tableName string) (*Mapper, error) {
	colMap := make(map[string]types.ChunkColumnType)
	if snapshot != nil {
		for _, col := range snapshot.Columns {
			colType, err := types.ChunkColumnTypeFromMetadataColumnType(col.Type)
			if err != nil {
				return nil, err
			}
			colMap[col.Name] = colType
		}
	}

	return &Mapper{
		tableName:  tableName,
		nameToType: colMap,
	}, nil
}

func (m *Mapper) MapExpression(apiExpr openapi.ColumnExpression) (expr.Expression, error) {
	if apiExpr.Expression == nil {
		return nil, fmt.Errorf("expression cannot be nil")
	}

	switch e := apiExpr.Expression.(type) {
	case openapi.ColumnReferenceExpression:
		return m.mapColumnReference(e)
	case openapi.Literal:
		return m.mapLiteral(e)
	case openapi.ColumnarBinaryOperation:
		return m.mapBinaryOp(e)
	case openapi.ColumnarUnaryOperation:
		return m.mapUnaryOp(e)
	case openapi.Function:
		return m.mapFunction(e)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func (m *Mapper) mapColumnReference(apiColRef openapi.ColumnReferenceExpression) (expr.Expression, error) {
	if apiColRef.TableName != "" && apiColRef.TableName != m.tableName {
		return nil, fmt.Errorf("column %s refers to table %s, but query is on table %s", apiColRef.ColumnName, apiColRef.TableName, m.tableName)
	}

	colType, ok := m.nameToType[apiColRef.ColumnName]
	if !ok {
		return nil, fmt.Errorf("column %s not found in table %s", apiColRef.ColumnName, m.tableName)
	}

	return &expr.ColumnRefExpr{
		ColName: apiColRef.ColumnName,
		ColType: colType,
	}, nil
}

func (m *Mapper) mapLiteral(lit openapi.Literal) (expr.Expression, error) {
	switch v := lit.Value.Data.(type) {
	case int64:
		return &expr.LiteralExpr{Value: v, Type: types.ChunkColumnTypeInt64}, nil
	case string:
		return &expr.LiteralExpr{Value: v, Type: types.ChunkColumnTypeVarchar}, nil
	case bool:
		return &expr.LiteralExpr{Value: v, Type: types.ChunkColumnTypeBoolean}, nil
	default:
		return nil, types.NewVErr(fmt.Sprintf("unsupported literal type: %T (value: %v)", v, v), "")
	}
}

func (m *Mapper) mapBinaryOp(apiOpExpr openapi.ColumnarBinaryOperation) (expr.Expression, error) {
	ve := &types.ValidationError{}

	left, lErr := m.MapExpression(apiOpExpr.LeftOperand)
	if lErr != nil {
		ve.Extend(lErr)
	}

	right, rErr := m.MapExpression(apiOpExpr.RightOperand)
	if rErr != nil {
		ve.Extend(rErr)
	}

	if ve.HasProblems() {
		return nil, ve
	}

	op, err := expr.BinaryOpFromString(string(apiOpExpr.Operator))
	if err != nil {
		return nil, err
	}

	binExpr, err := expr.NewBinaryOp(left, right, op)
	if err != nil {
		return nil, err
	}

	return binExpr, nil
}

func (m *Mapper) mapUnaryOp(apiOpExpr openapi.ColumnarUnaryOperation) (expr.Expression, error) {
	operand, valErr := m.MapExpression(apiOpExpr.Operand)
	if valErr != nil {
		return nil, valErr
	}

	op, err := expr.UnaryOpFromString(string(apiOpExpr.Operator))
	if err != nil {
		return nil, err
	}

	unExpr, err := expr.NewUnaryOp(operand, op)
	if err != nil {
		return nil, err
	}
	return unExpr, nil
}

func (m *Mapper) mapFunction(apiFnExpr openapi.Function) (expr.Expression, error) {
	ve := &types.ValidationError{}
	mappedArgs := make([]expr.Expression, len(apiFnExpr.Arguments))

	for i, arg := range apiFnExpr.Arguments {
		mArg, err := m.MapExpression(arg)
		if err != nil {
			ve.Extend(err)
		} else {
			mappedArgs[i] = mArg
		}
	}

	if ve.HasProblems() {
		return nil, ve
	}

	funcName, err := expr.FunctionNameFromString(string(apiFnExpr.FunctionName))
	if err != nil {
		return nil, err
	}

	fnExpr, err := expr.NewFunction(funcName, mappedArgs)
	if err != nil {
		return nil, err
	}
	return fnExpr, nil
}
