package expr

import (
	"fmt"
	"isbd4/pkg/engine/types"
	"strings"
)

type FunctionName string

const (
	StrLen  FunctionName = "STRLEN"
	Concat  FunctionName = "CONCAT"
	Replace FunctionName = "REPLACE"
	Upper   FunctionName = "UPPER"
	Lower   FunctionName = "LOWER"
)

func FunctionNameFromString(name string) (FunctionName, error) {
	switch name {
	case "STRLEN":
		return StrLen, nil
	case "CONCAT":
		return Concat, nil
	case "REPLACE":
		return Replace, nil
	case "UPPER":
		return Upper, nil
	case "LOWER":
		return Lower, nil
	default:
		return "", fmt.Errorf("unknown function: %s", name)
	}
}

type FunctionExpr struct {
	Name      FunctionName
	Arguments []Expression
	resType   types.ChunkColumnType
}

func NewFunction(name FunctionName, args []Expression) (*FunctionExpr, error) {
	var resType types.ChunkColumnType

	switch name {
	case StrLen:
		if len(args) != 1 {
			return nil, fmt.Errorf("STRLEN expects 1 argument, got %d", len(args))
		}
		if args[0].ResultType() != types.ChunkColumnTypeVarchar {
			return nil, fmt.Errorf("STRLEN argument must be VARCHAR, got %d", args[0].ResultType())
		}
		resType = types.ChunkColumnTypeInt64

	case Concat:
		if len(args) < 2 {
			return nil, fmt.Errorf("CONCAT expects at least 2 arguments")
		}
		for i, arg := range args {
			if arg.ResultType() != types.ChunkColumnTypeVarchar {
				return nil, fmt.Errorf("CONCAT argument %d must be VARCHAR, got %d", i, arg.ResultType())
			}
		}
		resType = types.ChunkColumnTypeVarchar

	case Upper, Lower:
		if len(args) != 1 || args[0].ResultType() != types.ChunkColumnTypeVarchar {
			return nil, fmt.Errorf("%s expects 1 VARCHAR argument", name)
		}
		resType = types.ChunkColumnTypeVarchar

	case Replace:
		if len(args) != 3 {
			return nil, fmt.Errorf("REPLACE expects 3 arguments (source, old, new)")
		}
		for _, arg := range args {
			if arg.ResultType() != types.ChunkColumnTypeVarchar {
				return nil, fmt.Errorf("REPLACE arguments must be VARCHAR")
			}
		}
		resType = types.ChunkColumnTypeVarchar

	default:
		return nil, fmt.Errorf("unsupported function: %s", name)
	}

	return &FunctionExpr{
		Name:      name,
		Arguments: args,
		resType:   resType,
	}, nil
}

func (e *FunctionExpr) ResultType() types.ChunkColumnType { return e.resType }

func (e *FunctionExpr) GetUsedColumns() []string {
	var cols []string
	for _, arg := range e.Arguments {
		cols = append(cols, arg.GetUsedColumns()...)
	}
	return cols
}

func (e *FunctionExpr) Evaluate(batch *types.ChunkResult, colMapping map[string]int) (types.ChunkColumn, error) {
	argColumns := make([]types.ChunkColumn, len(e.Arguments))
	for i, argExpr := range e.Arguments {
		col, err := argExpr.Evaluate(batch, colMapping)
		if err != nil {
			return nil, err
		}
		argColumns[i] = col
	}

	switch e.Name {
	case StrLen:
		col, ok := argColumns[0].(*types.VarcharChunkColumn)
		if !ok {
			return nil, fmt.Errorf("STRLEN expects VarcharChunkColumn")
		}
		res := make([]int64, batch.RowCount)
		for i := 0; i < int(batch.RowCount); i++ {
			start := col.Offsets[i]
			end := col.NextOffset(i)
			res[i] = int64(end - start)
		}
		return types.NewInt64Column("strlen", res), nil

	case Concat:
		res := make([]string, batch.RowCount)
		for i := 0; i < int(batch.RowCount); i++ {
			var sb strings.Builder
			for _, col := range argColumns {
				vc, ok := col.(*types.VarcharChunkColumn)
				if !ok {
					return nil, fmt.Errorf("CONCAT expects all arguments to be VarcharChunkColumn")
				}
				start := vc.Offsets[i]
				end := vc.NextOffset(i)
				sb.WriteString(string(vc.Data[start:end]))
			}
			res[i] = sb.String()
		}
		return types.VarcharChunkColumnFromStrings("concat", res), nil

	case Upper:
		col, ok := argColumns[0].(*types.VarcharChunkColumn)
		if !ok {
			return nil, fmt.Errorf("UPPER expects VarcharChunkColumn")
		}

		res := make([]string, batch.RowCount)
		for i := 0; i < int(batch.RowCount); i++ {
			start := col.Offsets[i]
			end := col.NextOffset(i)

			originalStr := string(col.Data[start:end])
			res[i] = strings.ToUpper(originalStr)
		}
		return types.VarcharChunkColumnFromStrings("upper", res), nil
	case Lower:
		col, ok := argColumns[0].(*types.VarcharChunkColumn)
		if !ok {
			return nil, fmt.Errorf("LOWER expects VarcharChunkColumn")
		}

		res := make([]string, batch.RowCount)
		for i := 0; i < int(batch.RowCount); i++ {
			start := col.Offsets[i]
			end := col.NextOffset(i)

			originalStr := string(col.Data[start:end])
			res[i] = strings.ToLower(originalStr)
		}
		return types.VarcharChunkColumnFromStrings("lower", res), nil

	case Replace:
		srcCol, ok1 := argColumns[0].(*types.VarcharChunkColumn)
		oldCol, ok2 := argColumns[1].(*types.VarcharChunkColumn)
		newCol, ok3 := argColumns[2].(*types.VarcharChunkColumn)
		if !ok1 || !ok2 || !ok3 {
			return nil, fmt.Errorf("REPLACE expects VarcharChunkColumn")
		}

		res := make([]string, batch.RowCount)

		for i := 0; i < int(batch.RowCount); i++ {
			srcStart := srcCol.Offsets[i]
			srcEnd := srcCol.NextOffset(i)
			srcVal := string(srcCol.Data[srcStart:srcEnd])

			oldStart := oldCol.Offsets[i]
			oldEnd := oldCol.NextOffset(i)
			oldVal := string(oldCol.Data[oldStart:oldEnd])

			newStart := newCol.Offsets[i]
			newEnd := newCol.NextOffset(i)
			newVal := string(newCol.Data[newStart:newEnd])

			res[i] = strings.ReplaceAll(srcVal, oldVal, newVal)
		}
		return types.VarcharChunkColumnFromStrings("replace", res), nil
	}

	return nil, fmt.Errorf("runtime error: function %s not implemented", e.Name)
}
