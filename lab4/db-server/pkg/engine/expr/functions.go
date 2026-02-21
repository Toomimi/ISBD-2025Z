package expr

import (
	"bytes"
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
		return e.evalStrLen(batch, argColumns)
	case Concat:
		return e.evalConcat(batch, argColumns)
	case Upper, Lower:
		return e.evalUpperLower(batch, argColumns)
	case Replace:
		return e.evalReplace(batch, argColumns)
	default:
		return nil, fmt.Errorf("runtime error: function %s not implemented", e.Name)
	}
}

func (e *FunctionExpr) evalStrLen(batch *types.ChunkResult, args []types.ChunkColumn) (types.ChunkColumn, error) {
	col := args[0].(*types.VarcharChunkColumn)
	res := make([]int64, batch.RowCount)
	for i := 0; i < int(batch.RowCount); i++ {
		res[i] = int64(col.NextOffset(i) - col.Offsets[i])
	}
	return types.NewInt64Column("strlen", res), nil
}

func (e *FunctionExpr) evalConcat(batch *types.ChunkResult, args []types.ChunkColumn) (types.ChunkColumn, error) {
	var totalSize int
	for _, arg := range args {
		vc := arg.(*types.VarcharChunkColumn)
		totalSize += len(vc.Data)
	}

	resCol := &types.VarcharChunkColumn{
		Name:    "concat",
		Offsets: make([]uint64, batch.RowCount),
		Data:    make([]byte, 0, totalSize),
	}

	for i := 0; i < int(batch.RowCount); i++ {
		resCol.Offsets[i] = uint64(len(resCol.Data))
		for _, arg := range args {
			vc := arg.(*types.VarcharChunkColumn)
			start, end := vc.Offsets[i], vc.NextOffset(i)
			resCol.Data = append(resCol.Data, vc.Data[start:end]...)
		}
	}
	return resCol, nil
}

func (e *FunctionExpr) evalUpperLower(batch *types.ChunkResult, args []types.ChunkColumn) (types.ChunkColumn, error) {
	col := args[0].(*types.VarcharChunkColumn)

	resCol := &types.VarcharChunkColumn{
		Name:    strings.ToLower(string(e.Name)),
		Offsets: make([]uint64, batch.RowCount),
		Data:    make([]byte, 0, len(col.Data)),
	}

	for i := 0; i < int(batch.RowCount); i++ {
		resCol.Offsets[i] = uint64(len(resCol.Data))
		start, end := col.Offsets[i], col.NextOffset(i)
		val := col.Data[start:end]
		if e.Name == Upper {
			resCol.Data = append(resCol.Data, bytes.ToUpper(val)...)
		} else {
			resCol.Data = append(resCol.Data, bytes.ToLower(val)...)
		}
	}
	return resCol, nil
}

func (e *FunctionExpr) evalReplace(batch *types.ChunkResult, args []types.ChunkColumn) (types.ChunkColumn, error) {
	srcCol := args[0].(*types.VarcharChunkColumn)
	oldCol := args[1].(*types.VarcharChunkColumn)
	newCol := args[2].(*types.VarcharChunkColumn)

	var totalSize int
	for i := 0; i < int(batch.RowCount); i++ {
		srcVal := srcCol.Data[srcCol.Offsets[i]:srcCol.NextOffset(i)]
		oldVal := oldCol.Data[oldCol.Offsets[i]:oldCol.NextOffset(i)]
		newVal := newCol.Data[newCol.Offsets[i]:newCol.NextOffset(i)]

		if len(oldVal) == 0 {
			totalSize += len(srcVal)
		} else {
			count := bytes.Count(srcVal, oldVal)
			totalSize += len(srcVal) + count*(len(newVal)-len(oldVal))
		}
	}

	resCol := &types.VarcharChunkColumn{
		Name:    "replace",
		Offsets: make([]uint64, batch.RowCount),
		Data:    make([]byte, 0, totalSize),
	}

	for i := 0; i < int(batch.RowCount); i++ {
		resCol.Offsets[i] = uint64(len(resCol.Data))
		currentSrc := srcCol.Data[srcCol.Offsets[i]:srcCol.NextOffset(i)]
		oldVal := oldCol.Data[oldCol.Offsets[i]:oldCol.NextOffset(i)]
		newVal := newCol.Data[newCol.Offsets[i]:newCol.NextOffset(i)]

		if len(oldVal) == 0 {
			resCol.Data = append(resCol.Data, currentSrc...)
		} else {
			for {
				idx := bytes.Index(currentSrc, oldVal)
				if idx == -1 {
					resCol.Data = append(resCol.Data, currentSrc...)
					break
				}
				resCol.Data = append(resCol.Data, currentSrc[:idx]...)
				resCol.Data = append(resCol.Data, newVal...)
				currentSrc = currentSrc[idx+len(oldVal):]
			}
		}
	}
	return resCol, nil
}
