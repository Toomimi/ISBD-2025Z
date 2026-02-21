package operators

import (
	"isbd4/pkg/engine/expr"
	"isbd4/pkg/engine/types"
)

type TransformationOperator struct {
	Child       Operator
	Expressions []expr.Expression
	IsFilter    bool // whether the transformation is used for filtering
}

func NewFilterTransformationOperator(child Operator, whereExpr expr.Expression) *TransformationOperator {
	return &TransformationOperator{
		Child:       child,
		Expressions: []expr.Expression{whereExpr},
		IsFilter:    true,
	}
}

func NewTransformationOperator(child Operator, expressions []expr.Expression) *TransformationOperator {
	return &TransformationOperator{
		Child:       child,
		Expressions: expressions,
		IsFilter:    false,
	}
}

func (op *TransformationOperator) Close() {
	if op.Child != nil {
		op.Child.Close()
		op.Child = nil
	}
	op.Expressions = nil
}

func (op *TransformationOperator) NextBatch() (*types.ChunkResult, error) {
	batch, err := op.Child.NextBatch()
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}

	newColumns := make([]types.ChunkColumn, len(op.Expressions))
	colMapping := make(map[string]int)
	for i, c := range batch.Columns {
		colMapping[c.GetName()] = i
	}
	for i, e := range op.Expressions {
		col, err := e.Evaluate(batch, colMapping)
		if err != nil {
			return nil, err
		}
		newColumns[i] = col
	}

	if op.IsFilter {
		return &types.ChunkResult{
			RowCount:  batch.RowCount,
			Columns:   append(batch.Columns, newColumns...),
			SelectIdx: batch.SelectIdx,
			FilterIdx: len(batch.Columns) + len(newColumns) - 1,
		}, nil
	}

	selectIdx := make([]int, len(newColumns))
	for i := range selectIdx {
		selectIdx[i] = i
	}

	return &types.ChunkResult{
		RowCount:  batch.RowCount,
		Columns:   newColumns,
		SelectIdx: selectIdx,
		FilterIdx: -1, // Projection overwrote all columns
	}, nil
}
