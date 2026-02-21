package executor

import (
	"isbd4/pkg/engine/executor/operators"
	operators_sort "isbd4/pkg/engine/executor/operators/sort"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
)

func (e *Executor) executeSelect(p *planner.SelectPlan) (*types.ColumnarResult, error) {
	var lastOp operators.Operator

	if p.Snapshot == nil {
		lastOp = &operators.DummyReaderOperator{}
	} else {
		lastOp = operators.NewReaderOperator(p.Snapshot, p.QueryDef, e.chunkSize)
	}

	if p.QueryDef.WhereExpr != nil {
		lastOp = operators.NewFilterTransformationOperator(lastOp, p.QueryDef.WhereExpr)
		lastOp = operators.NewFilterOperator(lastOp)
	}

	lastOp = operators.NewTransformationOperator(lastOp, p.QueryDef.SelectExpr)

	if len(p.QueryDef.OrderByClause) > 0 {
		lastOp = operators_sort.NewExternalMergeSortOperator(
			lastOp,
			p.QueryDef.OrderByClause,
			e.chunkSize,
			e.memoryLimitBytes,
			e.tablesDir,
		)
	}

	if p.QueryDef.Limit >= 0 {
		lastOp = operators.NewLimitOperator(lastOp, uint64(p.QueryDef.Limit))
	}

	defer lastOp.Close()
	return operators.CollectAllBatches(lastOp)
}
