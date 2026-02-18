package executor

import (
	"isbd4/pkg/engine/executor/operators"
	"isbd4/pkg/engine/expr"
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
		lastOp = &operators.TransformationOperator{
			Child:       lastOp,
			Expressions: []expr.Expression{p.QueryDef.WhereExpr},
			IsFilter:    true,
		}

		lastOp = &operators.FilterOperator{
			Child: lastOp,
		}
	}

	lastOp = &operators.TransformationOperator{
		Child:       lastOp,
		Expressions: p.QueryDef.SelectExpr,
	}

	if len(p.QueryDef.OrderByClause) > 0 {
		lastOp = &operators.SortOperator{
			Child:      lastOp,
			SortFields: p.QueryDef.OrderByClause,
			ChunkSize:  e.chunkSize,
		}
	}

	if p.QueryDef.Limit >= 0 {
		lastOp = operators.NewLimitOperator(lastOp, uint64(p.QueryDef.Limit))
	}

	defer lastOp.Close()
	return operators.CollectAllBatches(lastOp)
}
