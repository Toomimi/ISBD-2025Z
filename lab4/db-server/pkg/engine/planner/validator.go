package planner

import (
	"fmt"
	"isbd4/openapi"
	"isbd4/pkg/engine/expr"
	"isbd4/pkg/engine/types"
	"isbd4/pkg/metadata"
)

func extractTableName(queryDef openapi.SelectQuery) (string, bool) {
	hasColumnRefs := false

	for _, col := range queryDef.ColumnClauses {
		name, foundColRefs := traverseExpression(col)
		if name != "" {
			return name, true
		}
		hasColumnRefs = hasColumnRefs || foundColRefs
	}

	if queryDef.WhereClause.Expression != nil {
		name, foundColRefs := traverseExpression(queryDef.WhereClause)
		if name != "" {
			return name, true
		}
		hasColumnRefs = hasColumnRefs || foundColRefs
	}
	return "", hasColumnRefs
}

func traverseExpression(col openapi.ColumnExpression) (tableName string, hasColumnRefs bool) {
	if col.Expression == nil {
		return "", false
	}

	switch e := col.Expression.(type) {
	case openapi.ColumnReferenceExpression:
		if e.TableName != "" {
			return e.TableName, true
		}
		return "", true
	case openapi.ColumnarBinaryOperation:
		n1, f1 := traverseExpression(e.LeftOperand)
		if n1 != "" {
			return n1, true
		}
		n2, f2 := traverseExpression(e.RightOperand)
		if n2 != "" {
			return n2, true
		}
		return "", f1 || f2
	case openapi.ColumnarUnaryOperation:
		return traverseExpression(e.Operand)
	case openapi.Function:
		anyFound := false
		for _, arg := range e.Arguments {
			n, f := traverseExpression(arg)
			if n != "" {
				return n, true
			}
			anyFound = anyFound || f
		}
		return "", anyFound
	}
	return "", false
}

func validateAndMapQuery(apiQueryDef openapi.SelectQuery, tableName string, msSnapshot *metadata.MetastoreSnapshot) (*SelectQueryDefinition, error) {
	ve := &types.ValidationError{}

	selectExprs, whereExpr, exprErrs := validateAndPrepareExpressions(apiQueryDef, msSnapshot, tableName)
	if exprErrs != nil {
		ve.Extend(exprErrs)
	}

	orderByClause, orderByErrs := validateAndExtractOrderBy(apiQueryDef.OrderByClause, len(apiQueryDef.ColumnClauses)) // Note: using len(ColumnClauses) as approximation, technically valid expressions count
	if orderByErrs != nil {
		ve.Extend(orderByErrs)
	}

	limit, err := validateAndExtractLimit(apiQueryDef.LimitClause)
	if err != nil {
		ve.Extend(err)
	}

	if ve.HasProblems() {
		return nil, ve
	}

	selectQueryDef := &SelectQueryDefinition{
		TableName:     tableName,
		SelectExpr:    selectExprs,
		WhereExpr:     whereExpr,
		OrderByClause: orderByClause,
		Limit:         limit,
	}

	return selectQueryDef, nil
}

func validateAndPrepareExpressions(apiQueryDef openapi.SelectQuery, msSnapshot *metadata.MetastoreSnapshot, tableName string) ([]expr.Expression, expr.Expression, error) {
	mapper, err := NewMapper(msSnapshot, tableName)
	if err != nil {
		return nil, nil, err
	}
	ve := &types.ValidationError{}

	selectExprs := make([]expr.Expression, len(apiQueryDef.ColumnClauses))
	for i, selectExpr := range apiQueryDef.ColumnClauses {
		mappedExpr, err := mapper.MapExpression(selectExpr)
		if err != nil {
			ve.Extend(err)
		}
		selectExprs[i] = mappedExpr
	}

	whereExpr, err := mapper.MapExpression(apiQueryDef.WhereClause)
	if err != nil {
		if apiQueryDef.WhereClause.Expression != nil {
			ve.Extend(err)
		}
	} else if whereExpr.ResultType() != types.ChunkColumnTypeBoolean {
		ve.Add("where expression must return boolean", "WhereClause")
	}

	if ve.HasProblems() {
		return nil, nil, ve
	}

	return selectExprs, whereExpr, nil
}

func validateAndExtractOrderBy(orderByClauses []openapi.OrderByExpression, columnsCount int) ([]OrderByColumnReference, error) {
	orderByColumns := make([]OrderByColumnReference, len(orderByClauses))
	ve := &types.ValidationError{}

	for i, clause := range orderByClauses {
		colId := int(clause.ColumnIndex)
		if colId < 0 || colId >= columnsCount {
			ve.Add(fmt.Sprintf("invalid column index: %d", colId), fmt.Sprintf("OrderByClause %d", i))
			continue
		}
		orderByColumns[i] = OrderByColumnReference{
			Index:     colId,
			Ascending: clause.Ascending,
		}
	}

	if ve.HasProblems() {
		return nil, ve
	}

	return orderByColumns, nil
}

func validateAndExtractLimit(limitClause *openapi.LimitExpression) (int, error) {
	if limitClause == nil {
		return -1, nil
	}

	if limitClause.Limit < 0 {
		return -1, fmt.Errorf("limit must be non-negative")
	}

	return int(limitClause.Limit), nil
}
