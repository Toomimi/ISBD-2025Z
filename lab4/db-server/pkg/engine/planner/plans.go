package planner

import (
	"isbd4/pkg/engine/expr"
	"isbd4/pkg/metadata"
)

type PlanType int

const (
	PlanTypeCopy PlanType = iota
	PlanTypeSelect
)

type QueryPlan interface {
	Type() PlanType
}

type CopyPlan struct {
	TableName         string
	CsvFilePath       string
	ColumnsMapping    []string
	CsvContainsHeader bool
	Metastore         *metadata.Metastore
}

func (p *CopyPlan) Type() PlanType {
	return PlanTypeCopy
}

type SelectPlan struct {
	QueryDef *SelectQueryDefinition
	Snapshot *metadata.MetastoreSnapshot
}

func (p *SelectPlan) Type() PlanType {
	return PlanTypeSelect
}

type SelectQueryDefinition struct {
	TableName     string
	SelectExpr    []expr.Expression
	WhereExpr     expr.Expression
	OrderByClause []OrderByColumnReference
	Limit         int
}

type OrderByColumnReference struct {
	Index     int
	Ascending bool
}
