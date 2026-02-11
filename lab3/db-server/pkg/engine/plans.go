package engine

import "isbd3/pkg/metadata"

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
	TableName string
	Files     []*metadata.FileEntry
	Columns   []metadata.ColumnDef
	Limit     uint64
}

func (p *SelectPlan) Type() PlanType {
	return PlanTypeSelect
}
