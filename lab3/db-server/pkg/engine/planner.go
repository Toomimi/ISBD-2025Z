package engine

import (
	"fmt"
	"isbd3/pkg/metadata"
)

type Planner struct {
	Metastore *metadata.Metastore
}

func NewPlanner(m *metadata.Metastore) *Planner {
	return &Planner{Metastore: m}
}

func (p *Planner) PlanCopy(tableName string, csvFilePath string, columnsMapping []string, csvContainsHeader bool) (*CopyPlan, error) {
	if _, exists := p.Metastore.GetTableByName(tableName); !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	return &CopyPlan{
		TableName:         tableName,
		CsvFilePath:       csvFilePath,
		Metastore:         p.Metastore,
		ColumnsMapping:    columnsMapping,
		CsvContainsHeader: csvContainsHeader,
	}, nil
}

func (p *Planner) PlanSelect(tableName string, limit int32) (*SelectPlan, error) {
	files, columns, exists := p.Metastore.GetTableSnapshot(tableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	return &SelectPlan{
		TableName: tableName,
		Files:     files,
		Columns:   columns,
		Limit:     uint64(limit),
	}, nil
}
