package planner

import (
	"fmt"
	"isbd4/openapi"
	"isbd4/pkg/metadata"
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

func (p *Planner) PlanSelect(apiQueryDef openapi.SelectQuery) (QueryPlan, error) {
	candidateTableName, hasColRefs := extractTableName(apiQueryDef)
	if candidateTableName == "" {
		return p.planLiteralQuery(apiQueryDef, hasColRefs)
	}

	msSnapshot, err := p.Metastore.GetTableSnapshot(candidateTableName)
	if err != nil {
		return nil, err
	}

	selectQueryDef, err := validateAndMapQuery(apiQueryDef, candidateTableName, msSnapshot)
	if err != nil {
		return nil, err
	}

	return &SelectPlan{
		Snapshot: msSnapshot,
		QueryDef: selectQueryDef,
	}, nil
}

func (p *Planner) planLiteralQuery(apiQueryDef openapi.SelectQuery, hasColRefs bool) (QueryPlan, error) {
	if hasColRefs {
		return nil, fmt.Errorf("no table name specified and query contains column references")
	}
	selectQueryDef, err := validateAndMapQuery(apiQueryDef, "", nil)
	if err != nil {
		return nil, err
	}

	return &SelectPlan{
		QueryDef: selectQueryDef,
		Snapshot: nil,
	}, nil
}
