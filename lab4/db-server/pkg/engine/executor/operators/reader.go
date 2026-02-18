package operators

import (
	"fmt"
	"io"
	"isbd4/pkg/engine/expr"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
	"isbd4/pkg/metadata"
	"isbd4/pkg/tomy_file"
)

type ReaderOperator struct {
	TableReader   *tomy_file.BatchReader
	ChunkSize     uint64
	ColumnsToRead []string
}

func NewReaderOperator(snapshot *metadata.MetastoreSnapshot, queryDef *planner.SelectQueryDefinition, chunkSize uint64) *ReaderOperator {
	colNames := extractUsedColumns(queryDef)

	filePaths := metadata.FileNames(snapshot.Files)
	reader := tomy_file.NewBatchReader(filePaths, colNames)

	return &ReaderOperator{
		TableReader:   reader,
		ChunkSize:     chunkSize,
		ColumnsToRead: colNames,
	}
}

func extractUsedColumns(queryDef *planner.SelectQueryDefinition) []string {
	allExprs := queryDef.SelectExpr
	if queryDef.WhereExpr != nil {
		allExprs = append(allExprs, queryDef.WhereExpr)
	}
	return expr.GetUsedColumnsFromExpressions(allExprs)
}

func (r *ReaderOperator) Close() {
	if r.TableReader != nil {
		r.TableReader.Close()
	}
}

func (r *ReaderOperator) NextBatch() (*types.ChunkResult, error) {
	if r.TableReader == nil {
		return nil, nil
	}

	batch, err := r.TableReader.GetNextBatch(int(r.ChunkSize))
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}

	chunkColumns := make([]types.ChunkColumn, len(batch.Columns))
	for i, col := range batch.Columns {
		chunkCol, err := types.ChunkColumnFromTomy(col)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s: %w", col.GetName(), err)
		}
		chunkColumns[i] = chunkCol
	}

	return &types.ChunkResult{
		RowCount:  batch.NumRows,
		Columns:   chunkColumns,
		SelectIdx: nil,
		FilterIdx: -1,
	}, nil
}
