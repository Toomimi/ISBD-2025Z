package executor

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"isbd4/pkg/engine/planner"
	"isbd4/pkg/metadata"
	"isbd4/pkg/tomy_file"
)

func readCSV(path string, hasHeader bool) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	reader := csv.NewReader(f)

	if hasHeader {
		_, err := reader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV header: %w", err)
		}
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV: %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("empty CSV, no data imported")
	}

	return records, nil
}

func createCsvToTableMap(columnsMapping []string, schemaColumns []metadata.ColumnDef) (map[int]int, error) {
	csvToTableMap := make(map[int]int)
	if columnsMapping == nil {
		for i := range schemaColumns {
			csvToTableMap[i] = i
		}
		return csvToTableMap, nil
	}

	tableColToIndex := make(map[string]int)
	for i, col := range schemaColumns {
		tableColToIndex[col.Name] = i
	}

	for csvIdx, colName := range columnsMapping {
		if targetIdx, ok := tableColToIndex[colName]; ok {
			csvToTableMap[csvIdx] = targetIdx
		} else {
			return nil, fmt.Errorf("column %s from CSV mapping not found in table definition", colName)
		}
	}
	return csvToTableMap, nil
}

func (e *Executor) executeCopy(p *planner.CopyPlan) error {
	tableDef, exists := p.Metastore.GetTableByName(p.TableName)
	if !exists {
		return fmt.Errorf("table %s does not exist", p.TableName)
	}

	records, err := readCSV(p.CsvFilePath, p.CsvContainsHeader)
	if err != nil {
		return err
	}

	csvToTableMap, err := createCsvToTableMap(p.ColumnsMapping, tableDef.Columns)
	if err != nil {
		return err
	}

	numRows := uint64(len(records))

	colBuilders := make([]any, len(tableDef.Columns))

	for i, colDef := range tableDef.Columns {
		switch colDef.Type {
		case metadata.Int64Type:
			colBuilders[i] = &tomy_file.Int64Column{
				Name:   colDef.Name,
				Values: make([]int64, 0, numRows),
			}
		case metadata.VarcharType:
			colBuilders[i] = &tomy_file.VarcharColumn{
				Name:    colDef.Name,
				Offsets: make([]uint64, 0, numRows),
				Data:    make([]byte, 0),
			}
		default:
			return fmt.Errorf("unknown column type: %s", colDef.Type)
		}
	}

	for i, record := range records {
		if len(record) != len(tableDef.Columns) {
			return fmt.Errorf("row %d has %d columns, expected %d", i, len(record), len(tableDef.Columns))
		}

		for csvColIdx, value := range record {
			tableColIdx, ok := csvToTableMap[csvColIdx]
			if !ok {
				continue
			}
			colDef := tableDef.Columns[tableColIdx]

			switch colDef.Type {
			case metadata.Int64Type:
				val, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return fmt.Errorf("row %d, col %s: invalid INT64", i, colDef.Name)
				}
				col := colBuilders[tableColIdx].(*tomy_file.Int64Column)
				col.Values = append(col.Values, val)
			case metadata.VarcharType:
				col := colBuilders[tableColIdx].(*tomy_file.VarcharColumn)
				col.Offsets = append(col.Offsets, uint64(len(col.Data)))
				col.Data = append(col.Data, []byte(value)...)
			}
		}
	}

	columnarTable := tomy_file.ColumnarTable{
		NumRows: numRows,
		Columns: make([]tomy_file.AnyColumn, len(colBuilders)),
	}

	for i, builder := range colBuilders {
		if col, ok := builder.(tomy_file.AnyColumn); ok {
			columnarTable.Columns[i] = col
		} else {
			return fmt.Errorf("failed to cast column builder to AnyColumn")
		}
	}

	fileName := fmt.Sprintf("%s_%d.tomy", p.TableName, time.Now().UnixNano())
	outPath := filepath.Join(e.tablesDir, fileName)

	if err := columnarTable.Serialize(outPath); err != nil {
		return fmt.Errorf("failed to serialize data: %w", err)
	}

	if err := p.Metastore.AddFile(p.TableName, outPath); err != nil {
		os.Remove(outPath)
		fmt.Println("Warning: copy finished, but could not add file to metastore. Error: ", err)
		return fmt.Errorf("table %s was removed during import", p.TableName)
	}

	return nil
}
