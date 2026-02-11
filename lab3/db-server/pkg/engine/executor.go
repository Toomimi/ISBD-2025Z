package engine

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"isbd3/pkg/metadata"
	"isbd3/pkg/tomy_file"
)

type Executor struct {
	tablesDir string
}

func NewExecutor(baseDir string) *Executor {
	tablesDir := filepath.Join(baseDir, "tables")
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		log.Fatalf("failed to create tables directory: %v", err)
	}

	return &Executor{
		tablesDir: tablesDir,
	}
}

func (e *Executor) Execute(plan QueryPlan) (*ColumnarResult, error) {
	switch p := plan.(type) {
	case *CopyPlan:
		return nil, e.executeCopy(p)
	case *SelectPlan:
		return e.executeSelect(p)
	default:
		return nil, fmt.Errorf("unknown plan type")
	}
}

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

func (e *Executor) executeCopy(p *CopyPlan) error {
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
		case "INT64":
			colBuilders[i] = &tomy_file.Int64Column{
				Name:   colDef.Name,
				Values: make([]int64, 0, numRows),
			}
		case "VARCHAR":
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
				// Column does not have mapping to table column, skip it
				continue
			}
			colDef := tableDef.Columns[tableColIdx]

			switch colDef.Type {
			case "INT64":
				val, err := strconv.ParseInt(value, 10, 64)
				if err != nil {
					return fmt.Errorf("row %d, col %s: invalid INT64", i, colDef.Name)
				}
				col := colBuilders[tableColIdx].(*tomy_file.Int64Column)
				col.Values = append(col.Values, val)
			case "VARCHAR":
				col := colBuilders[tableColIdx].(*tomy_file.VarcharColumn)
				col.Data = append(col.Data, []byte(value)...)
				col.Offsets = append(col.Offsets, uint64(len(col.Data)))
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

func (e *Executor) executeSelect(p *SelectPlan) (*ColumnarResult, error) {
	// Ensure DecRef for all files
	defer func() {
		for _, f := range p.Files {
			f.DecRef()
		}
	}()

	resColumns := make([]any, len(p.Columns))

	// Once the interface of TomyFile allowing to get only metadata of the file is
	// implemented, we can use it preallocate the result slice and avoid unnecessary
	// memory allocations in dst = append(dst, c.Values...).
	var initialCap uint64
	if p.Limit > 0 {
		initialCap = p.Limit
	}
	for i, colDef := range p.Columns {
		switch colDef.Type {
		case "INT64":
			resColumns[i] = make([]int64, 0, initialCap)
		case "VARCHAR":
			resColumns[i] = make([]string, 0, initialCap)
		}
	}

	totalRows := uint64(0)
	for _, fileEntry := range p.Files {
		if p.Limit > 0 && totalRows >= p.Limit {
			break
		}

		table, err := tomy_file.Deserialize(fileEntry.Path)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize file %s: %w", fileEntry.Path, err)
		}

		rowsToCopy := table.NumRows
		if p.Limit > 0 && totalRows+rowsToCopy > p.Limit {
			rowsToCopy = p.Limit - totalRows
		}

		for i, colDef := range p.Columns {
			col := table.Columns[i]

			switch colDef.Type {
			case "INT64":
				c := col.(*tomy_file.Int64Column)
				resColumns[i] = append(resColumns[i].([]int64), c.Values[:rowsToCopy]...)

			case "VARCHAR":
				c := col.(*tomy_file.VarcharColumn)
				dst := resColumns[i].([]string)

				for rowIdx := range rowsToCopy {
					start := uint64(0)
					if rowIdx > 0 {
						start = c.Offsets[rowIdx-1]
					}
					end := c.Offsets[rowIdx]
					dst = append(dst, string(c.Data[start:end]))
				}
				resColumns[i] = dst
			}
		}
		totalRows += rowsToCopy
	}

	return &ColumnarResult{
		RowCount: totalRows,
		Columns:  resColumns,
	}, nil
}
