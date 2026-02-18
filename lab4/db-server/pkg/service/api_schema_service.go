package service

import (
	"context"
	"net/http"

	"isbd4/openapi"
	"isbd4/pkg/metadata"
)

// SchemaAPIService is a service that implements the logic for the SchemaAPIServicer
// This service should implement the business logic for every endpoint for the SchemaAPI API.
// Include any external packages or services that will be required by this service.
type SchemaAPIService struct {
	metastore *metadata.Metastore
}

// NewSchemaAPIService creates a default api service
func NewSchemaAPIService(m *metadata.Metastore) *SchemaAPIService {
	return &SchemaAPIService{
		metastore: m,
	}
}

// GetTables - Get list of tables with their accompanying IDs. Use those IDs to get details by calling /table endpoint.
func (s *SchemaAPIService) GetTables(ctx context.Context) (openapi.ImplResponse, error) {
	names, ids := s.metastore.GetTables()

	tables := []openapi.ShallowTable{}
	for i := range names {
		tables = append(tables, openapi.ShallowTable{
			TableId: ids[i],
			Name:    names[i],
		})
	}

	return openapi.Response(http.StatusOK, tables), nil
}

// GetTableById - Get detailed description of selected table
func (s *SchemaAPIService) GetTableById(ctx context.Context, tableId string) (openapi.ImplResponse, error) {
	tableDef, exists := s.metastore.GetTableById(tableId)
	if !exists {
		return openapi.Response(http.StatusNotFound, openapi.Error{Message: "Table not found"}), nil
	}

	cols := []openapi.Column{}
	for _, c := range tableDef.Columns {
		cols = append(cols, openapi.Column{Name: c.Name, Type: openapi.LogicalColumnType(c.Type)})
	}

	return openapi.Response(http.StatusOK, openapi.TableSchema{
		Name:    tableDef.Name,
		Columns: cols,
	}), nil
}

// DeleteTable - Delete selected table from database
func (s *SchemaAPIService) DeleteTable(ctx context.Context, tableId string) (openapi.ImplResponse, error) {
	err := s.metastore.DeleteTable(tableId)
	if err != nil {
		return openapi.Response(http.StatusNotFound, openapi.Error{Message: err.Error()}), nil
	}

	return openapi.Response(http.StatusOK, nil), nil
}

// CreateTable - Create new table in database
func (s *SchemaAPIService) CreateTable(ctx context.Context, tableSchema openapi.TableSchema) (openapi.ImplResponse, error) {
	if len(tableSchema.Columns) == 0 {
		return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{
			Problems: []openapi.MultipleProblemsErrorProblemsInner{
				{Error: "Table must have at least one column"},
			},
		}), nil
	}

	cols := []metadata.ColumnDef{}
	seenColumns := make(map[string]bool)
	problems := []openapi.MultipleProblemsErrorProblemsInner{}

	for _, c := range tableSchema.Columns {
		if !c.Type.IsValid() {
			problems = append(problems, openapi.MultipleProblemsErrorProblemsInner{
				Error: "Invalid column type: " + string(c.Type),
			})
		}
		if seenColumns[c.Name] {
			problems = append(problems, openapi.MultipleProblemsErrorProblemsInner{
				Error: "Duplicate column name: " + c.Name,
			})
		}
		seenColumns[c.Name] = true
		cols = append(cols, metadata.ColumnDef{Name: c.Name, Type: metadata.ColumnType(c.Type)})
	}

	if len(problems) > 0 {
		return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{
			Problems: problems,
		}), nil
	}

	tableId, err := s.metastore.CreateTable(tableSchema.Name, cols)
	if err != nil {
		return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{
			Problems: []openapi.MultipleProblemsErrorProblemsInner{
				{Error: err.Error()},
			},
		}), nil
	}

	return openapi.Response(http.StatusOK, tableId), nil
}
