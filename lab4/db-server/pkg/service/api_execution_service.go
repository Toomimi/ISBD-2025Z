package service

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"isbd4/openapi"
	"isbd4/pkg/engine"
	"isbd4/pkg/engine/types"
)

// ExecutionAPIService is a service that implements the logic for the ExecutionAPIServicer
type ExecutionAPIService struct {
	QueryManager *engine.QueryManager
}

// NewExecutionAPIService creates a default api service
func NewExecutionAPIService(qm *engine.QueryManager) *ExecutionAPIService {
	return &ExecutionAPIService{QueryManager: qm}
}

// GetQueries - Get list of queries (optional in project 3, but useful). Use those IDs to get details by calling /query endpoint.
func (s *ExecutionAPIService) GetQueries(ctx context.Context) (openapi.ImplResponse, error) {
	allInfos := s.QueryManager.GetAllQueriesInfo()

	shallowQueriesResponse := make([]openapi.ShallowQuery, 0, len(allInfos))

	var stateToStatus = map[engine.QueryState]openapi.QueryStatus{
		engine.QueryStatePending:  openapi.CREATED,
		engine.QueryStatePlanning: openapi.PLANNING,
		engine.QueryStateRunning:  openapi.RUNNING,
		engine.QueryStateFinished: openapi.COMPLETED,
		engine.QueryStateFailed:   openapi.FAILED,
	}

	for _, info := range allInfos {
		shallowQueriesResponse = append(shallowQueriesResponse, openapi.ShallowQuery{
			QueryId: info.Id,
			Status:  stateToStatus[info.State],
		})
	}

	return openapi.Response(http.StatusOK, shallowQueriesResponse), nil
}

// GetQueryById - Get detailed status of selected query
func (s *ExecutionAPIService) GetQueryById(ctx context.Context, queryId string) (openapi.ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return openapi.Response(http.StatusNotFound, openapi.Error{Message: "Query not found"}), nil
	}

	var stateToStatus = map[engine.QueryState]openapi.QueryStatus{
		engine.QueryStatePending:  openapi.CREATED,
		engine.QueryStatePlanning: openapi.PLANNING,
		engine.QueryStateRunning:  openapi.RUNNING,
		engine.QueryStateFinished: openapi.COMPLETED,
		engine.QueryStateFailed:   openapi.FAILED,
	}

	return openapi.Response(http.StatusOK, openapi.Query{
		QueryId:           queryId,
		Status:            stateToStatus[info.State],
		IsResultAvailable: info.State == engine.QueryStateFinished,
		QueryDefinition:   info.Definition.(openapi.QueryQueryDefinition),
	}), nil
}

// SubmitQuery - Submit new query for execution
func (s *ExecutionAPIService) SubmitQuery(ctx context.Context, req openapi.ExecuteQueryRequest) (openapi.ImplResponse, error) {
	qDef := req.QueryDefinition

	switch q := qDef.Definition.(type) {
	case openapi.SelectQuery:
		if len(q.ColumnClauses) == 0 {
			return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{Problems: []openapi.MultipleProblemsErrorProblemsInner{{Error: "No columns specified for SELECT"}}}), nil
		}

		queryId, err := s.QueryManager.SubmitSelect(q, req.QueryDefinition)
		if err != nil {
			return openapi.Response(http.StatusBadRequest, types.ToOpenApiError(err)), nil
		}
		return openapi.Response(http.StatusOK, queryId), nil

	case openapi.CopyQuery:
		if q.DestinationTableName == "" || q.SourceFilepath == "" {
			return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{Problems: []openapi.MultipleProblemsErrorProblemsInner{{Error: "Missing destination table or source filepath for COPY"}}}), nil
		}

		queryId, err := s.QueryManager.SubmitCopy(q.DestinationTableName, q.SourceFilepath, q.DestinationColumns, q.DoesCsvContainHeader, req.QueryDefinition)
		if err != nil {
			return openapi.Response(http.StatusBadRequest, types.ToOpenApiError(err)), nil
		}
		return openapi.Response(http.StatusOK, queryId), nil

	default:
		return openapi.Response(http.StatusBadRequest, openapi.MultipleProblemsError{Problems: []openapi.MultipleProblemsErrorProblemsInner{{Error: "Unknown query type"}}}), nil
	}
}

// GetQueryResult - Get result of selected query
func (s *ExecutionAPIService) GetQueryResult(ctx context.Context, queryId string, req openapi.GetQueryResultRequest) (openapi.ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return openapi.Response(http.StatusNotFound, openapi.Error{Message: "Query not found"}), nil
	}

	if strings.HasPrefix(queryId, "COPY_") {
		return openapi.Response(http.StatusBadRequest, openapi.Error{Message: "COPY queries do not return a result set"}), nil
	}

	if info.State != engine.QueryStateFinished {
		return openapi.Response(http.StatusBadRequest, openapi.Error{Message: fmt.Sprintf("Query is in state %s", info.State)}), nil
	}

	result, err := s.QueryManager.GetQueryResult(queryId, req.FlushResult, req.RowLimit)
	if err != nil {
		return openapi.Response(http.StatusInternalServerError, openapi.Error{Message: "Query result not found"}), nil
	}
	return openapi.Response(http.StatusOK, []any{result}), nil
}

// GetQueryError - Get error of selected query
func (s *ExecutionAPIService) GetQueryError(ctx context.Context, queryId string) (openapi.ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return openapi.Response(http.StatusNotFound, openapi.Error{Message: "Query not found"}), nil
	}

	if info.State != engine.QueryStateFailed {
		return openapi.Response(http.StatusBadRequest, openapi.Error{Message: "Query did not fail"}), nil
	}

	return openapi.Response(http.StatusOK, types.ToOpenApiError(info.Error)), nil
}
