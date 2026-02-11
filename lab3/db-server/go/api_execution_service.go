package openapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"isbd3/pkg/engine"
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
func (s *ExecutionAPIService) GetQueries(ctx context.Context) (ImplResponse, error) {
	allInfos := s.QueryManager.GetAllQueriesInfo()

	shallowQueriesResponse := make([]ShallowQuery, 0, len(allInfos))

	for _, info := range allInfos {
		var status QueryStatus
		switch info.State {
		case engine.QueryStatePending:
			status = CREATED
		case engine.QueryStatePlanning:
			status = PLANNING
		case engine.QueryStateRunning:
			status = RUNNING
		case engine.QueryStateFinished:
			status = COMPLETED
		case engine.QueryStateFailed:
			status = FAILED
		default:
			status = FAILED
		}

		shallowQueriesResponse = append(shallowQueriesResponse, ShallowQuery{
			QueryId: info.Id,
			Status:  status,
		})
	}

	return Response(http.StatusOK, shallowQueriesResponse), nil
}

// GetQueryById - Get detailed status of selected query
func (s *ExecutionAPIService) GetQueryById(ctx context.Context, queryId string) (ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return Response(http.StatusNotFound, Error{Message: "Query not found"}), nil
	}

	var status QueryStatus
	switch info.State {
	case engine.QueryStatePending:
		status = CREATED
	case engine.QueryStatePlanning:
		status = PLANNING
	case engine.QueryStateRunning:
		status = RUNNING
	case engine.QueryStateFinished:
		status = COMPLETED
	case engine.QueryStateFailed:
		status = FAILED
	default:
		status = FAILED
	}

	return Response(http.StatusOK, Query{
		QueryId:           queryId,
		Status:            status,
		IsResultAvailable: info.State == engine.QueryStateFinished,
		QueryDefinition:   info.Definition.(QueryQueryDefinition),
	}), nil
}

// SubmitQuery - Submit new query for execution
func (s *ExecutionAPIService) SubmitQuery(ctx context.Context, req ExecuteQueryRequest) (ImplResponse, error) {
	qDef := req.QueryDefinition

	switch q := qDef.Definition.(type) {
	case SelectQuery:

		tableName := q.TableName
		if tableName == "" {
			return Response(http.StatusBadRequest, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: "TableName required in ColumnClauses for SELECT"}}}), nil
		}

		// limit was implemented by mistake, as I thought it was part of lab3. I don't remove it here
		// as it will be useful for the next lab. Thus, I pass 0 as a limit, meaning no limit.
		queryId, err := s.QueryManager.SubmitSelect(tableName, 0, req.QueryDefinition)
		if err != nil {
			return Response(http.StatusBadRequest, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: err.Error()}}}), nil
		}
		return Response(http.StatusOK, queryId), nil

	case CopyQuery:
		if q.DestinationTableName == "" || q.SourceFilepath == "" {
			return Response(http.StatusBadRequest, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: "Missing destination table or source filepath for COPY"}}}), nil
		}

		queryId, err := s.QueryManager.SubmitCopy(q.DestinationTableName, q.SourceFilepath, q.DestinationColumns, q.DoesCsvContainHeader, req.QueryDefinition)
		if err != nil {
			return Response(http.StatusBadRequest, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: err.Error()}}}), nil
		}
		return Response(http.StatusOK, queryId), nil

	default:
		return Response(http.StatusBadRequest, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: "Unknown query type"}}}), nil
	}
}

// GetQueryResult - Get result of selected query
func (s *ExecutionAPIService) GetQueryResult(ctx context.Context, queryId string, req GetQueryResultRequest) (ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return Response(http.StatusNotFound, Error{Message: "Query not found"}), nil
	}

	if strings.HasPrefix(queryId, "COPY_") {
		return Response(http.StatusBadRequest, Error{Message: "COPY queries do not return a result set"}), nil
	}

	if info.State != engine.QueryStateFinished {
		return Response(http.StatusBadRequest, Error{Message: fmt.Sprintf("Query is in state %s", info.State)}), nil
	}

	result, err := s.QueryManager.GetQueryResult(queryId, req.FlushResult, req.RowLimit)
	if err != nil {
		return Response(http.StatusInternalServerError, Error{Message: "Query result not found"}), nil
	}
	return Response(http.StatusOK, result), nil
}

// GetQueryError - Get error of selected query (will be available only for queries in FAILED state)
func (s *ExecutionAPIService) GetQueryError(ctx context.Context, queryId string) (ImplResponse, error) {
	info, exists := s.QueryManager.GetQueryInfo(queryId)
	if !exists {
		return Response(http.StatusNotFound, Error{Message: "Query not found"}), nil
	}

	if info.State != engine.QueryStateFailed {
		return Response(http.StatusBadRequest, Error{Message: "Query did not fail"}), nil
	}

	return Response(http.StatusOK, MultipleProblemsError{Problems: []MultipleProblemsErrorProblemsInner{{Error: info.Error}}}), nil
}
