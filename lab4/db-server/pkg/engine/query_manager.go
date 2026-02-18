package engine

import (
	"fmt"
	"isbd4/openapi"
	"isbd4/pkg/engine/executor"
	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
	"isbd4/pkg/metadata"
	"sync"
	"time"
)

type QueryState string

const (
	QueryStatePending  QueryState = "PENDING"
	QueryStatePlanning QueryState = "PLANNING"
	QueryStateRunning  QueryState = "RUNNING"
	QueryStateFinished QueryState = "FINISHED"
	QueryStateFailed   QueryState = "FAILED"
)

type QueryInfo struct {
	Id         string
	State      QueryState
	Result     *types.ColumnarResult
	Error      error
	Definition any
}

type QueryManager struct {
	Planner  *planner.Planner
	Executor *executor.Executor
	Queries  map[string]*QueryInfo
	Mu       sync.RWMutex
}

func NewQueryManager(m *metadata.Metastore, baseDir string, chunkSize uint64, maxRowsInFile uint64) *QueryManager {
	return &QueryManager{
		Planner:  planner.NewPlanner(m),
		Executor: executor.NewExecutor(baseDir, chunkSize, maxRowsInFile),
		Queries:  make(map[string]*QueryInfo),
	}
}

func (qm *QueryManager) SubmitCopy(tableName, csvPath string, columnsMapping []string, csvContainsHeader bool, queryDefinition any) (string, error) {
	plan, err := qm.Planner.PlanCopy(tableName, csvPath, columnsMapping, csvContainsHeader)
	if err != nil {
		return "", err
	}

	queryId := fmt.Sprintf("COPY_%d", time.Now().UnixNano())
	qm.createQuery(queryId, queryDefinition)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				qm.failQuery(queryId, fmt.Errorf("panic: %v", r))
			}
		}()

		qm.updateState(queryId, QueryStateRunning)

		_, err = qm.Executor.Execute(plan)
		if err != nil {
			qm.failQuery(queryId, err)
			return
		}

		qm.finishQuery(queryId, nil)
	}()

	return queryId, nil
}

func (qm *QueryManager) SubmitSelect(queryDefinition openapi.SelectQuery, qd any) (string, error) {
	plan, err := qm.Planner.PlanSelect(queryDefinition)
	if err != nil {
		return "", err
	}

	queryId := fmt.Sprintf("SELECT_%d", time.Now().UnixNano())
	qm.createQuery(queryId, qd)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				qm.failQuery(queryId, fmt.Errorf("panic: %v", r))
			}
		}()

		qm.updateState(queryId, QueryStateRunning)

		result, err := qm.Executor.Execute(plan)
		if err != nil {
			qm.failQuery(queryId, err)
			return
		}

		qm.finishQuery(queryId, result)
	}()

	return queryId, nil
}

func (qm *QueryManager) GetQueryInfo(queryId string) (*QueryInfo, bool) {
	qm.Mu.RLock()
	defer qm.Mu.RUnlock()
	q, ok := qm.Queries[queryId]
	return q, ok
}

func (qm *QueryManager) GetAllQueriesInfo() []QueryInfo {
	qm.Mu.RLock()
	defer qm.Mu.RUnlock()

	result := make([]QueryInfo, 0, len(qm.Queries))
	for _, info := range qm.Queries {
		result = append(result, *info)
	}
	return result
}

func (qm *QueryManager) GetQueryResult(queryId string, flushResult bool, rowLimit int32) (any, error) {
	qm.Mu.RLock()
	info, ok := qm.Queries[queryId]
	if !ok {
		qm.Mu.RUnlock()
		return nil, fmt.Errorf("Query was already flushed")
	}

	result := info.Result
	qm.Mu.RUnlock()

	var err error
	result, err = trimResult(result, rowLimit)
	if err != nil {
		return nil, err
	}

	if flushResult {
		qm.flushQueryResult(queryId)
	}

	return result, nil
}

func (qm *QueryManager) flushQueryResult(queryId string) {
	qm.Mu.Lock()
	defer qm.Mu.Unlock()
	delete(qm.Queries, queryId)
}

func (qm *QueryManager) createQuery(id string, definition any) {
	qm.Mu.Lock()
	defer qm.Mu.Unlock()
	qm.Queries[id] = &QueryInfo{
		Id:         id,
		State:      QueryStatePending,
		Definition: definition,
	}
}

func (qm *QueryManager) updateState(id string, state QueryState) {
	qm.Mu.Lock()
	defer qm.Mu.Unlock()
	if q, ok := qm.Queries[id]; ok {
		q.State = state
	}
}

func (qm *QueryManager) failQuery(id string, err error) {
	qm.Mu.Lock()
	defer qm.Mu.Unlock()
	if q, ok := qm.Queries[id]; ok {
		q.State = QueryStateFailed
		q.Error = err
	}
}

func (qm *QueryManager) finishQuery(id string, result *types.ColumnarResult) {
	qm.Mu.Lock()
	defer qm.Mu.Unlock()
	if q, ok := qm.Queries[id]; ok {
		q.State = QueryStateFinished
		q.Result = result
	}
}

func copySlice[T any](src []T, limit int) []T {
	dst := make([]T, limit)
	copy(dst, src[:limit])
	return dst
}

func trimResult(original *types.ColumnarResult, rowLimit int32) (*types.ColumnarResult, error) {
	if original == nil {
		return nil, nil
	}

	limit := int(original.RowCount)
	if rowLimit > 0 && uint64(rowLimit) < original.RowCount {
		limit = int(rowLimit)
	}

	newResult := &types.ColumnarResult{
		RowCount: uint64(limit),
		Columns:  make([]any, len(original.Columns)),
	}

	for i, col := range original.Columns {
		switch v := col.(type) {
		case []int64:
			newResult.Columns[i] = copySlice(v, limit)
		case []string:
			newResult.Columns[i] = copySlice(v, limit)
		case []bool:
			newResult.Columns[i] = copySlice(v, limit)
		default:
			return nil, fmt.Errorf("unsupported type: %T", col)
		}
	}
	return newResult, nil
}
