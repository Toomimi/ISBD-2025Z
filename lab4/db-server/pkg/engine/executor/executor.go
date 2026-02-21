package executor

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"isbd4/pkg/engine/planner"
	"isbd4/pkg/engine/types"
)

type Executor struct {
	tablesDir        string
	chunkSize        uint64
	maxRowsInFile    uint64
	memoryLimitBytes uint64
}

func NewExecutor(baseDir string, chunkSize uint64, maxRowsInFile uint64, memoryLimitBytes uint64) *Executor {
	tablesDir := filepath.Join(baseDir, "tables")
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		log.Fatalf("failed to create tables directory: %v", err)
	}

	return &Executor{
		tablesDir:        tablesDir,
		chunkSize:        chunkSize,
		maxRowsInFile:    maxRowsInFile,
		memoryLimitBytes: memoryLimitBytes,
	}
}

func (e *Executor) Execute(plan planner.QueryPlan) (*types.ColumnarResult, error) {
	switch p := plan.(type) {
	case *planner.CopyPlan:
		return nil, e.executeCopy(p)
	case *planner.SelectPlan:
		return e.executeSelect(p)

	default:
		return nil, fmt.Errorf("unknown plan type")
	}
}
