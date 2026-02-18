package operators

import "isbd4/pkg/engine/types"

type Operator interface {
	Close()
	NextBatch() (*types.ChunkResult, error)
}
