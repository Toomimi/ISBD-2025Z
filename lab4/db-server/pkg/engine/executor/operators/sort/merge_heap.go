package sort

import (
	"isbd4/pkg/engine/planner"
)

type MergeNode struct {
	readerKIdx int
	rows       [][]any
	nextRow    int
}

func (n *MergeNode) currentRow() []any {
	if n.nextRow < len(n.rows) {
		return n.rows[n.nextRow]
	}
	return nil
}

func (n *MergeNode) advance() bool {
	n.nextRow++
	return n.nextRow < len(n.rows)
}

type MergeHeap struct {
	nodes      []*MergeNode
	sortFields []planner.OrderByColumnReference
}

func (h *MergeHeap) Len() int { return len(h.nodes) }

func (h *MergeHeap) Swap(i, j int) { h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i] }

func (h *MergeHeap) Push(x any) {
	h.nodes = append(h.nodes, x.(*MergeNode))
}

func (h *MergeHeap) Pop() any {
	old := h.nodes
	n := len(old)
	node := old[n-1]
	h.nodes = old[0 : n-1]
	return node
}

func (h *MergeHeap) Less(i, j int) bool {
	rowI := h.nodes[i].currentRow()
	rowJ := h.nodes[j].currentRow()

	for _, sf := range h.sortFields {
		valI := rowI[sf.Index]
		valJ := rowJ[sf.Index]

		res := compareAny(valI, valJ)
		if res == 0 {
			continue
		}

		if sf.Ascending {
			return res < 0
		}
		return res > 0
	}
	return false
}

// assumes the same types for a and b
func compareAny(a, b any) int {
	switch v1 := a.(type) {
	case int64:
		v2 := b.(int64)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2 := b.(string)
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case bool:
		v1b := v1
		v2b := b.(bool)
		if v1b == v2b {
			return 0
		}
		if !v1b {
			return -1
		}
		return 1
	}
	return 0
}
