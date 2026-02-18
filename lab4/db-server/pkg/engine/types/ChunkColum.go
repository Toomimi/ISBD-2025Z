package types

import (
	"fmt"
	"isbd4/pkg/metadata"
	"isbd4/pkg/tomy_file"
)

type ChunkColumn interface {
	GetType() ChunkColumnType
	GetName() string
	GetAnyRepr() any
	CopyTo(other ChunkColumn, rowOffset int)
}

func ChunkColumnFromTomy(tomyCol tomy_file.AnyColumn) (ChunkColumn, error) {
	switch col := tomyCol.(type) {
	case *tomy_file.Int64Column:
		return &Int64ChunkColumn{
			Name:   col.GetName(),
			Values: col.Values,
		}, nil
	case *tomy_file.VarcharColumn:
		return &VarcharChunkColumn{
			Name:    col.GetName(),
			Offsets: col.Offsets,
			Data:    col.Data,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported tomy column type: %T", tomyCol)
	}
}

type ChunkColumnType int

const (
	ChunkColumnTypeInt64 ChunkColumnType = iota
	ChunkColumnTypeVarchar
	ChunkColumnTypeBoolean
)

func ChunkColumnTypeFromMetadataColumnType(colType metadata.ColumnType) (ChunkColumnType, error) {
	switch colType {
	case metadata.Int64Type:
		return ChunkColumnTypeInt64, nil
	case metadata.VarcharType:
		return ChunkColumnTypeVarchar, nil
	default:
		return -1, fmt.Errorf("couldn't resolve chunk column type from metadata column type: %v", colType)
	}
}

type Int64ChunkColumn struct {
	Name   string
	Values []int64
}

func (c *Int64ChunkColumn) GetType() ChunkColumnType { return ChunkColumnTypeInt64 }
func (c *Int64ChunkColumn) GetName() string          { return c.Name }
func (c *Int64ChunkColumn) GetAnyRepr() any          { return c.Values }

func (c *Int64ChunkColumn) CopyTo(other ChunkColumn, rowOffset int) {
	target := other.(*Int64ChunkColumn)
	copy(target.Values[rowOffset:], c.Values)
}

func NewInt64Column(name string, values []int64) *Int64ChunkColumn {
	return &Int64ChunkColumn{
		Name:   name,
		Values: values,
	}
}

type BooleanChunkColumn struct {
	Name   string
	Values []bool
}

func (c *BooleanChunkColumn) GetType() ChunkColumnType { return ChunkColumnTypeBoolean }
func (c *BooleanChunkColumn) GetName() string          { return c.Name }
func (c *BooleanChunkColumn) GetAnyRepr() any          { return c.Values }

func (c *BooleanChunkColumn) CopyTo(other ChunkColumn, rowOffset int) {
	target := other.(*BooleanChunkColumn)
	copy(target.Values[rowOffset:], c.Values)
}

func NewBooleanColumn(name string, values []bool) *BooleanChunkColumn {
	return &BooleanChunkColumn{
		Name:   name,
		Values: values,
	}
}

type VarcharChunkColumn struct {
	Name    string
	Offsets []uint64
	Data    []byte
}

func (c *VarcharChunkColumn) GetType() ChunkColumnType { return ChunkColumnTypeVarchar }
func (c *VarcharChunkColumn) GetName() string          { return c.Name }
func (c *VarcharChunkColumn) GetAnyRepr() any          { return c.GetValuesAsString() }

func (c *VarcharChunkColumn) CopyTo(other ChunkColumn, rowOffset int) {
	target := other.(*VarcharChunkColumn)

	oldLen := len(target.Data)
	for i, off := range c.Offsets {
		target.Offsets[rowOffset+i] = off + uint64(oldLen)
	}
	target.Data = append(target.Data, c.Data...)
}

func (c *VarcharChunkColumn) GetValuesAsString() []string {
	res := make([]string, len(c.Offsets))
	for i := 0; i < len(c.Offsets); i++ {
		start := c.Offsets[i]
		end := c.NextOffset(i)
		res[i] = string(c.Data[start:end])
	}
	return res
}

func (c *VarcharChunkColumn) NextOffset(idx int) uint64 {
	if idx == len(c.Offsets)-1 {
		return uint64(len(c.Data))
	}
	return c.Offsets[idx+1]
}

func VarcharChunkColumnFromStrings(name string, values []string) *VarcharChunkColumn {
	totalSize := 0
	for _, str := range values {
		totalSize += len(str)
	}
	dataBytes := make([]byte, totalSize)
	offsets := make([]uint64, len(values))
	var currentOffset uint64
	for i, str := range values {
		offsets[i] = currentOffset
		copy(dataBytes[currentOffset:], []byte(str))
		currentOffset += uint64(len(str))
	}
	return &VarcharChunkColumn{Name: name, Offsets: offsets, Data: dataBytes}
}

func CloneEmpty(c ChunkColumn, capacity, maxDataSize int) ChunkColumn {
	switch c.GetType() {
	case ChunkColumnTypeInt64:
		return &Int64ChunkColumn{
			Name:   c.GetName(),
			Values: make([]int64, capacity),
		}
	case ChunkColumnTypeVarchar:
		return &VarcharChunkColumn{
			Name:    c.GetName(),
			Offsets: make([]uint64, capacity),
			Data:    make([]byte, 0, maxDataSize), // Len set to 0, used to know where to start copying
		}
	case ChunkColumnTypeBoolean:
		return &BooleanChunkColumn{
			Name:   c.GetName(),
			Values: make([]bool, capacity),
		}
	default:
		panic("unsupported chunk column type")
	}
}
