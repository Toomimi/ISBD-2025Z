package tomy_file

import "io"

// In memory data strucutres

type AnyColumn interface {
	GetName() string
	GetType() ColumnType
	GetNumRows() int
	SerializeData(w io.Writer) (compressedSize int64, err error) // implemented in serailize.go
}

type ColumnarTable struct {
	NumRows uint64
	Columns []AnyColumn
}

type Int64Column struct {
	Name   string
	Values []int64
}

func (c Int64Column) GetName() string {
	return c.Name
}

func (c Int64Column) GetType() ColumnType {
	return TypeInt64
}

func (c Int64Column) GetNumRows() int {
	return len(c.Values)
}

type VarcharColumn struct {
	Name    string
	Offsets []uint64
	Data    []byte
}

func (c VarcharColumn) GetName() string {
	return c.Name
}

func (c VarcharColumn) GetType() ColumnType {
	return TypeVarchar
}

func (c VarcharColumn) GetNumRows() int {
	return len(c.Offsets)
}

// File format constants and structures

const (
	BeginMagic = "Tomy" // 4B
	EndMagic   = "EndT" // 4B
)

type ColumnType byte

const (
	TypeInt64   ColumnType = 0x01
	TypeVarchar ColumnType = 0x02
)

type ColumnMetaData struct {
	Name           string
	Type           ColumnType
	DataOffset     int64
	CompressedSize int64
}

type FileMetaData struct {
	NumRows    uint64 // assuming there won't be more than 2^64 rows, with a single column and (2^64)-1 rows this would result in a huuuuge file.
	NumColumns uint64
	Columns    []ColumnMetaData
}
