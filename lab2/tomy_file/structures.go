package tomy_file

// In memory data strucutres

type ColumnarTable struct {
	NumRows int64
	// Pola przechowujące wskaźniki do konkretnych typów kolumn
	Int64Columns   []Int64Column
	VarcharColumns []VarcharColumn
}

type Int64Column struct {
	Name   string
	Values []int64
}

type VarcharColumn struct {
	Name   string
	Values []string
}

// File format constants and structures

const (
	BeginMagic        = "Tomy"            // 4B
	EndMagic          = "EndT"            // 4B
	AfterMetadataSize = 8 + len(EndMagic) // 8B for footer offset + len of EndMagic
)

// Typy kolumn
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
	NumColumns uint32
	Columns    []ColumnMetaData
}
