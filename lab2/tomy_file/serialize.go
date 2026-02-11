package tomy_file

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// AnyColumn interface method
func (c Int64Column) SerializeData(w io.Writer) (compressedSize int64, err error) {
	compressedData, err := CompressInt64Column(c)
	if err != nil {
		return 0, err
	}

	n, err := w.Write(compressedData)
	if err != nil {
		return 0, err
	}

	return int64(n), nil
}

// AnyColumn interface method
func (c VarcharColumn) SerializeData(w io.Writer) (compressedSize int64, err error) {
	compressedData, err := CompressVarcharColumn(c)
	if err != nil {
		return 0, err
	}

	n, err := w.Write(compressedData)
	if err != nil {
		return 0, err
	}

	return int64(n), nil
}

func (table ColumnarTable) Serialize(filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	// BeginMagic
	if _, err := f.WriteString(BeginMagic); err != nil {
		return fmt.Errorf("failed to write magic begin: %w", err)
	}

	// All columns
	numColumns := uint64(len(table.Columns))
	colMeta := make([]ColumnMetaData, 0, numColumns)

	for _, col := range table.Columns {
		offset, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get current offset for column %s: %w", col.GetName(), err)
		}

		compressedSize, err := col.SerializeData(f)
		if err != nil {
			return fmt.Errorf("failed to serialize data for column %s: %w", col.GetName(), err)
		}

		colMeta = append(colMeta, ColumnMetaData{
			Name:           col.GetName(),
			Type:           col.GetType(),
			DataOffset:     offset,
			CompressedSize: compressedSize,
		})
	}

	// Metadata
	if err := writeMetadataBlockAndOffset(f, FileMetaData{
		NumRows:    table.NumRows,
		NumColumns: numColumns,
		Columns:    colMeta,
	}); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// EndMagic
	if _, err := f.WriteString(EndMagic); err != nil {
		return fmt.Errorf("failed to write magic end: %w", err)
	}

	return nil
}

func writeMetadataBlockAndOffset(f *os.File, meta FileMetaData) error {
	metadataOffset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get metadata offset: %w", err)
	}

	// Write Metadata Block
	if err := writeMetadataVLE(f, meta.NumRows, meta.NumColumns, meta.Columns); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Write Metadata Offset
	if err := binary.Write(f, binary.LittleEndian, metadataOffset); err != nil {
		return fmt.Errorf("failed to write metadata offset: %w", err)
	}

	return nil
}

func writeMetadataVLE(w io.Writer, numRows uint64, numCols uint64, cols []ColumnMetaData) error {
	// NumRows
	if err := WriteVarint(w, numRows); err != nil {
		return err
	}
	// NumColumns
	if err := WriteVarint(w, numCols); err != nil {
		return err
	}

	for _, col := range cols {
		// Name Length + Name
		nameBytes := []byte(col.Name)
		if err := WriteVarint(w, uint64(len(nameBytes))); err != nil {
			return err
		}
		if _, err := w.Write(nameBytes); err != nil {
			return err
		}

		// Type (1 byte)
		if err := binary.Write(w, binary.LittleEndian, byte(col.Type)); err != nil {
			return err
		}

		// Data Offset (8 bytes, LE)
		if err := binary.Write(w, binary.LittleEndian, col.DataOffset); err != nil {
			return err
		}

		// Compressed Size (VLE)
		if err := WriteVarint(w, uint64(col.CompressedSize)); err != nil {
			return err
		}
	}
	return nil
}
