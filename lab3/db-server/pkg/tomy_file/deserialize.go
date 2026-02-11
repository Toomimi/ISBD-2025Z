package tomy_file

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

func Deserialize(filePath string) (table *ColumnarTable, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("can't open the file: %w", err)
	}
	defer f.Close()

	// BeginMagic
	if err := verifyMagicValue(f, BeginMagic, 0); err != nil {
		return nil, err
	}

	// Metadata
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("can't get the size of the file: %w", err)
	}
	fileSize := fi.Size()

	metadata, err := readMetadata(f, fileSize)
	if err != nil {
		return nil, err
	}

	table = &ColumnarTable{
		NumRows: metadata.NumRows,
		Columns: make([]AnyColumn, 0, len(metadata.Columns)),
	}

	// All columns
	for _, colMeta := range metadata.Columns {
		compressedData, err := readColumnData(f, colMeta)
		if err != nil {
			return nil, err
		}

		var col AnyColumn

		switch colMeta.Type {
		case TypeInt64:
			decodedCol, err := DecompressInt64Column(compressedData, metadata.NumRows)
			if err != nil {
				return nil, fmt.Errorf("failed to decode INT64 column '%s': %w", colMeta.Name, err)
			}
			decodedCol.Name = colMeta.Name
			col = decodedCol

		case TypeVarchar:
			decodedCol, err := DecompressVarcharColumn(compressedData, metadata.NumRows)
			if err != nil {
				return nil, fmt.Errorf("failed to decode VARCHAR column '%s': %w", colMeta.Name, err)
			}
			decodedCol.Name = colMeta.Name
			col = decodedCol

		default:
			return nil, fmt.Errorf("unknown column type for '%s': %v", colMeta.Name, colMeta.Type)
		}

		table.Columns = append(table.Columns, col)
	}

	return table, nil
}

func verifyMagicValue(f *os.File, expectedMagic string, offset int64) error {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to %s (offset %d): %w", expectedMagic, offset, err)
	}

	magicBuffer := make([]byte, len(expectedMagic))

	if _, err := io.ReadFull(f, magicBuffer); err != nil {
		return fmt.Errorf("file is too short. Error reading %s: %w", expectedMagic, err)
	}

	if string(magicBuffer) != expectedMagic {
		return fmt.Errorf("invalid magic: expected '%s', got '%s'",
			expectedMagic, string(magicBuffer))
	}
	return nil
}

func readMetadata(f *os.File, fileSize int64) (*FileMetaData, error) {

	endMagicStart := fileSize - int64(len(EndMagic))
	if err := verifyMagicValue(f, EndMagic, endMagicStart); err != nil {
		return nil, err
	}

	// Offset of the metadata is 8 bytes before MagicEnd
	offsetPointerStart := endMagicStart - 8

	if _, err := f.Seek(offsetPointerStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("couldn't seek to metadata offset pointer: %w", err)
	}

	var metadataOffset int64
	if err := binary.Read(f, binary.LittleEndian, &metadataOffset); err != nil {
		return nil, fmt.Errorf("couldn't read the metadata offset: %w", err)
	}

	if metadataOffset < int64(len(BeginMagic)) || metadataOffset >= offsetPointerStart {
		return nil, fmt.Errorf("invalid metadata offset value: %d", metadataOffset)
	}

	metadataLength := offsetPointerStart - metadataOffset
	if metadataLength <= 0 {
		return nil, errors.New("inappropriate metadata length")
	}

	if _, err := f.Seek(metadataOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("couldn't seek to the begining of metadata: %w", err)
	}

	metadataBuffer := make([]byte, metadataLength)
	if _, err := io.ReadFull(f, metadataBuffer); err != nil {
		return nil, fmt.Errorf("error while reading metadata block: %w", err)
	}

	return deserializeMetadata(metadataBuffer)
}

func deserializeMetadata(buf []byte) (*FileMetaData, error) {
	reader := bytes.NewReader(buf)
	meta := &FileMetaData{}

	numRows, err := ReadVarint(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read NumRows: %w", err)
	}
	meta.NumRows = numRows

	numColumns, err := ReadVarint(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read NumColumns: %w", err)
	}
	meta.NumColumns = numColumns

	meta.Columns = make([]ColumnMetaData, meta.NumColumns)

	for i := 0; i < int(meta.NumColumns); i++ {
		// columnName (VLE size + bytes)
		nameLength, err := ReadVarint(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read length of column %d name: %w", i, err)
		}
		nameBuffer := make([]byte, nameLength)
		if _, err := io.ReadFull(reader, nameBuffer); err != nil {
			return nil, fmt.Errorf("failed to read column %d name: %w", i, err)
		}
		meta.Columns[i].Name = string(nameBuffer)

		var colType byte
		if err := binary.Read(reader, binary.LittleEndian, &colType); err != nil {
			return nil, fmt.Errorf("failed to read type of column %d: %w", i, err)
		}
		meta.Columns[i].Type = ColumnType(colType)

		// Data Offset (INT64, Little Endian)
		if err := binary.Read(reader, binary.LittleEndian, &meta.Columns[i].DataOffset); err != nil {
			return nil, fmt.Errorf("failed to read offset of column %d data: %w", i, err)
		}

		// Column data size (VLE)
		compressedSize, err := ReadVarint(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read size of the column %d: %w", i, err)
		}
		meta.Columns[i].CompressedSize = int64(compressedSize)
	}

	return meta, nil
}

func readColumnData(f *os.File, colMeta ColumnMetaData) ([]byte, error) {
	if _, err := f.Seek(colMeta.DataOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to column %s data: %w", colMeta.Name, err)
	}

	compressedData := make([]byte, colMeta.CompressedSize)
	if _, err := io.ReadFull(f, compressedData); err != nil {
		return nil, fmt.Errorf("failed to read column %s data: %w", colMeta.Name, err)
	}

	return compressedData, nil
}
