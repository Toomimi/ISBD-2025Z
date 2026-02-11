package tomy_file

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteVarint(w io.Writer, value uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	if _, err := w.Write(buf[:n]); err != nil {
		return fmt.Errorf("failed to write varint: %w", err)
	}
	return nil
}

func ReadVarint(r io.Reader) (uint64, error) {
	byteReader, ok := r.(io.ByteReader)
	if !ok {
		return 0, fmt.Errorf("reader does not implement io.ByteReader")
	}

	value, err := binary.ReadUvarint(byteReader)
	if err != nil {
		return 0, fmt.Errorf("failed to read varint: %w", err)
	}
	return value, nil
}
