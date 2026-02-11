package tomy_file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Ints compression

// ZigZagEncode int64 => uint64.
func ZigZagEncode(n int64) uint64 {
	return uint64((n << 1) ^ (n >> 63))
}

// ZigZagDecode uint64 => int64.
func ZigZagDecode(z uint64) int64 {
	return int64((z >> 1) ^ uint64((int64(z&1)<<63)>>63))
}

// CompressInt64Column compresses a slice of int64 using Delta Encoding -> ZigZag -> Varint.
func CompressInt64Column(col Int64Column) ([]byte, error) {
	// Temporary buffer for varint encoding
	tmpBuf := make([]byte, binary.MaxVarintLen64)

	var buf bytes.Buffer
	var prev int64 = 0

	for _, v := range col.Values {
		delta := v - prev
		prev = v

		zz := ZigZagEncode(delta)
		n := binary.PutUvarint(tmpBuf, zz)

		if _, err := buf.Write(tmpBuf[:n]); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// DecompressInt64Column decomresses data: Varint -> ZigZag -> Delta Decoding.
func DecompressInt64Column(data []byte, numRows uint64) (*Int64Column, error) {
	reader := bytes.NewReader(data)
	values := make([]int64, numRows)
	var prev int64 = 0

	for i := range numRows {
		zz, err := ReadVarint(reader)
		if err != nil {
			return nil, err
		}

		delta := ZigZagDecode(zz)
		val := prev + delta
		values[i] = val
		prev = val
	}
	return &Int64Column{
		Values: values,
	}, nil
}

// Varchar compression

// CompressVarcharColumn compresses offsets and data of a VarcharColumn.
// Offsets: Delta -> Varint
// Data: ZSTD
// Output: [LenCompressedOffsets(varint)][CompressedOffsets][CompressedData]
func CompressVarcharColumn(col VarcharColumn) ([]byte, error) {
	// Temporary buffer for varint encoding
	tmpBuf := make([]byte, binary.MaxVarintLen64)

	// Compress Offsets
	var offsetsBuf bytes.Buffer
	var prevOffset uint64 = 0

	for _, off := range col.Offsets {
		delta := off - prevOffset
		prevOffset = off
		// We can skip zig zag because offsets are increasing
		n := binary.PutUvarint(tmpBuf, delta)
		offsetsBuf.Write(tmpBuf[:n])
	}

	compressedOffsets := offsetsBuf.Bytes()

	// Compress Data
	var dataBuf bytes.Buffer
	enc, err := zstd.NewWriter(&dataBuf)
	if err != nil {
		return nil, err
	}
	if _, err := enc.Write(col.Data); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	compressedData := dataBuf.Bytes()

	// Combine
	var finalBuf bytes.Buffer

	// Put length of compressed offsets
	n := binary.PutUvarint(tmpBuf, uint64(len(compressedOffsets)))
	finalBuf.Write(tmpBuf[:n])

	finalBuf.Write(compressedOffsets)
	finalBuf.Write(compressedData)

	return finalBuf.Bytes(), nil
}

// DecompressVarcharColumn decompress data for Varchar.
func DecompressVarcharColumn(data []byte, numRows uint64) (*VarcharColumn, error) {
	reader := bytes.NewReader(data)

	offsetsLen, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read offsets length: %w", err)
	}

	offsetsBytes := make([]byte, offsetsLen)
	if _, err := io.ReadFull(reader, offsetsBytes); err != nil {
		return nil, fmt.Errorf("failed to read compressed offsets: %w", err)
	}

	offsetReader := bytes.NewReader(offsetsBytes)
	offsets := make([]uint64, numRows)
	var prevOffset uint64 = 0

	for i := range numRows {
		delta, err := ReadVarint(offsetReader)
		if err != nil {
			return nil, fmt.Errorf("failed to decode offset delta at row %d: %w", i, err)
		}
		val := prevOffset + delta
		offsets[i] = val
		prevOffset = val
	}

	// Reader points to the start of compressed varchar data
	compressedData := make([]byte, reader.Len())
	if _, err := io.ReadFull(reader, compressedData); err != nil {
		return nil, fmt.Errorf("failed to read compressed varchar data: %w", err)
	}

	// Decompress varchar data
	dec, err := zstd.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd reader: %w", err)
	}
	defer dec.Close()

	uncompressedData, err := io.ReadAll(dec)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress varchar data: %w", err)
	}

	return &VarcharColumn{
		Offsets: offsets,
		Data:    uncompressedData,
	}, nil
}
