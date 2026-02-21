package sort

import (
	"encoding/gob"
	"fmt"
	"io"
	"isbd4/pkg/engine/types"
	"os"
	"path/filepath"
	"time"
)

func init() {
	gob.Register([]any{})
	gob.Register(int64(0))
	gob.Register("")
	gob.Register(bool(false))
}

type RunFilesManager struct {
	runFilesDir     string
	files           []string
	readers         []*RunReader
	readerBatchSize int
}

func NewRunFilesManager(baseDir string) *RunFilesManager {
	runFilesDir := filepath.Join(filepath.Dir(baseDir), ".sort_runs")
	os.MkdirAll(runFilesDir, 0755)

	return &RunFilesManager{
		runFilesDir: runFilesDir,
	}
}

func (rfm *RunFilesManager) close() error {
	for _, reader := range rfm.readers {
		if reader != nil {
			reader.close()
		}
	}
	rfm.readers = nil
	return os.RemoveAll(rfm.runFilesDir)
}

// whether any runs stored at disk
func (rfm *RunFilesManager) used() bool {
	return len(rfm.files) > 0
}

func (rfm *RunFilesManager) saveChunk(chunk *types.ChunkResult) error {

	path := filepath.Join(rfm.runFilesDir, fmt.Sprintf("run_%d.gob", time.Now().UnixNano()))
	writer, err := newRunWriter(path)
	if err != nil {
		return err
	}
	defer writer.close()

	if err := writer.writeChunk(chunk); err != nil {
		return err
	}

	rfm.files = append(rfm.files, path)
	return nil
}

func (rfm *RunFilesManager) openRunReadersAndReadFirstBatch(chunkSize uint64) ([]*MergeNode, error) {
	rfm.readerBatchSize = int(chunkSize) / len(rfm.files)
	var mergeHeapNodes = make([]*MergeNode, 0, len(rfm.files))
	for i, path := range rfm.files {
		r, err := newRunReader(path)
		if err != nil {
			return nil, err
		}
		rfm.readers = append(rfm.readers, r)

		rows, err := r.readBatch(rfm.readerBatchSize)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			mergeHeapNodes = append(mergeHeapNodes, &MergeNode{readerKIdx: i, rows: rows})
		}
	}
	return mergeHeapNodes, nil
}

func (rfm *RunFilesManager) readBatch(readerIdx int) ([][]any, error) {
	if rfm.readers[readerIdx].EOF() {
		return nil, nil
	}
	rows, err := rfm.readers[readerIdx].readBatch(rfm.readerBatchSize)
	return rows, err
}

type RunWriter struct {
	file    *os.File
	encoder *gob.Encoder
}

func newRunWriter(filePath string) (*RunWriter, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return &RunWriter{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (rw *RunWriter) writeRow(row []any) error {
	return rw.encoder.Encode(&row)
}

func (rw *RunWriter) writeChunk(chunk *types.ChunkResult) error {
	for i := uint64(0); i < chunk.RowCount; i++ {
		row := make([]any, len(chunk.Columns))
		for colIdx, col := range chunk.Columns {
			row[colIdx] = col.GetValueAny(int(i))
		}
		if err := rw.writeRow(row); err != nil {
			return err
		}
	}
	return nil
}

func (rw *RunWriter) close() error {
	return rw.file.Close()
}

type RunReader struct {
	file      *os.File
	decoder   *gob.Decoder
	endOfFile bool
}

func newRunReader(filePath string) (*RunReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	return &RunReader{
		file:    file,
		decoder: gob.NewDecoder(file),
	}, nil
}

func (rr *RunReader) readRow() ([]any, error) {
	var row []any
	err := rr.decoder.Decode(&row)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

func (rr *RunReader) readBatch(batchSize int) ([][]any, error) {
	if batchSize <= 0 {
		return nil, nil
	}

	var rows = make([][]any, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		row, err := rr.readRow()
		if err != nil {
			return nil, err
		}
		if row == nil {
			rr.close()
			rr.endOfFile = true
			break
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		return nil, nil
	}
	return rows, nil
}

func (rr *RunReader) EOF() bool {
	return rr.endOfFile
}

func (rr *RunReader) close() error {
	if rr.endOfFile {
		return nil
	}
	return rr.file.Close()
}
