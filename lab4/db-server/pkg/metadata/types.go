package metadata

import (
	"fmt"
	"os"
	"sync"
)

type FileEntry struct {
	Path     string     `json:"path"`
	refCount int        `json:"-"`
	deleted  bool       `json:"-"` // used to mark that no longer in Metastore
	mu       sync.Mutex `json:"-"`
}

func (f *FileEntry) IncRef() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refCount++
}

func (f *FileEntry) DecRef() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refCount--
	f.tryCleanup()
}

func (f *FileEntry) MarkDeleted() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = true
	f.tryCleanup()
}

// must be called with the mutex held
func (f *FileEntry) tryCleanup() {
	if f.deleted && f.refCount == 0 {
		err := os.Remove(f.Path)
		if err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error deleting file %s: %v\n", f.Path, err)
		}
	}
}

func FileNames(files []*FileEntry) []string {
	names := make([]string, len(files))
	for i, f := range files {
		names[i] = f.Path
	}
	return names
}

type ColumnType string

const (
	Int64Type   ColumnType = "INT64"
	VarcharType ColumnType = "VARCHAR"
)

type ColumnDef struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"` // INT64, VARCHAR
}

type TableDef struct {
	Name    string       `json:"name"`
	Columns []ColumnDef  `json:"columns"`
	Files   []*FileEntry `json:"files"`
}

type Schema struct {
	Tables map[string]*TableDef `json:"tables"`
}
