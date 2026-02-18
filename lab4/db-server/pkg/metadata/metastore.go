package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Metastore struct {
	Schema   Schema            `json:"schema"`
	NameToId map[string]string `json:"name_to_id"`
	FilePath string            `json:"-"`
	Mu       sync.RWMutex      `json:"-"`
}

type MetastoreSnapshot struct {
	Files   []*FileEntry `json:"files"`
	Columns []ColumnDef  `json:"columns"`
}

func NewMetastore(dbmsBaseDir string) *Metastore {
	metastoreDir := filepath.Join(dbmsBaseDir, "ms_data")
	if err := os.MkdirAll(metastoreDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating metastore directory: %v\n", err)
		os.Exit(1)
	}

	metaFilePath := filepath.Join(metastoreDir, "metastore.json")
	ms := &Metastore{
		Schema: Schema{
			Tables: make(map[string]*TableDef),
		},
		NameToId: make(map[string]string),
		FilePath: metaFilePath,
	}

	if err := ms.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing metastore: %v\n", err)
		os.Exit(1)
	}

	return ms
}

func (m *Metastore) Load() error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	data, err := os.ReadFile(m.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return json.Unmarshal(data, m)
}

// Save persists metadata to disk. It acquires a read lock
func (m *Metastore) Save() error {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.save()
}

// Assumes lock is held
func (m *Metastore) save() error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(m.FilePath, data, 0644)
}

func (m *Metastore) CreateTable(name string, columns []ColumnDef) (string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	tableId, err := m.newTableId(name)
	if err != nil {
		return "", err
	}

	m.Schema.Tables[tableId] = &TableDef{
		Name:    name,
		Columns: columns,
		Files:   make([]*FileEntry, 0),
	}
	return tableId, m.save()
}

func (m *Metastore) GetTableByName(name string) (*TableDef, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.getTableByNameUnlocked(name)
}

func (m *Metastore) getTableByNameUnlocked(name string) (*TableDef, bool) {
	id, err := m.tableIdFromName(name)
	if err != nil {
		return nil, false
	}
	return m.getTableByIdUnlocked(id)
}

func (m *Metastore) GetTableById(id string) (*TableDef, bool) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	return m.getTableByIdUnlocked(id)
}

func (m *Metastore) getTableByIdUnlocked(id string) (*TableDef, bool) {
	t, ok := m.Schema.Tables[id]
	return t, ok
}

func (m *Metastore) DeleteTable(tableId string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	table, exists := m.getTableByIdUnlocked(tableId)
	if !exists {
		return fmt.Errorf("table %s does not exist", tableId)
	}

	for _, f := range table.Files {
		f.MarkDeleted()
	}

	delete(m.Schema.Tables, tableId)
	delete(m.NameToId, table.Name)
	return m.save()
}

func (m *Metastore) AddFile(tableName string, filePath string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	table, exists := m.getTableByNameUnlocked(tableName)
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	entry := &FileEntry{
		Path:     filePath,
		refCount: 0,
		deleted:  false,
	}
	table.Files = append(table.Files, entry)
	return m.save()
}

func (m *Metastore) GetTableSnapshot(tableName string) (*MetastoreSnapshot, error) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	table, exists := m.getTableByNameUnlocked(tableName)
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	filesSnapshot := make([]*FileEntry, 0, len(table.Files))
	for _, f := range table.Files {
		f.IncRef()
		filesSnapshot = append(filesSnapshot, f)
	}

	return &MetastoreSnapshot{
		Files:   filesSnapshot,
		Columns: table.Columns,
	}, nil
}

func (m *Metastore) GetTables() (names []string, ids []string) {
	m.Mu.RLock()
	defer m.Mu.RUnlock()
	for id, table := range m.Schema.Tables {
		names = append(names, table.Name)
		ids = append(ids, id)
	}
	return names, ids
}

// Assumes lock is held
func (m *Metastore) tableIdFromName(name string) (string, error) {
	if _, exists := m.NameToId[name]; !exists {
		return "", fmt.Errorf("table %s does not exist", name)
	}
	return m.NameToId[name], nil
}

// Assumes write lock is held
func (m *Metastore) newTableId(tableName string) (string, error) {
	id := fmt.Sprintf("%s_%d", tableName, time.Now().UnixNano())
	if _, exists := m.NameToId[tableName]; exists {
		return "", fmt.Errorf("table %s already exists", tableName)
	}
	m.NameToId[tableName] = id
	return id, nil
}
