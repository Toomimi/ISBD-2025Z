package metadata

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMetastore_Persistence(t *testing.T) {
	tmpDir := t.TempDir()

	m1 := NewMetastore(tmpDir)
	cols := []ColumnDef{
		{Name: "id", Type: Int64Type},
		{Name: "name", Type: VarcharType},
	}
	tableId, err := m1.CreateTable("users", cols)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	m2 := NewMetastore(tmpDir)

	table, ok := m2.GetTableById(tableId)
	if !ok {
		t.Fatalf("Expected table with id %s to exist", tableId)
	}
	if table.Name != "users" {
		t.Errorf("Expected name 'users', got %s", table.Name)
	}
	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
}

func TestMetastore_DeleteTable(t *testing.T) {
	tmpDir := t.TempDir()

	m := NewMetastore(tmpDir)
	tableId, err := m.CreateTable("t1", []ColumnDef{{Name: "a", Type: Int64Type}})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	dummyFile := filepath.Join(tmpDir, "dummy.tomy")
	err = os.WriteFile(dummyFile, []byte("data"), 0644)
	if err != nil {
		t.Fatalf("Failed to create dummy file: %v", err)
	}

	err = m.AddFile("t1", dummyFile)
	if err != nil {
		t.Fatalf("AddFile failed: %v", err)
	}

	if _, err := os.Stat(dummyFile); os.IsNotExist(err) {
		t.Fatalf("Dummy file should exist before deletion")
	}

	err = m.DeleteTable(tableId)
	if err != nil {
		t.Fatalf("DeleteTable failed: %v", err)
	}

	m2 := NewMetastore(tmpDir)
	if _, ok := m2.GetTableById(tableId); ok {
		t.Errorf("Table t1 should be deleted")
	}

	if _, err := os.Stat(dummyFile); !os.IsNotExist(err) {
		t.Errorf("Dummy file should have been deleted, got err: %v", err)
	}
}
