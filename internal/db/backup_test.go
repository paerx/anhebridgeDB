package db

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/paerx/anhebridgedb/internal/storage"
)

func TestBackupViewRestoresToEventCut(t *testing.T) {
	source := t.TempDir()
	engine, err := Open(source)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("backup:key", json.RawMessage(`{"value":1}`)); err != nil {
		t.Fatalf("set initial value: %v", err)
	}
	view, err := engine.CreateBackupView(t.TempDir())
	if err != nil {
		t.Fatalf("create backup view: %v", err)
	}
	defer view.Remove()
	if view.LastEventID != 1 {
		t.Fatalf("last event ID = %d, want 1", view.LastEventID)
	}

	// Writes continue in the new active segment and must not leak into the view.
	if _, err := engine.Set("backup:key", json.RawMessage(`{"value":2}`)); err != nil {
		t.Fatalf("set post-cut value: %v", err)
	}

	var archive bytes.Buffer
	if err := storage.ExportBackup(&archive, view.Path, "full", nil); err != nil {
		t.Fatalf("export backup view: %v", err)
	}
	destination := t.TempDir()
	if _, err := storage.ImportBackup(bytes.NewReader(archive.Bytes()), destination, "full"); err != nil {
		t.Fatalf("import backup: %v", err)
	}
	restored, err := Open(destination)
	if err != nil {
		t.Fatalf("open restored database: %v", err)
	}
	defer restored.Close()

	record, err := restored.Get("backup:key")
	if err != nil {
		t.Fatalf("get restored value: %v", err)
	}
	if string(record.Value) != `{"value":1}` {
		t.Fatalf("restored value = %s, want cut value", record.Value)
	}
}
