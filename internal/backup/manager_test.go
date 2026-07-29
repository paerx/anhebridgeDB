package backup

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
)

func TestManagerCreatesLocalBackup(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()
	if _, err := engine.Set("backup:test", json.RawMessage(`42`)); err != nil {
		t.Fatalf("set value: %v", err)
	}

	cfg := config.Default().Backup
	cfg.Enabled = true
	cfg.SpoolDir = filepath.Join(t.TempDir(), "spool")
	cfg.KeepLocal = true
	cfg.LocalRetentionCount = 2
	cfg.Upload.Enabled = false
	cfg.Lark.Enabled = false
	manager, err := NewManager(engine, cfg)
	if err != nil {
		t.Fatal(err)
	}
	filename, objectKey, size, lastEventID, err := manager.runBackup(context.Background())
	if err != nil {
		t.Fatalf("run backup: %v", err)
	}
	if objectKey != "" || size == 0 || lastEventID != 1 {
		t.Fatalf("unexpected backup result: object=%q size=%d event=%d", objectKey, size, lastEventID)
	}
	if _, err := os.Stat(filename); err != nil {
		t.Fatalf("backup archive missing: %v", err)
	}
	if _, err := os.Stat(filename + ".sha256"); err != nil {
		t.Fatalf("backup checksum missing: %v", err)
	}
}
