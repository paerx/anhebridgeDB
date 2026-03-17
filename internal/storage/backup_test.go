package storage

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestExportImportFullBackup(t *testing.T) {
	src := t.TempDir()
	log, err := OpenEventLog(filepath.Join(src, "log"), DefaultSegmentOptions())
	if err != nil {
		t.Fatalf("open event log: %v", err)
	}
	if _, err := log.Append(Event{
		Timestamp: time.Now().UTC(),
		Key:       "user:1",
		Operation: "CREATE",
		NewValue:  json.RawMessage(`{"name":"tom"}`),
		Source:    "test",
		Actor:     "tester",
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}
	if err := SaveJSON(RulesPath(src), []Rule{{ID: "rule-1", Pattern: "A:*:b", Target: "A:{id}:c", Delay: "1m", Enabled: true}}); err != nil {
		t.Fatalf("save rules: %v", err)
	}

	var buf bytes.Buffer
	if err := ExportBackup(&buf, src, "full", nil); err != nil {
		t.Fatalf("export full backup: %v", err)
	}

	dest := t.TempDir()
	report, err := ImportBackup(bytes.NewReader(buf.Bytes()), dest, "full")
	if err != nil {
		t.Fatalf("import full backup: %v", err)
	}
	if !report.RebuiltPosition || !report.RebuiltLatest {
		t.Fatalf("expected rebuilt indexes after import: %+v", report)
	}
	refs, err := LoadPositionIndex(dest)
	if err != nil {
		t.Fatalf("load rebuilt position index: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	index, err := LoadKeyIndex(dest)
	if err != nil {
		t.Fatalf("load rebuilt latest index: %v", err)
	}
	entry, ok := index["user:1"]
	if !ok || entry.LatestEvent != 1 {
		t.Fatalf("unexpected latest index entry: %+v", entry)
	}
}

func TestExportImportSegmentBackup(t *testing.T) {
	src := t.TempDir()
	log, err := OpenEventLog(filepath.Join(src, "log"), SegmentOptions{MaxRecords: 100})
	if err != nil {
		t.Fatalf("open event log: %v", err)
	}
	for i, value := range []string{`1`, `2`} {
		if _, err := log.Append(Event{
			Timestamp: time.Now().UTC().Add(time.Duration(i) * time.Second),
			Key:       "balance:1",
			Operation: "UPDATE",
			NewValue:  json.RawMessage(value),
			Source:    "test",
			Actor:     "tester",
		}); err != nil {
			t.Fatalf("append event %d: %v", i+1, err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close event log: %v", err)
	}

	var buf bytes.Buffer
	if err := ExportBackup(&buf, src, "segments", []string{"segment_000001.anhe"}); err != nil {
		t.Fatalf("export segment backup: %v", err)
	}

	dest := t.TempDir()
	report, err := ImportBackup(bytes.NewReader(buf.Bytes()), dest, "segments")
	if err != nil {
		t.Fatalf("import segment backup: %v", err)
	}
	if !report.RebuiltPosition || !report.RebuiltLatest {
		t.Fatalf("expected rebuilt indexes after segment import: %+v", report)
	}
	refs, err := LoadPositionIndex(dest)
	if err != nil {
		t.Fatalf("load position index: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	index, err := LoadKeyIndex(dest)
	if err != nil {
		t.Fatalf("load latest index: %v", err)
	}
	entry, ok := index["balance:1"]
	if !ok || entry.LatestEvent != 2 {
		t.Fatalf("unexpected latest entry: %+v", entry)
	}
}
