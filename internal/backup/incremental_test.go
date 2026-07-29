package backup

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/storage"
)

type fakeR2 struct {
	mu       sync.Mutex
	objects  map[string][]byte
	putCount map[string]int
}

func newFakeR2Server(t *testing.T) (*fakeR2, *httptest.Server) {
	t.Helper()
	store := &fakeR2{
		objects:  map[string][]byte{},
		putCount: map[string]int{},
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		key := strings.TrimPrefix(request.URL.Path, "/")
		switch request.Method {
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
			store.mu.Lock()
			store.objects[key] = data
			store.putCount[key]++
			store.mu.Unlock()
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			store.mu.Lock()
			data, found := store.objects[key]
			store.mu.Unlock()
			if !found {
				http.NotFound(writer, request)
				return
			}
			writer.Header().Set("Content-Length", jsonNumber(len(data)))
			_, _ = writer.Write(data)
		case http.MethodHead:
			store.mu.Lock()
			data, found := store.objects[key]
			store.mu.Unlock()
			if !found {
				http.NotFound(writer, request)
				return
			}
			writer.Header().Set("Content-Length", jsonNumber(len(data)))
			writer.WriteHeader(http.StatusOK)
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return store, server
}

func (s *fakeR2) immutablePuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for key, count := range s.putCount {
		if strings.Contains(key, "/incremental/objects/") {
			total += count
		}
	}
	return total
}

func jsonNumber(value int) string {
	data, _ := json.Marshal(value)
	return string(data)
}

func TestIncrementalBackupReusesSegmentsAndRestoresLatest(t *testing.T) {
	store, server := newFakeR2Server(t)
	defer server.Close()

	sourceDir := t.TempDir()
	engine, err := db.Open(sourceDir)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer engine.Close()
	if _, err := engine.Set("restore:key", json.RawMessage(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Backup.Enabled = true
	cfg.Backup.Mode = "incremental"
	cfg.Backup.SpoolDir = filepath.Join(t.TempDir(), "spool")
	cfg.Backup.Upload.Enabled = true
	cfg.Backup.Upload.Endpoint = server.URL
	cfg.Backup.Upload.Bucket = "bucket"
	cfg.Backup.Upload.Prefix = "database"
	cfg.Backup.Upload.AccessKeyID = "access"
	cfg.Backup.Upload.SecretAccessKey = "secret"
	manager, err := NewManager(engine, cfg.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, lastEventID, err := manager.runBackup(context.Background()); err != nil || lastEventID != 1 {
		t.Fatalf("first incremental backup: event=%d err=%v", lastEventID, err)
	}
	if store.immutablePuts() != 1 {
		t.Fatalf("first backup immutable PUTs = %d, want 1", store.immutablePuts())
	}

	if _, err := engine.Set("restore:key", json.RawMessage(`{"version":2}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, lastEventID, err := manager.runBackup(context.Background()); err != nil || lastEventID != 2 {
		t.Fatalf("second incremental backup: event=%d err=%v", lastEventID, err)
	}
	if store.immutablePuts() != 2 {
		t.Fatalf("second backup should only upload one new segment; immutable PUTs=%d", store.immutablePuts())
	}
	if err := os.Remove(filepath.Join(cfg.Backup.SpoolDir, "incremental-state.json")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, lastEventID, err := manager.runBackup(context.Background()); err != nil || lastEventID != 2 {
		t.Fatalf("incremental backup after local state loss: event=%d err=%v", lastEventID, err)
	}
	if store.immutablePuts() != 2 {
		t.Fatalf("content-addressed segments should be reused after state loss; immutable PUTs=%d", store.immutablePuts())
	}

	restoreDir := filepath.Join(t.TempDir(), "restored")
	report, err := RestoreFromR2(context.Background(), cfg, RestoreOptions{
		TargetDir:   restoreDir,
		ManifestKey: "latest",
		Verify:      true,
		Workers:     3,
	})
	if err != nil {
		t.Fatalf("restore latest: %v", err)
	}
	if report.LastEventID != 2 || !report.Verified {
		t.Fatalf("unexpected restore report: %+v", report)
	}
	restored, err := db.Open(restoreDir)
	if err != nil {
		t.Fatalf("open restored engine: %v", err)
	}
	defer restored.Close()
	record, err := restored.Get("restore:key")
	if err != nil {
		t.Fatal(err)
	}
	if string(record.Value) != `{"version":2}` {
		t.Fatalf("restored value = %s", record.Value)
	}
}

func TestRestoreRejectsNonEmptyTargetWithoutForce(t *testing.T) {
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureRestoreTarget(target, false); err == nil {
		t.Fatal("expected non-empty target rejection")
	}
}

func TestIncrementalManifestHMACRejectsTampering(t *testing.T) {
	manifest := IncrementalManifest{
		FormatVersion: incrementalFormatVersion,
		BackupID:      "backup-1",
		LastEventID:   7,
	}
	unsigned, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	manifest.AuthTag = storage.ComputeDataAuthTag("incremental-manifest", unsigned)
	if err := verifyIncrementalManifest(manifest); err != nil {
		t.Fatalf("verify signed manifest: %v", err)
	}
	manifest.LastEventID++
	if err := verifyIncrementalManifest(manifest); err == nil {
		t.Fatal("expected tampered manifest rejection")
	}
}
