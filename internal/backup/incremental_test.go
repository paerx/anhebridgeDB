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
	"time"

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

func (s *fakeR2) totalPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, count := range s.putCount {
		total += count
	}
	return total
}

func (s *fakeR2) putsContaining(fragment string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for key, count := range s.putCount {
		if strings.Contains(key, fragment) {
			total += count
		}
	}
	return total
}

func (s *fakeR2) object(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, found := s.objects[key]
	return append([]byte(nil), data...), found
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

	putsAfterFirst := store.totalPuts()
	latestPutsAfterFirst := store.putsContaining("/incremental/latest.json")
	filename, _, size, lastEventID, err := manager.runBackup(context.Background())
	if err != nil {
		t.Fatalf("unchanged incremental backup: %v", err)
	}
	if filename != "" || size != 0 || lastEventID != 1 {
		t.Fatalf("unchanged backup was not skipped: file=%q size=%d event=%d", filename, size, lastEventID)
	}
	if store.totalPuts() != putsAfterFirst {
		t.Fatalf("unchanged backup issued PUTs: before=%d after=%d", putsAfterFirst, store.totalPuts())
	}
	if store.putsContaining("/incremental/latest.json") != latestPutsAfterFirst {
		t.Fatal("unchanged backup updated latest.json")
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

	putsBeforeRule := store.totalPuts()
	if _, err := engine.CreateRule(db.RuleSpec{
		ID:      "backup_rule",
		Pattern: "status:*:pending",
		Target:  "status:{id}:done",
		Delay:   "1h",
	}); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	filename, _, _, lastEventID, err = manager.runBackup(context.Background())
	if err != nil {
		t.Fatalf("backup after rule change: %v", err)
	}
	if filename == "" || lastEventID != 2 || store.totalPuts() <= putsBeforeRule {
		t.Fatalf("rule-only change did not create generation: file=%q event=%d", filename, lastEventID)
	}

	putsBeforeTask := store.totalPuts()
	bucket := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	if err := storage.SaveTaskBucket(sourceDir, bucket, []storage.Task{{
		ID:        "backup-task",
		BucketTS:  bucket,
		RuleID:    "backup_rule",
		EntityKey: "status:1",
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
	}}); err != nil {
		t.Fatalf("save task bucket: %v", err)
	}
	filename, _, _, lastEventID, err = manager.runBackup(context.Background())
	if err != nil {
		t.Fatalf("backup after task change: %v", err)
	}
	if filename == "" || lastEventID != 2 || store.totalPuts() <= putsBeforeTask {
		t.Fatalf("task-only change did not create generation: file=%q event=%d", filename, lastEventID)
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

	volumeTarget := t.TempDir()
	cfg.Backup.BootstrapRestore.Enabled = true
	cfg.Backup.BootstrapRestore.Workers = 3
	volumeReport, attempted, err := BootstrapRestoreIfEmpty(context.Background(), cfg, volumeTarget)
	if err != nil {
		t.Fatalf("bootstrap latest into existing empty volume: %v", err)
	}
	if !attempted {
		t.Fatal("expected bootstrap restore attempt")
	}
	if volumeReport.LastEventID != 2 {
		t.Fatalf("unexpected volume restore report: %+v", volumeReport)
	}
	volumeEngine, err := db.Open(volumeTarget)
	if err != nil {
		t.Fatalf("open volume-restored engine: %v", err)
	}
	defer volumeEngine.Close()
	volumeRecord, err := volumeEngine.Get("restore:key")
	if err != nil {
		t.Fatal(err)
	}
	if string(volumeRecord.Value) != `{"version":2}` {
		t.Fatalf("volume restored value = %s", volumeRecord.Value)
	}
}

func TestIncrementalBackupPrefixMigrationRepairsImmutableObjectKeys(t *testing.T) {
	store, server := newFakeR2Server(t)
	defer server.Close()

	sourceDir := t.TempDir()
	engine, err := db.Open(sourceDir)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer engine.Close()
	if _, err := engine.Set("prefix:key", json.RawMessage(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.Backup.Enabled = true
	cfg.Backup.Mode = "incremental"
	cfg.Backup.SpoolDir = filepath.Join(t.TempDir(), "spool")
	cfg.Backup.Upload.Enabled = true
	cfg.Backup.Upload.Endpoint = server.URL
	cfg.Backup.Upload.Bucket = "bucket"
	cfg.Backup.Upload.Prefix = "anhe"
	cfg.Backup.Upload.AccessKeyID = "access"
	cfg.Backup.Upload.SecretAccessKey = "secret"
	manager, err := NewManager(engine, cfg.Backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := manager.runBackup(context.Background()); err != nil {
		t.Fatalf("backup using old prefix: %v", err)
	}

	manager.uploader.prefix = "anhebackup_prod"
	statePath := filepath.Join(cfg.Backup.SpoolDir, "incremental-state.json")
	state := loadIncrementalState(statePath)
	state.RemoteTarget = manager.uploader.stateTarget()
	state.ManifestKey = "anhebackup_prod/incremental/manifests/stale.json"
	if err := saveIncrementalState(statePath, state); err != nil {
		t.Fatal(err)
	}

	filename, _, _, _, err := manager.runBackup(context.Background())
	if err != nil {
		t.Fatalf("backup after prefix migration: %v", err)
	}
	if filename == "" {
		t.Fatal("prefix migration was incorrectly skipped")
	}

	pointerData, found := store.object("bucket/anhebackup_prod/incremental/latest.json")
	if !found {
		t.Fatal("new prefix latest pointer was not uploaded")
	}
	var pointer LatestPointer
	if err := json.Unmarshal(pointerData, &pointer); err != nil {
		t.Fatal(err)
	}
	manifestData, found := store.object("bucket/" + pointer.ManifestKey)
	if !found {
		t.Fatalf("new manifest %q was not uploaded", pointer.ManifestKey)
	}
	var manifest IncrementalManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, file := range manifest.Files {
		if !file.Immutable {
			continue
		}
		if !strings.HasPrefix(file.ObjectKey, "anhebackup_prod/incremental/objects/") {
			t.Fatalf("immutable object retained stale prefix: %q", file.ObjectKey)
		}
		if _, found := store.object("bucket/" + file.ObjectKey); !found {
			t.Fatalf("immutable object %q was not uploaded", file.ObjectKey)
		}
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

func TestBootstrapTargetEmptiness(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	empty, err := bootstrapTargetIsEmpty(missing)
	if err != nil || !empty {
		t.Fatalf("missing target: empty=%t err=%v", empty, err)
	}

	target := t.TempDir()
	empty, err = bootstrapTargetIsEmpty(target)
	if err != nil || !empty {
		t.Fatalf("empty target: empty=%t err=%v", empty, err)
	}
	if err := os.WriteFile(filepath.Join(target, "existing"), []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty, err = bootstrapTargetIsEmpty(target)
	if err != nil || empty {
		t.Fatalf("non-empty target: empty=%t err=%v", empty, err)
	}
}

func TestBootstrapRejectsIncompleteActivation(t *testing.T) {
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, restoreActivationMarker), []byte("stage"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := bootstrapTargetIsEmpty(target); err == nil {
		t.Fatal("expected incomplete activation error")
	}
}

func TestActivateRestoreInsideExistingEmptyDirectory(t *testing.T) {
	target := t.TempDir()
	stage, inPlace, err := createRestoreStage(target)
	if err != nil {
		t.Fatal(err)
	}
	if !inPlace {
		t.Fatal("expected in-place staging for existing empty directory")
	}
	if err := os.MkdirAll(filepath.Join(stage, "log"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, "log", "segment_000001.anhe"), []byte("event"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := activateRestoredDirectory(stage, target, false, inPlace); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, "log", "segment_000001.anhe"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "event" {
		t.Fatalf("restored data = %q", data)
	}
	if _, err := os.Stat(filepath.Join(target, restoreActivationMarker)); !os.IsNotExist(err) {
		t.Fatalf("activation marker remains: %v", err)
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
