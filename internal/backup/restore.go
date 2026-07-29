package backup

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/storage"
)

type RestoreOptions struct {
	TargetDir   string
	ManifestKey string
	Force       bool
	Verify      bool
	Workers     int
}

type RestoreReport struct {
	BackupID      string    `json:"backup_id"`
	ManifestKey   string    `json:"manifest_key"`
	LastEventID   uint64    `json:"last_event_id"`
	RestoredFiles int       `json:"restored_files"`
	RestoredBytes int64     `json:"restored_bytes"`
	TargetDir     string    `json:"target_dir"`
	PreviousData  string    `json:"previous_data,omitempty"`
	Verified      bool      `json:"verified"`
	CompletedAt   time.Time `json:"completed_at"`
}

func RestoreFromR2(ctx context.Context, cfg config.Config, options RestoreOptions) (RestoreReport, error) {
	if strings.TrimSpace(options.TargetDir) == "" {
		return RestoreReport{}, fmt.Errorf("restore target directory is required")
	}
	client, err := newR2Uploader(cfg.Backup.Upload)
	if err != nil {
		return RestoreReport{}, err
	}
	if options.Workers <= 0 {
		options.Workers = 4
	}
	if options.Workers > 32 {
		options.Workers = 32
	}

	manifestKey := strings.TrimSpace(options.ManifestKey)
	var expectedManifestHash string
	if manifestKey == "" || strings.EqualFold(manifestKey, "latest") {
		latestKey := client.prefixedKey(filepath.ToSlash(filepath.Join("incremental", "latest.json")))
		data, err := client.getBytes(ctx, latestKey, 1024*1024)
		if err != nil {
			return RestoreReport{}, err
		}
		var pointer LatestPointer
		if err := json.Unmarshal(data, &pointer); err != nil {
			return RestoreReport{}, fmt.Errorf("decode latest backup pointer: %w", err)
		}
		if pointer.FormatVersion != incrementalFormatVersion || pointer.ManifestKey == "" {
			return RestoreReport{}, fmt.Errorf("latest backup pointer is invalid")
		}
		if err := verifyLatestPointer(pointer); err != nil {
			return RestoreReport{}, err
		}
		manifestKey = pointer.ManifestKey
		expectedManifestHash = pointer.ManifestSHA256
	}

	manifestBytes, err := client.getBytes(ctx, manifestKey, 64*1024*1024)
	if err != nil {
		return RestoreReport{}, err
	}
	if expectedManifestHash != "" {
		sum := sha256.Sum256(manifestBytes)
		if !strings.EqualFold(hex.EncodeToString(sum[:]), expectedManifestHash) {
			return RestoreReport{}, fmt.Errorf("incremental manifest SHA-256 mismatch")
		}
	}
	var manifest IncrementalManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return RestoreReport{}, fmt.Errorf("decode incremental manifest: %w", err)
	}
	if err := validateIncrementalManifest(manifest); err != nil {
		return RestoreReport{}, err
	}
	if err := verifyIncrementalManifest(manifest); err != nil {
		return RestoreReport{}, err
	}

	target, err := filepath.Abs(options.TargetDir)
	if err != nil {
		return RestoreReport{}, err
	}
	if err := ensureRestoreTarget(target, options.Force); err != nil {
		return RestoreReport{}, err
	}
	stage, inPlace, err := createRestoreStage(target)
	if err != nil {
		return RestoreReport{}, err
	}
	defer func() { _ = os.RemoveAll(stage) }()

	restoredBytes, err := restoreManifestFiles(ctx, client, stage, manifest.Files, options.Workers)
	if err != nil {
		return RestoreReport{}, err
	}
	refs, err := storage.RebuildPositionIndex(stage)
	if err != nil {
		return RestoreReport{}, fmt.Errorf("rebuild restored position index: %w", err)
	}
	if err := storage.RebuildLatestIndex(stage); err != nil {
		return RestoreReport{}, fmt.Errorf("rebuild restored latest index: %w", err)
	}
	if maxEventID(refs) != manifest.LastEventID {
		return RestoreReport{}, fmt.Errorf(
			"restored event boundary mismatch: got %d want %d",
			maxEventID(refs), manifest.LastEventID,
		)
	}

	if options.Verify {
		storageCfg := cfg.Storage
		storageCfg.StrictRecovery = true
		engine, err := db.OpenWithStorageConfig(stage, storageCfg, cfg.Performance)
		if err != nil {
			return RestoreReport{}, fmt.Errorf("verify restored database: %w", err)
		}
		if err := engine.Close(); err != nil {
			return RestoreReport{}, fmt.Errorf("close restored database verification: %w", err)
		}
	}

	previous, err := activateRestoredDirectory(stage, target, options.Force, inPlace)
	if err != nil {
		return RestoreReport{}, err
	}
	stage = ""
	return RestoreReport{
		BackupID:      manifest.BackupID,
		ManifestKey:   manifestKey,
		LastEventID:   manifest.LastEventID,
		RestoredFiles: len(manifest.Files),
		RestoredBytes: restoredBytes,
		TargetDir:     target,
		PreviousData:  previous,
		Verified:      options.Verify,
		CompletedAt:   time.Now().UTC(),
	}, nil
}

func validateIncrementalManifest(manifest IncrementalManifest) error {
	if manifest.FormatVersion != incrementalFormatVersion {
		return fmt.Errorf("unsupported incremental manifest version: %d", manifest.FormatVersion)
	}
	if manifest.BackupID == "" {
		return fmt.Errorf("incremental manifest backup_id is required")
	}
	if len(manifest.Files) > 1_000_000 {
		return fmt.Errorf("incremental manifest has too many files: %d", len(manifest.Files))
	}
	seen := make(map[string]struct{}, len(manifest.Files))
	for _, file := range manifest.Files {
		if _, err := safeRestorePath(file.Path); err != nil {
			return err
		}
		if file.ObjectKey == "" || file.SizeBytes < 0 || len(file.SHA256) != 64 {
			return fmt.Errorf("invalid incremental file entry: %s", file.Path)
		}
		if _, err := hex.DecodeString(file.SHA256); err != nil {
			return fmt.Errorf("invalid SHA-256 for incremental file: %s", file.Path)
		}
		if _, exists := seen[file.Path]; exists {
			return fmt.Errorf("duplicate incremental file path: %s", file.Path)
		}
		seen[file.Path] = struct{}{}
	}
	return nil
}

func verifyLatestPointer(pointer LatestPointer) error {
	actual := pointer.AuthTag
	pointer.AuthTag = ""
	unsigned, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return err
	}
	expected := storage.ComputeDataAuthTag("incremental-latest", unsigned)
	if actual == "" || !hmac.Equal([]byte(actual), []byte(expected)) {
		return fmt.Errorf("latest backup pointer HMAC mismatch")
	}
	return nil
}

func verifyIncrementalManifest(manifest IncrementalManifest) error {
	actual := manifest.AuthTag
	manifest.AuthTag = ""
	unsigned, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	expected := storage.ComputeDataAuthTag("incremental-manifest", unsigned)
	if actual == "" || !hmac.Equal([]byte(actual), []byte(expected)) {
		return fmt.Errorf("incremental manifest HMAC mismatch")
	}
	return nil
}

func restoreManifestFiles(
	ctx context.Context,
	client *r2Uploader,
	stage string,
	files []IncrementalManifestFile,
	workers int,
) (int64, error) {
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan IncrementalManifestFile)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	var total int64
	var totalMu sync.Mutex

	worker := func() {
		defer wg.Done()
		for file := range jobs {
			if workCtx.Err() != nil {
				return
			}
			relative, err := safeRestorePath(file.Path)
			if err == nil {
				err = restoreOneFile(workCtx, client, stage, relative, file)
			}
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				errMu.Unlock()
				return
			}
			totalMu.Lock()
			total += file.SizeBytes
			totalMu.Unlock()
		}
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}
sendLoop:
	for _, file := range files {
		select {
		case <-workCtx.Done():
			break sendLoop
		case jobs <- file:
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return 0, firstErr
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return total, nil
}

func restoreOneFile(
	ctx context.Context,
	client *r2Uploader,
	stage string,
	relative string,
	entry IncrementalManifestFile,
) error {
	target := filepath.Join(stage, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	partial := target + ".partial"
	file, err := os.OpenFile(partial, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	maxDownload := entry.SizeBytes
	if maxDownload == 0 {
		maxDownload = 1
	}
	downloadErr := client.download(ctx, entry.ObjectKey, file, maxDownload)
	if syncErr := file.Sync(); downloadErr == nil {
		downloadErr = syncErr
	}
	if closeErr := file.Close(); downloadErr == nil {
		downloadErr = closeErr
	}
	if downloadErr != nil {
		_ = os.Remove(partial)
		return downloadErr
	}
	info, err := os.Stat(partial)
	if err != nil {
		return err
	}
	if info.Size() != entry.SizeBytes {
		_ = os.Remove(partial)
		return fmt.Errorf("restored size mismatch for %s: got %d want %d", relative, info.Size(), entry.SizeBytes)
	}
	hash, err := fileSHA256(partial)
	if err != nil {
		return err
	}
	if !strings.EqualFold(hash, entry.SHA256) {
		_ = os.Remove(partial)
		return fmt.Errorf("restored SHA-256 mismatch for %s", relative)
	}
	if err := os.Rename(partial, target); err != nil {
		return err
	}
	return nil
}

func safeRestorePath(raw string) (string, error) {
	normalized := filepath.ToSlash(strings.TrimSpace(raw))
	cleaned := filepath.ToSlash(filepath.Clean(filepath.FromSlash(normalized)))
	if normalized == "" || cleaned == "." || filepath.IsAbs(filepath.FromSlash(normalized)) ||
		cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("unsafe restore path: %q", raw)
	}
	return cleaned, nil
}

func ensureRestoreTarget(target string, force bool) error {
	entries, err := os.ReadDir(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 && !force {
		return fmt.Errorf("restore target is not empty; stop the server and pass -force to preserve and replace it")
	}
	return nil
}

func createRestoreStage(target string) (string, bool, error) {
	entries, err := os.ReadDir(target)
	if err == nil && len(entries) == 0 {
		stage, err := os.MkdirTemp(target, ".anhe-restore-")
		return stage, true, err
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", false, err
	}
	stage, err := os.MkdirTemp(parent, "."+filepath.Base(target)+".restore-")
	return stage, false, err
}

func activateRestoredDirectory(stage, target string, force, inPlace bool) (string, error) {
	if inPlace {
		return "", activateRestoreInPlace(stage, target)
	}
	var previous string
	if _, err := os.Stat(target); err == nil {
		entries, readErr := os.ReadDir(target)
		if readErr != nil {
			return "", readErr
		}
		if len(entries) == 0 {
			if err := os.Remove(target); err != nil {
				return "", err
			}
		} else {
			if !force {
				return "", fmt.Errorf("restore target became non-empty before activation")
			}
			previous = fmt.Sprintf("%s.pre-restore-%s", target, time.Now().UTC().Format("20060102T150405Z"))
			if err := os.Rename(target, previous); err != nil {
				return "", err
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err := os.Rename(stage, target); err != nil {
		if previous != "" {
			_ = os.Rename(previous, target)
		}
		return "", err
	}
	return previous, nil
}

func activateRestoreInPlace(stage, target string) error {
	stageBase := filepath.Base(stage)
	targetEntries, err := os.ReadDir(target)
	if err != nil {
		return err
	}
	if len(targetEntries) != 1 || targetEntries[0].Name() != stageBase {
		return fmt.Errorf("restore target changed while restore was running")
	}
	marker := filepath.Join(target, restoreActivationMarker)
	if err := os.WriteFile(marker, []byte(stageBase+"\n"), 0o600); err != nil {
		return err
	}

	entries, err := os.ReadDir(stage)
	if err != nil {
		_ = os.Remove(marker)
		return err
	}
	moved := make([]string, 0, len(entries))
	for _, entry := range entries {
		source := filepath.Join(stage, entry.Name())
		destination := filepath.Join(target, entry.Name())
		if err := os.Rename(source, destination); err != nil {
			rollbackErr := rollbackRestoreMoves(stage, target, moved)
			if rollbackErr == nil {
				_ = os.Remove(marker)
			}
			if rollbackErr != nil {
				return fmt.Errorf("activate restored data: %w; rollback failed: %v", err, rollbackErr)
			}
			return fmt.Errorf("activate restored data: %w", err)
		}
		moved = append(moved, entry.Name())
	}
	if err := os.Remove(stage); err != nil {
		return fmt.Errorf("remove restore staging directory: %w", err)
	}
	if err := os.Remove(marker); err != nil {
		return fmt.Errorf("remove restore activation marker: %w", err)
	}
	return syncDirectory(target)
}

func rollbackRestoreMoves(stage, target string, moved []string) error {
	for i := len(moved) - 1; i >= 0; i-- {
		name := moved[i]
		if err := os.Rename(filepath.Join(target, name), filepath.Join(stage, name)); err != nil {
			return err
		}
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func maxEventID(refs map[uint64]storage.EventRef) uint64 {
	var maximum uint64
	for eventID := range refs {
		if eventID > maximum {
			maximum = eventID
		}
	}
	return maximum
}
