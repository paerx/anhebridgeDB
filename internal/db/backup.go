package db

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paerx/anhebridgedb/internal/storage"
)

type BackupView struct {
	Path        string    `json:"path"`
	LastEventID uint64    `json:"last_event_id"`
	LastAuthTag string    `json:"last_auth_tag,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	FileCount   int       `json:"file_count"`
	SizeBytes   int64     `json:"size_bytes"`
}

type backupManifest struct {
	FormatVersion int                  `json:"format_version"`
	CreatedAt     time.Time            `json:"created_at"`
	LastEventID   uint64               `json:"last_event_id"`
	LastAuthTag   string               `json:"last_auth_tag,omitempty"`
	Files         []backupManifestFile `json:"files"`
}

type backupManifestFile struct {
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

// CreateBackupView creates a point-in-time filesystem view. Event segments are
// hard-linked after rotation; mutable operational metadata is copied.
func (e *Engine) CreateBackupView(parent string) (view BackupView, err error) {
	return e.createBackupView(parent, true)
}

func (e *Engine) CreateIncrementalBackupView(parent string) (view BackupView, err error) {
	return e.createBackupView(parent, false)
}

func (e *Engine) createBackupView(parent string, includeHashes bool) (view BackupView, err error) {
	e.backupMu.Lock()
	defer e.backupMu.Unlock()

	if parent == "" {
		parent = os.TempDir()
	}
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return BackupView{}, err
	}
	root, err := os.MkdirTemp(parent, "anhe-backup-view-")
	if err != nil {
		return BackupView{}, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
		}
	}()

	e.maintenanceMu.RLock()
	defer e.maintenanceMu.RUnlock()

	cut, err := e.log.RotateForBackup()
	if err != nil {
		return BackupView{}, fmt.Errorf("rotate event log: %w", err)
	}
	if err := e.linkBackupSegments(root, cut.LastSegment); err != nil {
		return BackupView{}, err
	}

	// Rule and task files are changed under metaMu. Copy rather than hard-link:
	// SaveJSON may truncate a mutable inode.
	e.metaMu.RLock()
	if err := copyIfExists(storage.RulesPath(e.dataDir), storage.RulesPath(root)); err == nil {
		err = copyDirFiles(storage.TaskBucketDir(e.dataDir), storage.TaskBucketDir(root))
	}
	e.metaMu.RUnlock()
	if err != nil {
		return BackupView{}, fmt.Errorf("copy operational metadata: %w", err)
	}

	checkpoint := storage.LogCheckpoint{
		NextID:         cut.LastEventID + 1,
		CurrentSegment: cut.LastSegment + 1,
		LastAuthTag:    cut.LastAuthTag,
		UpdatedAt:      cut.CreatedAt,
	}
	if err := storage.SaveJSON(storage.CheckpointPath(filepath.Join(root, "log")), checkpoint); err != nil {
		return BackupView{}, err
	}
	if err := os.WriteFile(storage.WalPath(filepath.Join(root, "log")), nil, 0o644); err != nil {
		return BackupView{}, err
	}

	fileCount := 0
	var total int64
	if includeHashes {
		manifest, manifestTotal, err := buildBackupManifest(root, cut)
		if err != nil {
			return BackupView{}, err
		}
		if err := storage.SaveJSON(filepath.Join(root, "backup.manifest.json"), manifest); err != nil {
			return BackupView{}, err
		}
		fileCount = len(manifest.Files)
		total = manifestTotal
	} else {
		fileCount, total, err = backupViewStats(root)
		if err != nil {
			return BackupView{}, err
		}
	}
	return BackupView{
		Path:        root,
		LastEventID: cut.LastEventID,
		LastAuthTag: cut.LastAuthTag,
		CreatedAt:   cut.CreatedAt,
		FileCount:   fileCount,
		SizeBytes:   total,
	}, nil
}

func (e *Engine) linkBackupSegments(root string, lastSegment int) error {
	sourceLog := filepath.Join(e.dataDir, "log")
	targetLog := filepath.Join(root, "log")
	if err := os.MkdirAll(targetLog, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(sourceLog)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !isBackupSegmentFile(entry.Name(), lastSegment) {
			continue
		}
		if err := linkOrCopy(filepath.Join(sourceLog, entry.Name()), filepath.Join(targetLog, entry.Name())); err != nil {
			return err
		}
	}
	return copyArchiveByLink(filepath.Join(sourceLog, "archive"), filepath.Join(targetLog, "archive"))
}

func isBackupSegmentFile(name string, lastSegment int) bool {
	if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".anhe") {
		var number int
		if _, err := fmt.Sscanf(name, "segment_%06d.anhe", &number); err == nil {
			return number <= lastSegment
		}
	}
	if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".manifest.json") {
		var number int
		if _, err := fmt.Sscanf(name, "segment_%06d.manifest.json", &number); err == nil {
			return number <= lastSegment
		}
	}
	return false
}

func copyArchiveByLink(source, target string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(dest, 0o755)
		}
		if strings.HasSuffix(entry.Name(), ".anhe") {
			return linkOrCopy(path, dest)
		}
		return copyFile(path, dest)
	})
}

func copyDirFiles(source, target string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if errors.Is(walkErr, os.ErrNotExist) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		dest := filepath.Join(target, rel)
		if entry.IsDir() {
			return os.MkdirAll(dest, 0o755)
		}
		return copyFile(path, dest)
	})
}

func copyIfExists(source, target string) error {
	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	return copyFile(source, target)
}

func linkOrCopy(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	if err := os.Link(source, target); err == nil {
		return nil
	}
	return copyFile(source, target)
}

func copyFile(source, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	info, err := src.Stat()
	if err != nil {
		return err
	}
	dst, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		return err
	}
	if err := dst.Sync(); err != nil {
		_ = dst.Close()
		return err
	}
	return dst.Close()
}

func buildBackupManifest(root string, cut storage.BackupCut) (backupManifest, int64, error) {
	manifest := backupManifest{
		FormatVersion: 1,
		CreatedAt:     cut.CreatedAt,
		LastEventID:   cut.LastEventID,
		LastAuthTag:   cut.LastAuthTag,
	}
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Base(path) == "backup.manifest.json" {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		size, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		manifest.Files = append(manifest.Files, backupManifestFile{
			Path:      filepath.ToSlash(rel),
			SizeBytes: size,
			SHA256:    hex.EncodeToString(hash.Sum(nil)),
		})
		total += size
		return nil
	})
	return manifest, total, err
}

func backupViewStats(root string) (int, int64, error) {
	var count int
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		count++
		total += info.Size()
		return nil
	})
	return count, total, err
}

func (v BackupView) Remove() error {
	if v.Path == "" {
		return nil
	}
	return os.RemoveAll(v.Path)
}
