package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/paerx/anhebridgedb/internal/storage"
)

const incrementalFormatVersion = 1
const incrementalStateFormatVersion = 2

type IncrementalManifest struct {
	FormatVersion int                       `json:"format_version"`
	BackupID      string                    `json:"backup_id"`
	CreatedAt     time.Time                 `json:"created_at"`
	LastEventID   uint64                    `json:"last_event_id"`
	LastAuthTag   string                    `json:"last_auth_tag,omitempty"`
	Files         []IncrementalManifestFile `json:"files"`
	AuthTag       string                    `json:"auth_tag"`
}

type IncrementalManifestFile struct {
	Path      string `json:"path"`
	ObjectKey string `json:"object_key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Immutable bool   `json:"immutable"`
}

type LatestPointer struct {
	FormatVersion  int       `json:"format_version"`
	BackupID       string    `json:"backup_id"`
	CreatedAt      time.Time `json:"created_at"`
	LastEventID    uint64    `json:"last_event_id"`
	ManifestKey    string    `json:"manifest_key"`
	ManifestSHA256 string    `json:"manifest_sha256"`
	AuthTag        string    `json:"auth_tag"`
}

type incrementalState struct {
	FormatVersion int                              `json:"format_version"`
	UpdatedAt     time.Time                        `json:"updated_at"`
	LastEventID   uint64                           `json:"last_event_id"`
	ContentHash   string                           `json:"content_hash,omitempty"`
	BackupID      string                           `json:"backup_id,omitempty"`
	ManifestKey   string                           `json:"manifest_key,omitempty"`
	RemoteTarget  string                           `json:"remote_target,omitempty"`
	Files         map[string]incrementalStateEntry `json:"files"`
}

type incrementalStateEntry struct {
	SizeBytes int64  `json:"size_bytes"`
	ModTimeNS int64  `json:"mod_time_ns"`
	SHA256    string `json:"sha256"`
	ObjectKey string `json:"object_key"`
}

func (m *Manager) runIncrementalBackup(ctx context.Context) (filename, objectKey string, size int64, lastEventID uint64, err error) {
	if m.uploader == nil {
		if m.uploadErr != nil {
			return "", "", 0, 0, m.uploadErr
		}
		return "", "", 0, 0, fmt.Errorf("incremental backup requires R2 upload")
	}
	if err := os.MkdirAll(m.cfg.SpoolDir, 0o755); err != nil {
		return "", "", 0, 0, err
	}
	view, err := m.engine.CreateIncrementalBackupView(filepath.Join(m.cfg.SpoolDir, ".views"))
	if err != nil {
		return "", "", 0, 0, err
	}
	defer view.Remove()

	statePath := filepath.Join(m.cfg.SpoolDir, "incremental-state.json")
	state := loadIncrementalState(statePath)
	paths, err := listViewFiles(view.Path)
	if err != nil {
		return "", "", 0, view.LastEventID, err
	}
	contentHash, err := incrementalViewContentHash(view.Path, paths, view.LastEventID, view.LastAuthTag)
	if err != nil {
		return "", "", 0, view.LastEventID, err
	}
	remoteTarget := m.uploader.stateTarget()
	if state.ContentHash != "" &&
		state.ContentHash == contentHash &&
		state.LastEventID == view.LastEventID &&
		state.ManifestKey != "" &&
		state.RemoteTarget == remoteTarget {
		return "", state.ManifestKey, 0, view.LastEventID, nil
	}

	backupID := fmt.Sprintf("%s-e%d", view.CreatedAt.UTC().Format("20060102T150405.000000000Z"), view.LastEventID)
	manifest := IncrementalManifest{
		FormatVersion: incrementalFormatVersion,
		BackupID:      backupID,
		CreatedAt:     view.CreatedAt,
		LastEventID:   view.LastEventID,
		LastAuthTag:   view.LastAuthTag,
	}
	nextState := incrementalState{
		FormatVersion: incrementalStateFormatVersion,
		UpdatedAt:     view.CreatedAt,
		LastEventID:   view.LastEventID,
		ContentHash:   contentHash,
		BackupID:      backupID,
		RemoteTarget:  remoteTarget,
		Files:         make(map[string]incrementalStateEntry),
	}

	var uploadedBytes int64
	for _, relative := range paths {
		source := filepath.Join(view.Path, filepath.FromSlash(relative))
		info, err := os.Stat(source)
		if err != nil {
			return "", "", uploadedBytes, view.LastEventID, err
		}
		immutable := isImmutableEventSegment(relative)
		entry, reusable := state.Files[relative]
		reusable = reusable &&
			immutable &&
			entry.SizeBytes == info.Size() &&
			entry.ModTimeNS == info.ModTime().UnixNano() &&
			entry.SHA256 != "" &&
			entry.ObjectKey != ""

		if !reusable {
			hash, err := fileSHA256(source)
			if err != nil {
				return "", "", uploadedBytes, view.LastEventID, err
			}
			entry = incrementalStateEntry{
				SizeBytes: info.Size(),
				ModTimeNS: info.ModTime().UnixNano(),
				SHA256:    hash,
			}
			if immutable {
				entry.ObjectKey = m.uploader.prefixedKey(filepath.ToSlash(filepath.Join(
					"incremental", "objects", hash[:2], hash+".anhe",
				)))
			} else {
				entry.ObjectKey = m.uploader.prefixedKey(filepath.ToSlash(filepath.Join(
					"incremental", "backups", backupID, "files", relative,
				)))
			}
			exists := false
			if immutable {
				exists, err = m.uploader.exists(ctx, entry.ObjectKey, info.Size())
				if err != nil {
					return "", entry.ObjectKey, uploadedBytes, view.LastEventID, err
				}
			}
			if !exists {
				if err := m.retry(ctx, func() error {
					return m.uploader.putFile(ctx, source, entry.ObjectKey, "application/octet-stream")
				}); err != nil {
					return "", entry.ObjectKey, uploadedBytes, view.LastEventID, err
				}
				uploadedBytes += info.Size()
			}
		}
		if immutable {
			nextState.Files[relative] = entry
		}
		manifest.Files = append(manifest.Files, IncrementalManifestFile{
			Path:      relative,
			ObjectKey: entry.ObjectKey,
			SizeBytes: entry.SizeBytes,
			SHA256:    entry.SHA256,
			Immutable: immutable,
		})
	}

	unsignedManifest, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", uploadedBytes, view.LastEventID, err
	}
	manifest.AuthTag = storage.ComputeDataAuthTag("incremental-manifest", unsignedManifest)
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", "", uploadedBytes, view.LastEventID, err
	}
	manifestSum := sha256.Sum256(manifestBytes)
	manifestKey := m.uploader.prefixedKey(filepath.ToSlash(filepath.Join(
		"incremental", "manifests", backupID+".json",
	)))
	if err := m.retry(ctx, func() error {
		return m.uploader.putBytes(ctx, manifestBytes, manifestKey, "application/json")
	}); err != nil {
		return "", manifestKey, uploadedBytes, view.LastEventID, err
	}
	uploadedBytes += int64(len(manifestBytes))

	pointer := LatestPointer{
		FormatVersion:  incrementalFormatVersion,
		BackupID:       backupID,
		CreatedAt:      view.CreatedAt,
		LastEventID:    view.LastEventID,
		ManifestKey:    manifestKey,
		ManifestSHA256: hex.EncodeToString(manifestSum[:]),
	}
	unsignedPointer, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return "", manifestKey, uploadedBytes, view.LastEventID, err
	}
	pointer.AuthTag = storage.ComputeDataAuthTag("incremental-latest", unsignedPointer)
	pointerBytes, err := json.MarshalIndent(pointer, "", "  ")
	if err != nil {
		return "", manifestKey, uploadedBytes, view.LastEventID, err
	}

	localManifestDir := filepath.Join(m.cfg.SpoolDir, "manifests")
	if err := os.MkdirAll(localManifestDir, 0o755); err != nil {
		return "", manifestKey, uploadedBytes, view.LastEventID, err
	}
	localManifest := filepath.Join(localManifestDir, backupID+".json")
	if err := writeFileAtomic(localManifest, manifestBytes, 0o600); err != nil {
		return "", manifestKey, uploadedBytes, view.LastEventID, err
	}
	latestKey := m.uploader.prefixedKey(filepath.ToSlash(filepath.Join("incremental", "latest.json")))
	if err := m.retry(ctx, func() error {
		return m.uploader.putBytes(ctx, pointerBytes, latestKey, "application/json")
	}); err != nil {
		return localManifest, latestKey, uploadedBytes, view.LastEventID, err
	}
	uploadedBytes += int64(len(pointerBytes))
	nextState.ManifestKey = manifestKey
	if err := saveIncrementalState(statePath, nextState); err != nil {
		return localManifest, manifestKey, uploadedBytes, view.LastEventID, err
	}
	m.pruneLocalManifests()
	return localManifest, manifestKey, uploadedBytes, view.LastEventID, nil
}

type incrementalFingerprint struct {
	LastEventID uint64                       `json:"last_event_id"`
	LastAuthTag string                       `json:"last_auth_tag,omitempty"`
	Files       []incrementalFingerprintFile `json:"files"`
}

type incrementalFingerprintFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func incrementalViewContentHash(
	root string,
	paths []string,
	lastEventID uint64,
	lastAuthTag string,
) (string, error) {
	fingerprint := incrementalFingerprint{
		LastEventID: lastEventID,
		LastAuthTag: lastAuthTag,
	}
	for _, relative := range paths {
		if isImmutableEventSegment(relative) {
			continue
		}
		source := filepath.Join(root, filepath.FromSlash(relative))
		data, err := stableIncrementalMetadata(relative, source)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(data)
		fingerprint.Files = append(fingerprint.Files, incrementalFingerprintFile{
			Path:   relative,
			SHA256: hex.EncodeToString(sum[:]),
		})
	}
	data, err := json.Marshal(fingerprint)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func stableIncrementalMetadata(relative, source string) ([]byte, error) {
	data, err := os.ReadFile(source)
	if err != nil {
		return nil, err
	}
	if filepath.ToSlash(relative) != "log/checkpoint.json" {
		return data, nil
	}
	var checkpoint storage.LogCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode backup checkpoint for fingerprint: %w", err)
	}
	checkpoint.UpdatedAt = time.Time{}
	return json.Marshal(checkpoint)
}

func listViewFiles(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() == "backup.manifest.json" {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		paths = append(paths, filepath.ToSlash(relative))
		return nil
	})
	sort.Strings(paths)
	return paths, err
}

func isImmutableEventSegment(relative string) bool {
	relative = filepath.ToSlash(relative)
	name := filepath.Base(relative)
	return strings.HasPrefix(relative, "log/") &&
		strings.HasPrefix(name, "segment_") &&
		strings.HasSuffix(name, ".anhe")
}

func loadIncrementalState(path string) incrementalState {
	state := incrementalState{FormatVersion: incrementalStateFormatVersion, Files: map[string]incrementalStateEntry{}}
	data, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(data, &state) != nil || state.Files == nil {
		state.Files = map[string]incrementalStateEntry{}
	}
	return state
}

func saveIncrementalState(path string, state incrementalState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o600)
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	temp := path + ".partial"
	if err := os.WriteFile(temp, data, mode); err != nil {
		return err
	}
	return os.Rename(temp, path)
}

func (m *Manager) pruneLocalManifests() {
	if m.cfg.LocalRetentionCount <= 0 {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(m.cfg.SpoolDir, "manifests", "*.json"))
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	if len(matches) <= m.cfg.LocalRetentionCount {
		return
	}
	for _, filename := range matches[m.cfg.LocalRetentionCount:] {
		_ = os.Remove(filename)
	}
}
