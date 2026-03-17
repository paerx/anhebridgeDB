package storage

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type ImportReport struct {
	ImportedFiles   []string `json:"imported_files"`
	RebuiltPosition bool     `json:"rebuilt_position"`
	RebuiltLatest   bool     `json:"rebuilt_latest"`
	RestartRequired bool     `json:"restart_required"`
}

func ExportBackup(w io.Writer, dataDir, scope string, segments []string) error {
	paths, err := collectBackupPaths(dataDir, scope, segments)
	if err != nil {
		return err
	}
	gzw := gzip.NewWriter(w)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for _, rel := range paths {
		abs := filepath.Join(dataDir, rel)
		info, err := os.Stat(abs)
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			continue
		}
		file, err := os.Open(abs)
		if err != nil {
			return err
		}
		if _, err := io.Copy(tw, file); err != nil {
			_ = file.Close()
			return err
		}
		_ = file.Close()
	}
	return nil
}

func ImportBackup(r io.Reader, dataDir, scope string) (ImportReport, error) {
	report := ImportReport{RestartRequired: true}
	gzr, err := gzip.NewReader(r)
	if err != nil {
		return report, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return report, err
		}
		rel, err := safeArchivePath(header.Name)
		if err != nil {
			return report, err
		}
		target := filepath.Join(dataDir, rel)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return report, err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return report, err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return report, err
			}
			if _, err := io.Copy(file, tr); err != nil {
				_ = file.Close()
				return report, err
			}
			if err := file.Close(); err != nil {
				return report, err
			}
			report.ImportedFiles = append(report.ImportedFiles, rel)
		}
	}

	if _, err := RebuildPositionIndex(dataDir); err != nil {
		return report, err
	}
	report.RebuiltPosition = true
	if err := RebuildLatestIndex(dataDir); err != nil {
		return report, err
	}
	report.RebuiltLatest = true
	_ = os.Remove(CheckpointPath(filepath.Join(dataDir, "log")))
	sort.Strings(report.ImportedFiles)
	return report, nil
}

func RebuildLatestIndex(dataDir string) error {
	refs, err := LoadPositionIndex(dataDir)
	if err != nil {
		return err
	}
	if len(refs) == 0 {
		return CompactKeyIndex(dataDir, map[string]KeyIndexEntry{})
	}
	ids := make([]uint64, 0, len(refs))
	for id := range refs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	entries := map[string]KeyIndexEntry{}
	for _, id := range ids {
		ref := refs[id]
		event, err := ReadEventAt(dataDir, ref)
		if err != nil {
			return err
		}
		entries[event.Key] = KeyIndexEntry{
			Key:         event.Key,
			LatestEvent: event.EventID,
			UpdatedAt:   event.Timestamp,
		}
	}
	return CompactKeyIndex(dataDir, entries)
}

func collectBackupPaths(dataDir, scope string, segments []string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "", "full":
		return collectAllFiles(dataDir)
	case "segments":
		return collectSegmentFiles(dataDir, segments)
	default:
		return nil, fmt.Errorf("unsupported export scope: %s", scope)
	}
}

func collectAllFiles(dataDir string) ([]string, error) {
	paths := make([]string, 0)
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		paths = append(paths, rel)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func collectSegmentFiles(dataDir string, segments []string) ([]string, error) {
	if len(segments) == 0 {
		return nil, fmt.Errorf("segments scope requires at least one segment")
	}
	set := map[string]struct{}{}
	include := func(rel string) error {
		abs := filepath.Join(dataDir, rel)
		if _, err := os.Stat(abs); err != nil {
			return err
		}
		set[rel] = struct{}{}
		return nil
	}

	includeArchiveManifest := false
	for _, raw := range segments {
		segment := filepath.Base(strings.TrimSpace(raw))
		if segment == "" {
			continue
		}
		active := filepath.Join("log", segment)
		archive := filepath.Join("log", "archive", segment)
		switch {
		case fileExists(filepath.Join(dataDir, active)):
			if err := include(active); err != nil {
				return nil, err
			}
			manifest := filepath.Join("log", strings.TrimSuffix(segment, ".anhe")+".manifest.json")
			if fileExists(filepath.Join(dataDir, manifest)) {
				if err := include(manifest); err != nil {
					return nil, err
				}
			}
		case fileExists(filepath.Join(dataDir, archive)):
			if err := include(archive); err != nil {
				return nil, err
			}
			manifest := filepath.Join("log", "archive", strings.TrimSuffix(segment, ".anhe")+".manifest.json")
			if fileExists(filepath.Join(dataDir, manifest)) {
				if err := include(manifest); err != nil {
					return nil, err
				}
			}
			includeArchiveManifest = true
		default:
			return nil, fmt.Errorf("segment not found: %s", segment)
		}
	}
	if includeArchiveManifest && fileExists(filepath.Join(dataDir, "log", "archive", "archive.manifest.json")) {
		if err := include(filepath.Join("log", "archive", "archive.manifest.json")); err != nil {
			return nil, err
		}
	}
	paths := make([]string, 0, len(set))
	for rel := range set {
		paths = append(paths, rel)
	}
	sort.Strings(paths)
	return paths, nil
}

func safeArchivePath(name string) (string, error) {
	clean := filepath.Clean(name)
	if clean == "." || clean == "" {
		return "", fmt.Errorf("invalid archive path")
	}
	if strings.HasPrefix(clean, "..") || filepath.IsAbs(clean) {
		return "", fmt.Errorf("unsafe archive path: %s", name)
	}
	return clean, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
