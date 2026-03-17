package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

type SnapshotRecord struct {
	Value     json.RawMessage `json:"value,omitempty"`
	Version   uint64          `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type Snapshot struct {
	CreatedAt   time.Time                 `json:"created_at"`
	LastEventID uint64                    `json:"last_event_id"`
	State       map[string]SnapshotRecord `json:"state"`
}

func SnapshotPath(dir string) string {
	return filepath.Join(dir, "snapshot", "latest.json")
}

func LoadSnapshot(dir string) (Snapshot, error) {
	path := SnapshotPath(dir)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) == 0 || len(bytes.TrimSpace(data)) == 0 {
		return Snapshot{State: map[string]SnapshotRecord{}}, nil
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		if errors.Is(err, io.EOF) {
			return Snapshot{State: map[string]SnapshotRecord{}}, nil
		}
		return Snapshot{}, err
	}
	if snapshot.State == nil {
		snapshot.State = map[string]SnapshotRecord{}
	}
	return snapshot, nil
}

func SaveSnapshot(dir string, snapshot Snapshot) error {
	snapshotDir := filepath.Dir(SnapshotPath(dir))
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return err
	}

	bytes, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(SnapshotPath(dir), bytes, 0o644)
}
