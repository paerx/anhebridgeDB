package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type EventRef struct {
	EventID     uint64    `json:"event_id"`
	Key         string    `json:"key"`
	Timestamp   time.Time `json:"timestamp"`
	Segment     string    `json:"segment,omitempty"`
	Offset      int64     `json:"offset,omitempty"`
	Size        int       `json:"size,omitempty"`
	PrevEventID uint64    `json:"prev_event_id,omitempty"`
}

type KeyIndexEntry struct {
	Key         string    `json:"key"`
	LatestEvent uint64    `json:"latest_event"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type PositionIndex struct {
	file *os.File
}

func PositionIndexPath(dir string) string {
	return filepath.Join(dir, "index", "positions.anhe")
}

func KeyIndexPath(dir string) string {
	return KeyIndexCurrentPath(dir)
}

func KeyIndexCurrentPath(dir string) string {
	return filepath.Join(dir, "index", "latest.snapshot.anhe")
}

func KeyIndexDeltaPath(dir string) string {
	return filepath.Join(dir, "index", "latest.delta.anhe")
}

func OpenPositionIndex(dir string) (*PositionIndex, error) {
	path := PositionIndexPath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &PositionIndex{file: file}, nil
}

func (p *PositionIndex) Append(refs []EventRef) error {
	if len(refs) == 0 {
		return nil
	}
	var payload []byte
	for _, ref := range refs {
		bytes, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		payload = append(payload, bytes...)
		payload = append(payload, '\n')
	}
	if _, err := p.file.Write(payload); err != nil {
		return err
	}
	return p.file.Sync()
}

func (p *PositionIndex) Close() error {
	if p.file == nil {
		return nil
	}
	return p.file.Close()
}

func SavePositionIndex(dir string, refs map[uint64]EventRef) error {
	path := PositionIndexPath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	list := make([]EventRef, 0, len(refs))
	for _, ref := range refs {
		list = append(list, ref)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].EventID < list[j].EventID
	})
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, ref := range list {
		bytes, err := json.Marshal(ref)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(bytes, '\n')); err != nil {
			return err
		}
	}
	return file.Sync()
}

func LoadPositionIndex(dir string) (map[uint64]EventRef, error) {
	path := PositionIndexPath(dir)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[uint64]EventRef{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	index := map[uint64]EventRef{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var ref EventRef
		if err := json.Unmarshal(scanner.Bytes(), &ref); err != nil {
			return nil, err
		}
		index[ref.EventID] = ref
	}
	return index, scanner.Err()
}

func SaveKeyIndexSnapshot(dir string, entries map[string]KeyIndexEntry) error {
	list := make([]KeyIndexEntry, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Key < list[j].Key
	})
	return SaveJSON(KeyIndexCurrentPath(dir), list)
}

func AppendKeyIndexDelta(dir string, entry KeyIndexEntry) error {
	path := KeyIndexDeltaPath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(bytes, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func CompactKeyIndex(dir string, entries map[string]KeyIndexEntry) error {
	if err := SaveKeyIndexSnapshot(dir, entries); err != nil {
		return err
	}
	path := KeyIndexDeltaPath(dir)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func LoadKeyIndex(dir string) (map[string]KeyIndexEntry, error) {
	var list []KeyIndexEntry
	if err := LoadJSON(KeyIndexCurrentPath(dir), &list); err != nil {
		return nil, err
	}
	index := map[string]KeyIndexEntry{}
	for _, entry := range list {
		index[entry.Key] = entry
	}
	deltaPath := KeyIndexDeltaPath(dir)
	file, err := os.Open(deltaPath)
	if errors.Is(err, os.ErrNotExist) {
		return index, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry KeyIndexEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		index[entry.Key] = entry
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func ReadEventAt(dir string, ref EventRef) (Event, error) {
	path := filepath.Join(dir, "log", ref.Segment)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Open(ArchiveSegmentPath(filepath.Join(dir, "log"), ref.Segment))
	}
	if err != nil {
		return Event{}, err
	}
	defer file.Close()

	if _, err := file.Seek(ref.Offset, 0); err != nil {
		return Event{}, err
	}
	buf := make([]byte, ref.Size)
	if _, err := file.Read(buf); err != nil {
		return Event{}, err
	}
	if len(buf) > 0 && buf[len(buf)-1] == '\n' {
		buf = buf[:len(buf)-1]
	}
	return unmarshalRecord(buf)
}

func LastPositionIndexEventID(dir string) (uint64, error) {
	path := PositionIndexPath(dir)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var last uint64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var ref EventRef
		if err := json.Unmarshal(scanner.Bytes(), &ref); err != nil {
			return 0, err
		}
		if ref.EventID > last {
			last = ref.EventID
		}
	}
	return last, scanner.Err()
}

func RebuildPositionIndex(dir string) (map[uint64]EventRef, error) {
	logDir := filepath.Join(dir, "log")
	paths, err := listAllSegmentPaths(logDir)
	if err != nil {
		return nil, err
	}
	refs := map[uint64]EventRef{}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		var offset int64
		segment := filepath.Base(path)
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			size := len(line) + 1
			event, err := unmarshalRecord(line)
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			refs[event.EventID] = EventRef{
				EventID:     event.EventID,
				Key:         event.Key,
				Timestamp:   event.Timestamp,
				Segment:     segment,
				Offset:      offset,
				Size:        size,
				PrevEventID: event.PrevVersionOffset,
			}
			offset += int64(size)
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		_ = file.Close()
	}
	if err := SavePositionIndex(dir, refs); err != nil {
		return nil, err
	}
	return refs, nil
}
