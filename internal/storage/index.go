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

type FastStore struct {
	path string
}

type PositionIndex struct {
	file *os.File
}

func PositionIndexPath(dir string) string {
	return filepath.Join(dir, "index", "positions.anhe")
}

func KeyIndexPath(dir string) string {
	return filepath.Join(dir, "index", "latest.anhe")
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

func SaveKeyIndex(dir string, entries map[string]KeyIndexEntry) error {
	list := make([]KeyIndexEntry, 0, len(entries))
	for _, entry := range entries {
		list = append(list, entry)
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Key < list[j].Key
	})
	return SaveJSON(KeyIndexPath(dir), list)
}

func LoadKeyIndex(dir string) (map[string]KeyIndexEntry, error) {
	var list []KeyIndexEntry
	if err := LoadJSON(KeyIndexPath(dir), &list); err != nil {
		return nil, err
	}
	index := map[string]KeyIndexEntry{}
	for _, entry := range list {
		index[entry.Key] = entry
	}
	return index, nil
}

func ReadEventAt(dir string, ref EventRef) (Event, error) {
	path := filepath.Join(dir, "log", ref.Segment)
	file, err := os.Open(path)
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
	var event Event
	if err := json.Unmarshal(buf, &event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func OpenFastStore(dir string) *FastStore {
	return &FastStore{path: filepath.Join(dir, "log", "fast.anhe")}
}

func (f *FastStore) Load() ([]FastEntry, error) {
	var entries []FastEntry
	if err := LoadJSON(f.path, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (f *FastStore) Save(entries []FastEntry) error {
	return SaveJSON(f.path, entries)
}
