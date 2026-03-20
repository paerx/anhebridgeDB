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

type SearchIndexEntry struct {
	EventID        uint64    `json:"event_id"`
	Key            string    `json:"key"`
	EventName      string    `json:"event_name,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

type IdempotencyIndexEntry struct {
	CompositeKey   string    `json:"composite_key"`
	Key            string    `json:"key"`
	EventName      string    `json:"event_name,omitempty"`
	IdempotencyKey string    `json:"idempotency_key"`
	EventID        uint64    `json:"event_id"`
	RequestHash    string    `json:"request_hash"`
	Timestamp      time.Time `json:"timestamp"`
}

func SearchIndexPath(dir string) string {
	return filepath.Join(dir, "index", "search_events.anhe")
}

func IdempotencyIndexPath(dir string) string {
	return filepath.Join(dir, "index", "idempotency.anhe")
}

func AppendSearchIndexEntry(dir string, entry SearchIndexEntry) error {
	return AppendSearchIndexEntries(dir, []SearchIndexEntry{entry})
}

func AppendSearchIndexEntries(dir string, entries []SearchIndexEntry) error {
	return appendJSONLines(SearchIndexPath(dir), entries)
}

func SaveSearchIndex(dir string, entries []SearchIndexEntry) error {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].EventID < entries[j].EventID
	})
	return saveJSONLines(SearchIndexPath(dir), entries)
}

func LoadSearchIndex(dir string) ([]SearchIndexEntry, error) {
	path := SearchIndexPath(dir)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []SearchIndexEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry SearchIndexEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func AppendIdempotencyIndexEntry(dir string, entry IdempotencyIndexEntry) error {
	return AppendIdempotencyIndexEntries(dir, []IdempotencyIndexEntry{entry})
}

func AppendIdempotencyIndexEntries(dir string, entries []IdempotencyIndexEntry) error {
	return appendJSONLines(IdempotencyIndexPath(dir), entries)
}

func SaveIdempotencyIndex(dir string, entries []IdempotencyIndexEntry) error {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].EventID < entries[j].EventID
	})
	return saveJSONLines(IdempotencyIndexPath(dir), entries)
}

func LoadIdempotencyIndex(dir string) ([]IdempotencyIndexEntry, error) {
	path := IdempotencyIndexPath(dir)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []IdempotencyIndexEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry IdempotencyIndexEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, scanner.Err()
}

func SearchIndexExists(dir string) bool {
	_, err := os.Stat(SearchIndexPath(dir))
	return err == nil
}

func IdempotencyIndexExists(dir string) bool {
	_, err := os.Stat(IdempotencyIndexPath(dir))
	return err == nil
}

func appendJSONLine(path string, value any) error {
	return appendJSONLines(path, []any{value})
}

func appendJSONLines[T any](path string, values []T) error {
	if len(values) == 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	payload := make([]byte, 0, len(values)*96)
	for _, value := range values {
		bytes, err := json.Marshal(value)
		if err != nil {
			return err
		}
		payload = append(payload, bytes...)
		payload = append(payload, '\n')
	}
	if _, err := file.Write(payload); err != nil {
		return err
	}
	return file.Sync()
}

func saveJSONLines[T any](path string, items []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, item := range items {
		bytes, err := json.Marshal(item)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(bytes, '\n')); err != nil {
			return err
		}
	}
	return file.Sync()
}
