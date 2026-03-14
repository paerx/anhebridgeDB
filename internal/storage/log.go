package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type SegmentOptions struct {
	MaxBytes   int64
	MaxRecords int
}

func DefaultSegmentOptions() SegmentOptions {
	return SegmentOptions{
		MaxBytes:   64 * 1024 * 1024,
		MaxRecords: 30000,
	}
}

type Event struct {
	EventID           uint64          `json:"event_id"`
	Timestamp         time.Time       `json:"timestamp"`
	Key               string          `json:"key"`
	Operation         string          `json:"operation"`
	EventName         string          `json:"event_name,omitempty"`
	OldValue          json.RawMessage `json:"old_value,omitempty"`
	NewValue          json.RawMessage `json:"new_value,omitempty"`
	Source            string          `json:"source,omitempty"`
	Actor             string          `json:"actor,omitempty"`
	RuleID            string          `json:"rule_id,omitempty"`
	CauseEventID      uint64          `json:"cause_event_id,omitempty"`
	PrevVersionOffset uint64          `json:"prev_version_offset,omitempty"`
}

type EventLog struct {
	mu                 sync.Mutex
	dir                string
	file               *os.File
	positionIndex      *PositionIndex
	nextID             uint64
	currentSegment     int
	currentRecordCount int
	currentSizeBytes   int64
	options            SegmentOptions
}

func EventLogDir(dir string) string {
	return dir
}

func EventSegmentPath(dir string, segment int) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.anhe", segment))
}

func FastPath(dir string) string {
	return filepath.Join(dir, "fast.anhe")
}

func OpenEventLog(dir string, options SegmentOptions) (*EventLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if options.MaxBytes == 0 && options.MaxRecords == 0 {
		options = DefaultSegmentOptions()
	}

	log := &EventLog{dir: dir, options: options}
	if err := log.bootstrap(); err != nil {
		return nil, err
	}
	index, err := OpenPositionIndex(filepath.Dir(dir))
	if err != nil {
		return nil, err
	}
	log.positionIndex = index
	return log, nil
}

func (l *EventLog) bootstrap() error {
	events, err := ReadAllEvents(l.dir)
	if err != nil {
		return err
	}

	var maxEventID uint64
	for _, event := range events {
		if event.EventID > maxEventID {
			maxEventID = event.EventID
		}
	}
	l.nextID = maxEventID + 1

	segments, err := listSegmentPaths(l.dir)
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		l.currentSegment = 1
		return l.openSegment(1)
	}

	lastPath := segments[len(segments)-1]
	segment, err := parseSegmentNumber(filepath.Base(lastPath))
	if err != nil {
		return err
	}
	count, err := countSegmentRecords(lastPath)
	if err != nil {
		return err
	}
	info, err := os.Stat(lastPath)
	if err != nil {
		return err
	}
	l.currentSegment = segment
	l.currentRecordCount = count
	l.currentSizeBytes = info.Size()
	if l.shouldRotateForExisting() {
		l.currentSegment++
		l.currentRecordCount = 0
		l.currentSizeBytes = 0
	}
	return l.openSegment(l.currentSegment)
}

func (l *EventLog) openSegment(segment int) error {
	if l.file != nil {
		_ = l.file.Close()
	}
	path := EventSegmentPath(l.dir, segment)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.file = file
	l.currentSegment = segment
	return nil
}

func (l *EventLog) shouldRotateForExisting() bool {
	if l.options.MaxRecords > 0 && l.currentRecordCount >= l.options.MaxRecords {
		return true
	}
	if l.options.MaxBytes > 0 && l.currentSizeBytes >= l.options.MaxBytes {
		return true
	}
	return false
}

func (l *EventLog) ensureCapacity(additionalRecords int, additionalBytes int64) error {
	rotateByRecords := l.options.MaxRecords > 0 && l.currentRecordCount+additionalRecords > l.options.MaxRecords
	rotateByBytes := l.options.MaxBytes > 0 && l.currentSizeBytes+additionalBytes > l.options.MaxBytes
	if !rotateByRecords && !rotateByBytes {
		return nil
	}
	l.currentSegment++
	l.currentRecordCount = 0
	l.currentSizeBytes = 0
	return l.openSegment(l.currentSegment)
}

func (l *EventLog) Append(event Event) (Event, error) {
	events, err := l.AppendBatch([]Event{event})
	if err != nil {
		return Event{}, err
	}
	return events[0], nil
}

func (l *EventLog) AppendBatch(events []Event) ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(events) == 0 {
		return nil, nil
	}

	persisted := make([]Event, 0, len(events))
	var (
		payload []byte
		refs    []EventRef
		offset  int64
	)
	if currentOffset, err := l.file.Seek(0, os.SEEK_END); err == nil {
		offset = currentOffset
	}

	flush := func() error {
		if len(payload) == 0 {
			return nil
		}
		if _, err := l.file.Write(payload); err != nil {
			return err
		}
		if err := l.file.Sync(); err != nil {
			return err
		}
		if err := l.positionIndex.Append(refs); err != nil {
			return err
		}
		l.currentRecordCount += len(refs)
		l.currentSizeBytes += int64(len(payload))
		payload = nil
		refs = nil
		nextOffset, err := l.file.Seek(0, os.SEEK_END)
		if err != nil {
			return err
		}
		offset = nextOffset
		return nil
	}

	for _, event := range events {
		event.EventID = l.nextID
		l.nextID++

		bytes, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		recordSize := int64(len(bytes) + 1)
		if err := l.ensureCapacity(len(refs)+1, int64(len(payload))+recordSize); err != nil {
			return nil, err
		}
		if (l.options.MaxRecords > 0 && l.currentRecordCount+len(refs)+1 > l.options.MaxRecords) ||
			(l.options.MaxBytes > 0 && l.currentSizeBytes+int64(len(payload))+recordSize > l.options.MaxBytes) {
			if err := flush(); err != nil {
				return nil, err
			}
			if err := l.ensureCapacity(1, recordSize); err != nil {
				return nil, err
			}
		}

		segmentName := filepath.Base(EventSegmentPath(l.dir, l.currentSegment))
		payload = append(payload, bytes...)
		payload = append(payload, '\n')
		refs = append(refs, EventRef{
			EventID:     event.EventID,
			Key:         event.Key,
			Timestamp:   event.Timestamp,
			Segment:     segmentName,
			Offset:      offset + int64(len(payload)-len(bytes)-1),
			Size:        len(bytes) + 1,
			PrevEventID: event.PrevVersionOffset,
		})
		persisted = append(persisted, event)
	}

	if err := flush(); err != nil {
		return nil, err
	}

	return persisted, nil
}

func (l *EventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.positionIndex != nil {
		_ = l.positionIndex.Close()
	}
	if l.file == nil {
		return nil
	}
	return l.file.Close()
}

func ReadAllEvents(dir string) ([]Event, error) {
	paths, err := listSegmentPaths(dir)
	if err != nil {
		return nil, err
	}

	var events []Event
	for _, path := range paths {
		segmentEvents, err := ReadEvents(path)
		if err != nil {
			return nil, err
		}
		events = append(events, segmentEvents...)
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].EventID < events[j].EventID
	})
	return events, nil
}

func ReadEvents(path string) ([]Event, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var events []Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func SaveJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o644)
}

func LoadJSON(path string, target any) error {
	bytes, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(bytes) == 0 {
		return nil
	}
	if err := json.Unmarshal(bytes, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func listSegmentPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".anhe") {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func parseSegmentNumber(name string) (int, error) {
	var segment int
	if _, err := fmt.Sscanf(name, "segment_%06d.anhe", &segment); err != nil {
		return 0, err
	}
	return segment, nil
}

func countSegmentRecords(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}
