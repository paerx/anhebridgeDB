package storage

import (
	"bufio"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
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
	IdempotencyKey    string          `json:"idempotency_key,omitempty"`
	IdempotencyHash   string          `json:"idempotency_hash,omitempty"`
	OldValue          json.RawMessage `json:"old_value,omitempty"`
	NewValue          json.RawMessage `json:"new_value,omitempty"`
	Source            string          `json:"source,omitempty"`
	Actor             string          `json:"actor,omitempty"`
	RuleID            string          `json:"rule_id,omitempty"`
	CauseEventID      uint64          `json:"cause_event_id,omitempty"`
	PrevVersionOffset uint64          `json:"prev_version_offset,omitempty"`
	AuthTag           string          `json:"auth_tag,omitempty"`
}

type SegmentManifest struct {
	Segment        string    `json:"segment"`
	FirstEventID   uint64    `json:"first_event_id,omitempty"`
	LastEventID    uint64    `json:"last_event_id,omitempty"`
	RecordCount    int       `json:"record_count"`
	SizeBytes      int64     `json:"size_bytes"`
	FirstTimestamp time.Time `json:"first_timestamp,omitempty"`
	LastTimestamp  time.Time `json:"last_timestamp,omitempty"`
	FirstAuthTag   string    `json:"first_auth_tag,omitempty"`
	LastAuthTag    string    `json:"last_auth_tag,omitempty"`
	ManifestAuth   string    `json:"manifest_auth,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type LogCheckpoint struct {
	NextID         uint64    `json:"next_id"`
	CurrentSegment int       `json:"current_segment"`
	LastAuthTag    string    `json:"last_auth_tag,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type ArchiveSegmentEntry struct {
	Segment       string    `json:"segment"`
	FirstEventID  uint64    `json:"first_event_id,omitempty"`
	LastEventID   uint64    `json:"last_event_id,omitempty"`
	RecordCount   int       `json:"record_count"`
	SizeBytes     int64     `json:"size_bytes"`
	ArchivedAt    time.Time `json:"archived_at"`
	Compacted     bool      `json:"compacted"`
	CompactedFrom []string  `json:"compacted_from,omitempty"`
}

type ArchiveManifest struct {
	UpdatedAt time.Time             `json:"updated_at"`
	Segments  []ArchiveSegmentEntry `json:"segments"`
}

type recordEnvelope struct {
	CRC32 uint32          `json:"crc32"`
	Data  json.RawMessage `json:"data"`
}

type EventLog struct {
	mu                 sync.Mutex
	dir                string
	file               *os.File
	walFile            *os.File
	positionIndex      *PositionIndex
	nextID             uint64
	currentSegment     int
	currentRecordCount int
	currentSizeBytes   int64
	options            SegmentOptions
	lastAuthTag        string
	manifests          map[string]SegmentManifest
}

func EventLogDir(dir string) string {
	return dir
}

func EventSegmentPath(dir string, segment int) string {
	return filepath.Join(dir, fmt.Sprintf("segment_%06d.anhe", segment))
}

func ArchiveDir(dir string) string {
	return filepath.Join(dir, "archive")
}

func ArchiveManifestPath(dir string) string {
	return filepath.Join(ArchiveDir(dir), "archive.manifest.json")
}

func ArchiveSegmentPath(dir, segment string) string {
	return filepath.Join(ArchiveDir(dir), segment)
}

func FastPath(dir string) string {
	return filepath.Join(dir, "fast.anhe")
}

func WalPath(dir string) string {
	return filepath.Join(dir, "events.wal")
}

func SegmentManifestPath(dir, segment string) string {
	return filepath.Join(dir, strings.TrimSuffix(segment, ".anhe")+".manifest.json")
}

func ArchiveSegmentManifestPath(dir, segment string) string {
	return filepath.Join(ArchiveDir(dir), strings.TrimSuffix(segment, ".anhe")+".manifest.json")
}

func CheckpointPath(dir string) string {
	return filepath.Join(dir, "checkpoint.json")
}

func OpenEventLog(dir string, options SegmentOptions) (*EventLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	if options.MaxBytes == 0 && options.MaxRecords == 0 {
		options = DefaultSegmentOptions()
	}

	log := &EventLog{dir: dir, options: options, manifests: map[string]SegmentManifest{}}
	if err := log.bootstrap(); err != nil {
		return nil, err
	}
	walFile, err := os.OpenFile(WalPath(dir), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	log.walFile = walFile
	index, err := OpenPositionIndex(filepath.Dir(dir))
	if err != nil {
		return nil, err
	}
	log.positionIndex = index
	if err := log.recoverFromWAL(); err != nil {
		return nil, err
	}
	return log, nil
}

func (l *EventLog) bootstrap() error {
	checkpoint, err := LoadLogCheckpoint(l.dir)
	if err != nil {
		return err
	}
	lastEventID, err := LastPositionIndexEventID(filepath.Dir(l.dir))
	if err != nil {
		return err
	}
	if checkpoint.NextID > 0 {
		l.nextID = checkpoint.NextID
		l.lastAuthTag = checkpoint.LastAuthTag
		if checkpoint.CurrentSegment > 0 {
			l.currentSegment = checkpoint.CurrentSegment
		}
	}
	if l.nextID == 0 {
		l.nextID = lastEventID + 1
	}

	manifests, err := LoadSegmentManifests(l.dir)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		l.manifests[manifest.Segment] = manifest
		if l.lastAuthTag == "" && manifest.LastEventID == lastEventID {
			l.lastAuthTag = manifest.LastAuthTag
		}
	}

	segments, err := listSegmentPaths(l.dir)
	if err != nil {
		return err
	}

	if len(segments) == 0 {
		if l.currentSegment == 0 {
			l.currentSegment = 1
		}
		return l.openSegment(l.currentSegment)
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
	if l.currentSegment == 0 {
		l.currentSegment = segment
	}
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
		if err := l.updateManifest(refs, persisted[len(persisted)-len(refs):]); err != nil {
			return err
		}
		if err := l.persistCheckpoint(); err != nil {
			return err
		}
		if err := l.clearWAL(); err != nil {
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
		authTag, err := ComputeEventAuthTag(event, l.lastAuthTag)
		if err != nil {
			return nil, err
		}
		event.AuthTag = authTag
		l.lastAuthTag = event.AuthTag

		payloadBytes, err := marshalRecord(event)
		if err != nil {
			return nil, err
		}
		recordSize := int64(len(payloadBytes) + 1)
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
		payload = append(payload, payloadBytes...)
		payload = append(payload, '\n')
		refs = append(refs, EventRef{
			EventID:     event.EventID,
			Key:         event.Key,
			Timestamp:   event.Timestamp,
			Segment:     segmentName,
			Offset:      offset + int64(len(payload)-len(payloadBytes)-1),
			Size:        len(payloadBytes) + 1,
			PrevEventID: event.PrevVersionOffset,
		})
		persisted = append(persisted, event)
	}
	if err := l.writeWAL(persisted); err != nil {
		return nil, err
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
	if l.walFile != nil {
		_ = l.walFile.Close()
	}
	if l.file == nil {
		return nil
	}
	return l.file.Close()
}

func marshalRecord(event Event) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	env := recordEnvelope{
		CRC32: crc32.ChecksumIEEE(data),
		Data:  data,
	}
	return json.Marshal(env)
}

func unmarshalRecord(line []byte) (Event, error) {
	var env recordEnvelope
	if err := json.Unmarshal(line, &env); err == nil && len(env.Data) > 0 {
		if crc32.ChecksumIEEE(env.Data) != env.CRC32 {
			return Event{}, fmt.Errorf("crc mismatch")
		}
		var event Event
		if err := json.Unmarshal(env.Data, &event); err != nil {
			return Event{}, err
		}
		return event, nil
	}
	var event Event
	if err := json.Unmarshal(line, &event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func (l *EventLog) writeWAL(events []Event) error {
	if l.walFile == nil || len(events) == 0 {
		return nil
	}
	if err := l.walFile.Truncate(0); err != nil {
		return err
	}
	if _, err := l.walFile.Seek(0, 0); err != nil {
		return err
	}
	for _, event := range events {
		bytes, err := marshalRecord(event)
		if err != nil {
			return err
		}
		if _, err := l.walFile.Write(append(bytes, '\n')); err != nil {
			return err
		}
	}
	return l.walFile.Sync()
}

func (l *EventLog) clearWAL() error {
	if l.walFile == nil {
		return nil
	}
	if err := l.walFile.Truncate(0); err != nil {
		return err
	}
	_, err := l.walFile.Seek(0, 0)
	return err
}

func (l *EventLog) recoverFromWAL() error {
	events, err := readWAL(l.dir)
	if err != nil || len(events) == 0 {
		return err
	}
	lastPositionID, err := LastPositionIndexEventID(filepath.Dir(l.dir))
	if err != nil {
		return err
	}
	replay := make([]Event, 0, len(events))
	for _, event := range events {
		if event.EventID > lastPositionID {
			replay = append(replay, event)
		}
	}
	if len(replay) == 0 {
		return l.clearWAL()
	}
	var (
		payload []byte
		refs    []EventRef
		offset  int64
	)
	if currentOffset, err := l.file.Seek(0, os.SEEK_END); err == nil {
		offset = currentOffset
	}
	for _, event := range replay {
		bytes, err := marshalRecord(event)
		if err != nil {
			return err
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
		l.lastAuthTag = event.AuthTag
		if event.EventID >= l.nextID {
			l.nextID = event.EventID + 1
		}
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
	if err := l.updateManifest(refs, replay); err != nil {
		return err
	}
	l.currentRecordCount += len(refs)
	l.currentSizeBytes += int64(len(payload))
	if err := l.persistCheckpoint(); err != nil {
		return err
	}
	return l.clearWAL()
}

func persistLogCheckpoint(dir string, checkpoint LogCheckpoint) error {
	return SaveJSON(CheckpointPath(dir), checkpoint)
}

func LoadLogCheckpoint(dir string) (LogCheckpoint, error) {
	var checkpoint LogCheckpoint
	if err := LoadJSON(CheckpointPath(dir), &checkpoint); err != nil {
		return LogCheckpoint{}, err
	}
	return checkpoint, nil
}

func (l *EventLog) persistCheckpoint() error {
	return persistLogCheckpoint(l.dir, LogCheckpoint{
		NextID:         l.nextID,
		CurrentSegment: l.currentSegment,
		LastAuthTag:    l.lastAuthTag,
		UpdatedAt:      time.Now().UTC(),
	})
}

func readWAL(dir string) ([]Event, error) {
	path := WalPath(dir)
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
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		event, err := unmarshalRecord(line)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, scanner.Err()
}

func ArchiveColdSegments(logDir string, keepSegment string, lastSafeEventID uint64) (int, error) {
	manifests, err := LoadSegmentManifests(logDir)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(ArchiveDir(logDir), 0o755); err != nil {
		return 0, err
	}
	archived := 0
	archiveManifest, err := LoadArchiveManifest(logDir)
	if err != nil {
		return 0, err
	}
	for _, manifest := range manifests {
		if manifest.Segment == "" || manifest.Segment == keepSegment {
			continue
		}
		if manifest.LastEventID == 0 || manifest.LastEventID > lastSafeEventID {
			continue
		}
		activeSegment := filepath.Join(logDir, manifest.Segment)
		if _, err := os.Stat(activeSegment); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := os.Rename(activeSegment, ArchiveSegmentPath(logDir, manifest.Segment)); err != nil {
			return archived, err
		}
		activeManifest := SegmentManifestPath(logDir, manifest.Segment)
		if _, err := os.Stat(activeManifest); err == nil {
			if err := os.Rename(activeManifest, ArchiveSegmentManifestPath(logDir, manifest.Segment)); err != nil {
				return archived, err
			}
		}
		archiveManifest = upsertArchiveEntry(archiveManifest, ArchiveSegmentEntry{
			Segment:      manifest.Segment,
			FirstEventID: manifest.FirstEventID,
			LastEventID:  manifest.LastEventID,
			RecordCount:  manifest.RecordCount,
			SizeBytes:    manifest.SizeBytes,
			ArchivedAt:   time.Now().UTC(),
		})
		archived++
	}
	if archived > 0 {
		archiveManifest.UpdatedAt = time.Now().UTC()
		if err := SaveJSON(ArchiveManifestPath(logDir), archiveManifest); err != nil {
			return archived, err
		}
	}
	return archived, nil
}

func CompactArchivedSegments(dataDir string, currentSegment string, lastSafeEventID uint64) (int, error) {
	logDir := filepath.Join(dataDir, "log")
	archiveDir := ArchiveDir(logDir)
	if err := os.MkdirAll(archiveDir, 0o755); err != nil {
		return 0, err
	}
	manifests, err := LoadSegmentManifests(logDir)
	if err != nil {
		return 0, err
	}
	candidates := make([]SegmentManifest, 0)
	for _, manifest := range manifests {
		if manifest.Segment == "" || manifest.Segment == currentSegment {
			continue
		}
		path := ArchiveSegmentPath(logDir, manifest.Segment)
		if _, err := os.Stat(path); err != nil {
			continue
		}
		if manifest.LastEventID == 0 || manifest.LastEventID > lastSafeEventID {
			continue
		}
		candidates = append(candidates, manifest)
	}
	if len(candidates) < 2 {
		return 0, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].FirstEventID < candidates[j].FirstEventID
	})

	refs, err := LoadPositionIndex(dataDir)
	if err != nil {
		return 0, err
	}
	archiveManifest, err := LoadArchiveManifest(logDir)
	if err != nil {
		return 0, err
	}
	segmentNumber, err := nextArchiveCompactSegmentNumber(logDir)
	if err != nil {
		return 0, err
	}
	targetSegment := fmt.Sprintf("segment_%06d.anhe", segmentNumber)
	targetPath := ArchiveSegmentPath(logDir, targetSegment)
	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}

	var (
		offset     int64
		totalBytes int64
		outRefs    []EventRef
		outEvents  []Event
		sources    []string
	)
	for _, manifest := range candidates {
		events, err := ReadEvents(ArchiveSegmentPath(logDir, manifest.Segment))
		if err != nil {
			_ = file.Close()
			return 0, err
		}
		for _, event := range events {
			payload, err := marshalRecord(event)
			if err != nil {
				_ = file.Close()
				return 0, err
			}
			if _, err := file.Write(append(payload, '\n')); err != nil {
				_ = file.Close()
				return 0, err
			}
			outRefs = append(outRefs, EventRef{
				EventID:     event.EventID,
				Key:         event.Key,
				Timestamp:   event.Timestamp,
				Segment:     targetSegment,
				Offset:      offset,
				Size:        len(payload) + 1,
				PrevEventID: event.PrevVersionOffset,
			})
			offset += int64(len(payload) + 1)
			totalBytes += int64(len(payload) + 1)
			outEvents = append(outEvents, event)
		}
		sources = append(sources, manifest.Segment)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return 0, err
	}
	if err := file.Close(); err != nil {
		return 0, err
	}

	manifest := buildSegmentManifest(targetSegment, outRefs, outEvents)
	if err := SaveJSON(ArchiveSegmentManifestPath(logDir, targetSegment), manifest); err != nil {
		return 0, err
	}
	for _, ref := range outRefs {
		refs[ref.EventID] = ref
	}
	if err := SavePositionIndex(dataDir, refs); err != nil {
		return 0, err
	}

	for _, source := range sources {
		_ = os.Remove(ArchiveSegmentPath(logDir, source))
		_ = os.Remove(ArchiveSegmentManifestPath(logDir, source))
		archiveManifest = removeArchiveEntry(archiveManifest, source)
	}
	archiveManifest = upsertArchiveEntry(archiveManifest, ArchiveSegmentEntry{
		Segment:       targetSegment,
		FirstEventID:  manifest.FirstEventID,
		LastEventID:   manifest.LastEventID,
		RecordCount:   manifest.RecordCount,
		SizeBytes:     totalBytes,
		ArchivedAt:    time.Now().UTC(),
		Compacted:     true,
		CompactedFrom: sources,
	})
	archiveManifest.UpdatedAt = time.Now().UTC()
	if err := SaveJSON(ArchiveManifestPath(logDir), archiveManifest); err != nil {
		return 0, err
	}
	return len(sources), nil
}

func ComputeEventAuthTag(event Event, previousTag string) (string, error) {
	unsigned := event
	unsigned.AuthTag = ""
	bytes, err := json.Marshal(unsigned)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, hmacSecret())
	_, _ = mac.Write([]byte(previousTag))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write(bytes)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func ComputeRollingAuth(current, next string) string {
	mac := hmac.New(sha256.New, hmacSecret())
	_, _ = mac.Write([]byte(current))
	_, _ = mac.Write([]byte{'\n'})
	_, _ = mac.Write([]byte(next))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func hmacSecret() []byte {
	secret := os.Getenv("ANHEBRIDGE_HMAC_KEY")
	if secret == "" {
		secret = "anhebridgedb-dev-hmac-key"
	}
	return []byte(secret)
}

func (l *EventLog) updateManifest(refs []EventRef, events []Event) error {
	if len(refs) == 0 || len(events) == 0 {
		return nil
	}
	segment := refs[0].Segment
	manifest := l.manifests[segment]
	if manifest.Segment == "" {
		manifest = SegmentManifest{Segment: segment}
	}
	for i, event := range events {
		ref := refs[i]
		if manifest.RecordCount == 0 {
			manifest.FirstEventID = event.EventID
			manifest.FirstTimestamp = event.Timestamp
			manifest.FirstAuthTag = event.AuthTag
			manifest.ManifestAuth = event.AuthTag
		} else {
			manifest.ManifestAuth = ComputeRollingAuth(manifest.ManifestAuth, event.AuthTag)
		}
		manifest.LastEventID = event.EventID
		manifest.LastTimestamp = event.Timestamp
		manifest.LastAuthTag = event.AuthTag
		manifest.RecordCount++
		manifest.SizeBytes += int64(ref.Size)
	}
	manifest.UpdatedAt = time.Now().UTC()
	l.manifests[segment] = manifest
	return SaveJSON(SegmentManifestPath(l.dir, segment), manifest)
}

func LoadSegmentManifest(logDir, segment string) (SegmentManifest, error) {
	var manifest SegmentManifest
	if err := LoadJSON(SegmentManifestPath(logDir, segment), &manifest); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return SegmentManifest{}, err
		}
		if err := LoadJSON(ArchiveSegmentManifestPath(logDir, segment), &manifest); err != nil {
			return SegmentManifest{}, err
		}
	}
	return manifest, nil
}

func LoadSegmentManifests(logDir string) ([]SegmentManifest, error) {
	paths, err := filepath.Glob(filepath.Join(logDir, "*.manifest.json"))
	if err != nil {
		return nil, err
	}
	archived, err := filepath.Glob(filepath.Join(ArchiveDir(logDir), "*.manifest.json"))
	if err != nil {
		return nil, err
	}
	paths = append(paths, archived...)
	sort.Strings(paths)
	manifests := make([]SegmentManifest, 0, len(paths))
	for _, path := range paths {
		if filepath.Base(path) == "archive.manifest.json" {
			continue
		}
		var manifest SegmentManifest
		if err := LoadJSON(path, &manifest); err != nil {
			return nil, err
		}
		if manifest.Segment != "" {
			manifests = append(manifests, manifest)
		}
	}
	return manifests, nil
}

func LoadArchiveManifest(logDir string) (ArchiveManifest, error) {
	var manifest ArchiveManifest
	if err := LoadJSON(ArchiveManifestPath(logDir), &manifest); err != nil {
		return ArchiveManifest{}, err
	}
	if manifest.Segments == nil {
		manifest.Segments = []ArchiveSegmentEntry{}
	}
	return manifest, nil
}

func ValidateSegmentManifest(logDir, segment string) ([]string, error) {
	manifest, err := LoadSegmentManifest(logDir, segment)
	if err != nil {
		return nil, err
	}
	if manifest.Segment == "" {
		return []string{"missing manifest"}, nil
	}
	events, err := ReadEvents(filepath.Join(logDir, segment))
	if len(events) == 0 {
		events, err = ReadEvents(ArchiveSegmentPath(logDir, segment))
	}
	if err != nil {
		return nil, err
	}
	if len(events) != manifest.RecordCount {
		return []string{fmt.Sprintf("manifest record_count=%d actual=%d", manifest.RecordCount, len(events))}, nil
	}
	if len(events) == 0 {
		return nil, nil
	}

	issues := make([]string, 0)
	if manifest.FirstEventID != events[0].EventID {
		issues = append(issues, fmt.Sprintf("manifest first_event_id=%d actual=%d", manifest.FirstEventID, events[0].EventID))
	}
	if manifest.LastEventID != events[len(events)-1].EventID {
		issues = append(issues, fmt.Sprintf("manifest last_event_id=%d actual=%d", manifest.LastEventID, events[len(events)-1].EventID))
	}
	if manifest.FirstAuthTag != events[0].AuthTag {
		issues = append(issues, "manifest first_auth_tag mismatch")
	}
	if manifest.LastAuthTag != events[len(events)-1].AuthTag {
		issues = append(issues, "manifest last_auth_tag mismatch")
	}

	rolling := ""
	for i, event := range events {
		if i == 0 {
			rolling = event.AuthTag
			continue
		}
		rolling = ComputeRollingAuth(rolling, event.AuthTag)
	}
	if manifest.ManifestAuth != rolling {
		issues = append(issues, "manifest manifest_auth mismatch")
	}
	return issues, nil
}

func (l *EventLog) Stats() map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()

	var totalBytes int64
	for _, manifest := range l.manifests {
		totalBytes += manifest.SizeBytes
	}
	archiveManifest, _ := LoadArchiveManifest(l.dir)
	return map[string]any{
		"segments":                len(l.manifests),
		"manifest_total_bytes":    totalBytes,
		"segment_total_bytes":     totalBytes,
		"current_segment":         filepath.Base(EventSegmentPath(l.dir, l.currentSegment)),
		"current_segment_records": l.currentRecordCount,
		"current_segment_bytes":   l.currentSizeBytes,
		"auth_chain_head":         l.lastAuthTag,
		"archive_segments":        len(archiveManifest.Segments),
	}
}

func buildSegmentManifest(segment string, refs []EventRef, events []Event) SegmentManifest {
	manifest := SegmentManifest{Segment: segment}
	for i, event := range events {
		ref := refs[i]
		if manifest.RecordCount == 0 {
			manifest.FirstEventID = event.EventID
			manifest.FirstTimestamp = event.Timestamp
			manifest.FirstAuthTag = event.AuthTag
			manifest.ManifestAuth = event.AuthTag
		} else {
			manifest.ManifestAuth = ComputeRollingAuth(manifest.ManifestAuth, event.AuthTag)
		}
		manifest.LastEventID = event.EventID
		manifest.LastTimestamp = event.Timestamp
		manifest.LastAuthTag = event.AuthTag
		manifest.RecordCount++
		manifest.SizeBytes += int64(ref.Size)
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest
}

func upsertArchiveEntry(manifest ArchiveManifest, entry ArchiveSegmentEntry) ArchiveManifest {
	for idx := range manifest.Segments {
		if manifest.Segments[idx].Segment == entry.Segment {
			manifest.Segments[idx] = entry
			return manifest
		}
	}
	manifest.Segments = append(manifest.Segments, entry)
	sort.Slice(manifest.Segments, func(i, j int) bool {
		return manifest.Segments[i].FirstEventID < manifest.Segments[j].FirstEventID
	})
	return manifest
}

func removeArchiveEntry(manifest ArchiveManifest, segment string) ArchiveManifest {
	filtered := manifest.Segments[:0]
	for _, entry := range manifest.Segments {
		if entry.Segment == segment {
			continue
		}
		filtered = append(filtered, entry)
	}
	manifest.Segments = filtered
	return manifest
}

func nextArchiveCompactSegmentNumber(logDir string) (int, error) {
	entries, err := os.ReadDir(ArchiveDir(logDir))
	if errors.Is(err, os.ErrNotExist) {
		return 900001, nil
	}
	if err != nil {
		return 0, err
	}
	maxSegment := 900000
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "segment_") || !strings.HasSuffix(name, ".anhe") {
			continue
		}
		segment, err := parseSegmentNumber(name)
		if err != nil {
			continue
		}
		if segment > maxSegment {
			maxSegment = segment
		}
	}
	return maxSegment + 1, nil
}

func ReadAllEvents(dir string) ([]Event, error) {
	paths, err := listAllSegmentPaths(dir)
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
		event, err = unmarshalRecord(scanner.Bytes())
		if err != nil {
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
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) == 0 || len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
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

func listAllSegmentPaths(dir string) ([]string, error) {
	paths, err := listSegmentPaths(dir)
	if err != nil {
		return nil, err
	}
	archiveEntries, err := os.ReadDir(ArchiveDir(dir))
	if errors.Is(err, os.ErrNotExist) {
		sort.Strings(paths)
		return paths, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range archiveEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "segment_") && strings.HasSuffix(name, ".anhe") {
			paths = append(paths, filepath.Join(ArchiveDir(dir), name))
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
