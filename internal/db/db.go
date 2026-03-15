package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"anhebridgedb/internal/config"
	"anhebridgedb/internal/storage"
)

var ErrNotFound = errors.New("key not found")

type Record struct {
	Value     json.RawMessage `json:"value,omitempty"`
	Version   uint64          `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type TimelineEntry struct {
	EventID           uint64          `json:"event_id"`
	Key               string          `json:"key,omitempty"`
	Timestamp         time.Time       `json:"timestamp"`
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
	Diff              any             `json:"diff,omitempty"`
}

type IntegrityIssue struct {
	EventID  uint64 `json:"event_id,omitempty"`
	Segment  string `json:"segment,omitempty"`
	Type     string `json:"type"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

type CheckReport struct {
	Key             string              `json:"key"`
	CheckedVersions int                 `json:"checked_versions"`
	Segments        []string            `json:"segments,omitempty"`
	ManifestStatus  map[string][]string `json:"manifest_status,omitempty"`
	OK              bool                `json:"ok"`
	Issues          []IntegrityIssue    `json:"issues,omitempty"`
}

type StorageVerifyReport struct {
	OK        bool                      `json:"ok"`
	Segments  []storage.SegmentManifest `json:"segments,omitempty"`
	Issues    []IntegrityIssue          `json:"issues,omitempty"`
	CheckedAt time.Time                 `json:"checked_at"`
}

type CompactReport struct {
	KeyIndexCompacted bool      `json:"key_index_compacted"`
	RemovedBuckets    int       `json:"removed_buckets"`
	ArchivedSegments  int       `json:"archived_segments"`
	CompactedSegments int       `json:"compacted_segments"`
	CheckedAt         time.Time `json:"checked_at"`
}

type RuleSpec struct {
	ID      string `json:"id"`
	Pattern string `json:"pattern"`
	Target  string `json:"target"`
	Delay   string `json:"delay"`
	Enabled *bool  `json:"enabled,omitempty"`
}

type SchedulerStats struct {
	Executed uint64 `json:"executed"`
	Skipped  uint64 `json:"skipped"`
	Pending  int    `json:"pending"`
}

type BatchSetItem struct {
	Key            string          `json:"key"`
	Value          json.RawMessage `json:"value"`
	EventName      string          `json:"event_name,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

type BatchGetItem struct {
	Key       string          `json:"key"`
	Found     bool            `json:"found"`
	Value     json.RawMessage `json:"value,omitempty"`
	Version   uint64          `json:"version,omitempty"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
}

type SearchOptions struct {
	Key            string `json:"key,omitempty"`
	EventName      string `json:"event_name,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	WithSameI      bool   `json:"with_same_i"`
	Desc           bool   `json:"desc"`
	Limit          int    `json:"limit"`
	Page           int    `json:"page"`
}

type SearchResultPage struct {
	Page    int             `json:"page"`
	Limit   int             `json:"limit"`
	Total   int             `json:"total"`
	HasMore bool            `json:"has_more"`
	Results []TimelineEntry `json:"results"`
}

const keyLockCount = 256

type Engine struct {
	metaMu         sync.RWMutex
	persistMu      sync.Mutex
	dataDir        string
	log            *storage.EventLog
	keyLocks       [keyLockCount]sync.RWMutex
	state          *recordShardMap
	eventCache     *eventCache
	eventRefs      *eventRefShardMap
	latestVersion  *uint64ShardMap
	keyIndex       *keyIndexShardMap
	rules          map[string]storage.Rule
	buckets        map[time.Time]storage.TaskBucketMeta
	snapshotTime   time.Time
	nextTaskID     uint64
	lastEventID    uint64
	schedulerRuns  uint64
	schedulerExecs uint64
	schedulerSkips uint64
	keyIndexOps    uint64
	metrics        *perfMetrics
	perfCfg        config.PerformanceConfig
	strictRecovery bool
	searchAll      []storage.SearchIndexEntry
	searchByKey    map[string][]storage.SearchIndexEntry
	searchByEvent  map[string][]storage.SearchIndexEntry
	searchByIDKey  map[string][]storage.SearchIndexEntry
	idempotency    map[string]storage.IdempotencyIndexEntry
}

func Open(dataDir string) (*Engine, error) {
	cfg := config.Default()
	return OpenWithConfig(dataDir, cfg.Storage.Segment, cfg.Performance, cfg.Storage.StrictRecovery)
}

func OpenWithConfig(dataDir string, segmentCfg config.SegmentConfig, perfCfg config.PerformanceConfig, strictRecovery bool) (*Engine, error) {
	log, err := storage.OpenEventLog(filepath.Join(dataDir, "log"), storage.SegmentOptions{
		MaxBytes:   segmentCfg.MaxBytes,
		MaxRecords: segmentCfg.MaxRecords,
	})
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		dataDir:        dataDir,
		log:            log,
		rules:          map[string]storage.Rule{},
		buckets:        map[time.Time]storage.TaskBucketMeta{},
		state:          newRecordShardMap(),
		eventRefs:      newEventRefShardMap(),
		latestVersion:  newUint64ShardMap(),
		keyIndex:       newKeyIndexShardMap(),
		eventCache:     newEventCache(perfCfg.EventCacheMaxItems, perfCfg.EventCacheMaxBytes),
		metrics:        newPerfMetrics(),
		perfCfg:        perfCfg,
		strictRecovery: strictRecovery,
		searchByKey:    map[string][]storage.SearchIndexEntry{},
		searchByEvent:  map[string][]storage.SearchIndexEntry{},
		searchByIDKey:  map[string][]storage.SearchIndexEntry{},
		idempotency:    map[string]storage.IdempotencyIndexEntry{},
	}

	if err := engine.recover(); err != nil {
		_ = log.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) recover() error {
	start := time.Now()
	snapshot, err := storage.LoadSnapshot(e.dataDir)
	if err != nil {
		return err
	}
	e.snapshotTime = snapshot.CreatedAt
	for key, value := range snapshot.State {
		e.storeState(key, Record{
			Value:     clone(value.Value),
			Version:   value.Version,
			UpdatedAt: value.UpdatedAt,
		})
	}

	var rules []storage.Rule
	if err := storage.LoadJSON(storage.RulesPath(e.dataDir), &rules); err != nil {
		return err
	}
	for _, rule := range rules {
		e.rules[rule.ID] = rule
	}

	metas, err := storage.LoadTaskBucketMeta(e.dataDir)
	if err != nil {
		return err
	}
	for bucket, meta := range metas {
		e.buckets[bucket] = meta
	}

	refs, err := storage.LoadPositionIndex(e.dataDir)
	if err != nil {
		return err
	}
	for id, ref := range refs {
		e.storeEventRef(id, ref)
		e.updateLastEventID(id)
	}

	keyIndex, err := storage.LoadKeyIndex(e.dataDir)
	if err != nil {
		return err
	}
	for key, entry := range keyIndex {
		e.storeKeyIndex(key, entry)
		e.storeLatestVersion(key, entry.LatestEvent)
	}

	searchEntries, err := storage.LoadSearchIndex(e.dataDir)
	if err != nil {
		return err
	}
	for _, entry := range searchEntries {
		e.storeSearchEntryLocked(entry)
	}

	idempotencyEntries, err := storage.LoadIdempotencyIndex(e.dataDir)
	if err != nil {
		return err
	}
	for _, entry := range idempotencyEntries {
		e.idempotency[entry.CompositeKey] = entry
	}
	searchIndexExists := storage.SearchIndexExists(e.dataDir)
	idempotencyIndexExists := storage.IdempotencyIndexExists(e.dataDir)

	lastEventID := e.lastEventIDLocked()
	repairedRefs := false
	for eventID := snapshot.LastEventID + 1; eventID <= lastEventID; eventID++ {
		ref, ok := e.loadEventRef(eventID)
		if !ok {
			continue
		}
		event, err := storage.ReadEventAt(e.dataDir, ref)
		if err != nil {
			if repairedRefs {
				return err
			}
			refs, rebuildErr := storage.RebuildPositionIndex(e.dataDir)
			if rebuildErr != nil {
				return err
			}
			for id, repairedRef := range refs {
				e.storeEventRef(id, repairedRef)
			}
			repairedRefs = true
			ref, ok = e.loadEventRef(eventID)
			if !ok {
				continue
			}
			event, err = storage.ReadEventAt(e.dataDir, ref)
			if err != nil {
				return err
			}
		}
		e.eventCache.add(event)
		e.storeLatestVersion(event.Key, event.EventID)
		e.storeKeyIndex(event.Key, storage.KeyIndexEntry{
			Key:         event.Key,
			LatestEvent: event.EventID,
			UpdatedAt:   event.Timestamp,
		})
		e.applyLocked(event)
	}
	if needsSearchRebuild(searchIndexExists, lastEventID, searchEntries) || needsIdempotencyRebuild(idempotencyIndexExists, idempotencyEntries) {
		if err := e.rebuildSecondaryIndexesLocked(); err != nil {
			return err
		}
	}
	if e.strictRecovery {
		if err := e.verifyStorageIntegrity(); err != nil {
			return err
		}
	}
	e.metrics.recovery.observe(time.Since(start))
	return nil
}

func (e *Engine) verifyStorageIntegrity() error {
	manifests, err := storage.LoadSegmentManifests(filepath.Join(e.dataDir, "log"))
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		issues, err := storage.ValidateSegmentManifest(filepath.Join(e.dataDir, "log"), manifest.Segment)
		if err != nil {
			return err
		}
		if len(issues) > 0 {
			return fmt.Errorf("strict recovery failed for %s: %s", manifest.Segment, strings.Join(issues, "; "))
		}
	}
	events, err := storage.ReadAllEvents(filepath.Join(e.dataDir, "log"))
	if err != nil {
		return err
	}
	prev := ""
	for _, event := range events {
		expected, err := storage.ComputeEventAuthTag(event, prev)
		if err != nil {
			return err
		}
		if expected != event.AuthTag {
			return fmt.Errorf("strict recovery failed at event %d: auth_tag mismatch", event.EventID)
		}
		prev = event.AuthTag
	}
	return nil
}

func (e *Engine) VerifyStorage() (StorageVerifyReport, error) {
	start := time.Now()
	report := StorageVerifyReport{OK: true, CheckedAt: start}
	manifests, err := storage.LoadSegmentManifests(filepath.Join(e.dataDir, "log"))
	if err != nil {
		return StorageVerifyReport{}, err
	}
	report.Segments = manifests
	for _, manifest := range manifests {
		issues, err := storage.ValidateSegmentManifest(filepath.Join(e.dataDir, "log"), manifest.Segment)
		if err != nil {
			return StorageVerifyReport{}, err
		}
		for _, issue := range issues {
			report.Issues = append(report.Issues, IntegrityIssue{
				Segment: manifest.Segment,
				Type:    "manifest_mismatch",
				Detail:  issue,
			})
		}
	}
	events, err := storage.ReadAllEvents(filepath.Join(e.dataDir, "log"))
	if err != nil {
		return StorageVerifyReport{}, err
	}
	prev := ""
	for _, event := range events {
		expected, err := storage.ComputeEventAuthTag(event, prev)
		if err != nil {
			return StorageVerifyReport{}, err
		}
		if expected != event.AuthTag {
			report.Issues = append(report.Issues, IntegrityIssue{
				EventID:  event.EventID,
				Type:     "auth_tag_mismatch",
				Expected: expected,
				Actual:   event.AuthTag,
			})
		}
		prev = event.AuthTag
	}
	report.OK = len(report.Issues) == 0
	return report, nil
}

func (e *Engine) CompactStorage() (CompactReport, error) {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()

	entries := map[string]storage.KeyIndexEntry{}
	e.keyIndex.rangeAll(func(key string, value storage.KeyIndexEntry) bool {
		entries[key] = value
		return true
	})
	if err := storage.CompactKeyIndex(e.dataDir, entries); err != nil {
		return CompactReport{}, err
	}

	removed := 0
	now := time.Now().UTC()
	for bucket, meta := range e.buckets {
		if meta.Pending > 0 || bucket.After(now.Add(-1*time.Hour)) {
			continue
		}
		if err := storage.SaveTaskBucket(e.dataDir, bucket, nil); err != nil {
			return CompactReport{}, err
		}
		delete(e.buckets, bucket)
		removed++
	}
	logStats := e.log.Stats()
	currentSegment, _ := logStats["current_segment"].(string)
	archived, err := storage.ArchiveColdSegments(filepath.Join(e.dataDir, "log"), currentSegment, e.snapshotLastEventID())
	if err != nil {
		return CompactReport{}, err
	}
	compacted, err := storage.CompactArchivedSegments(e.dataDir, currentSegment, e.snapshotLastEventID())
	if err != nil {
		return CompactReport{}, err
	}
	if compacted > 0 {
		refs, err := storage.LoadPositionIndex(e.dataDir)
		if err != nil {
			return CompactReport{}, err
		}
		for id, ref := range refs {
			e.storeEventRef(id, ref)
		}
	}
	return CompactReport{
		KeyIndexCompacted: true,
		RemovedBuckets:    removed,
		ArchivedSegments:  archived,
		CompactedSegments: compacted,
		CheckedAt:         now,
	}, nil
}

func (e *Engine) Close() error {
	return e.log.Close()
}

func (e *Engine) DataDir() string {
	return e.dataDir
}

func (e *Engine) StartScheduler(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				_, _ = e.ProcessDueTasks(now.UTC())
			}
		}
	}()
}

func (e *Engine) Set(key string, value json.RawMessage) (storage.Event, error) {
	return e.setWithMeta(key, value, "api", "user", "", "", "", 0)
}

func (e *Engine) SetInternal(key string, value json.RawMessage, source, actor, ruleID string, causeEventID uint64) (storage.Event, error) {
	return e.setWithMeta(key, value, source, actor, ruleID, "", "", causeEventID)
}

func (e *Engine) SetWithEventName(key string, value json.RawMessage, eventName string) (storage.Event, error) {
	return e.setWithMeta(key, value, "api", "user", "", eventName, "", 0)
}

func (e *Engine) SetWithIdempotencyKey(key string, value json.RawMessage, eventName, idempotencyKey string) (storage.Event, error) {
	return e.setWithMeta(key, value, "api", "user", "", eventName, idempotencyKey, 0)
}

func (e *Engine) BatchSet(items []BatchSetItem) ([]storage.Event, error) {
	start := time.Now()
	defer observe(e.metrics.aset, start)
	if len(items) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	unlock := e.lockKeys(keys)
	defer unlock()

	results := make([]storage.Event, len(items))
	events := make([]storage.Event, 0, len(items))
	eventIndexes := make([]int, 0, len(items))
	batchIdempotency := map[string]string{}
	for idx, item := range items {
		if item.IdempotencyKey != "" {
			composite := buildIdempotencyCompositeKey(item.Key, item.EventName, item.IdempotencyKey)
			hash, err := computeIdempotencyHash(item.Key, item.EventName, item.Value)
			if err != nil {
				return nil, err
			}
			if existingHash, ok := batchIdempotency[composite]; ok {
				if existingHash != hash {
					return nil, fmt.Errorf("idempotency key conflict in batch for %s", composite)
				}
				return nil, fmt.Errorf("duplicate idempotency key in batch for %s", composite)
			}
			batchIdempotency[composite] = hash
			if existing, ok, err := e.resolveIdempotencyLocked(item.Key, item.EventName, item.IdempotencyKey, hash); err != nil {
				return nil, err
			} else if ok {
				results[idx] = existing
				continue
			}
		}
		value, err := e.resolveValueLocked(item.Key, item.Value, item.EventName)
		if err != nil {
			return nil, err
		}
		current, exists := e.loadState(item.Key)
		op := "CREATE"
		if exists {
			op = "UPDATE"
		}
		events = append(events, storage.Event{
			Timestamp:         e.nextTimestampLocked(),
			Key:               item.Key,
			Operation:         op,
			EventName:         item.EventName,
			IdempotencyKey:    item.IdempotencyKey,
			OldValue:          clone(current.Value),
			NewValue:          clone(value),
			Source:            "api",
			Actor:             "user",
			PrevVersionOffset: current.Version,
		})
		if item.IdempotencyKey != "" {
			hash, err := computeIdempotencyHash(item.Key, item.EventName, item.Value)
			if err != nil {
				return nil, err
			}
			events[len(events)-1].IdempotencyHash = hash
		}
		eventIndexes = append(eventIndexes, idx)
	}

	persisted := []storage.Event{}
	if len(events) > 0 {
		var err error
		persisted, err = e.log.AppendBatch(events)
		if err != nil {
			return nil, err
		}
	}
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	for i, event := range persisted {
		e.applyPersistedLocked(event)
		if err := e.scheduleMatchingRulesLocked(event, false); err != nil {
			return nil, err
		}
		results[eventIndexes[i]] = event
	}
	if err := e.persistRuleStateLocked(); err != nil {
		return nil, err
	}
	return results, nil
}

func (e *Engine) setWithMeta(key string, value json.RawMessage, source, actor, ruleID, eventName, idempotencyKey string, causeEventID uint64) (storage.Event, error) {
	start := time.Now()
	defer observe(e.metrics.set, start)
	unlock := e.lockKey(key)
	defer unlock()

	requestHash := ""
	if idempotencyKey != "" {
		var err error
		requestHash, err = computeIdempotencyHash(key, eventName, value)
		if err != nil {
			return storage.Event{}, err
		}
		if existing, ok, err := e.resolveIdempotencyLocked(key, eventName, idempotencyKey, requestHash); err != nil {
			return storage.Event{}, err
		} else if ok {
			return existing, nil
		}
	}

	value, err := e.resolveValueLocked(key, value, eventName)
	if err != nil {
		return storage.Event{}, err
	}
	current, exists := e.loadState(key)
	op := "CREATE"
	if exists {
		op = "UPDATE"
	}
	if ruleID != "" {
		op = "AUTO_TRANSITION"
	}

	event := storage.Event{
		Timestamp:         e.nextTimestampLocked(),
		Key:               key,
		Operation:         op,
		EventName:         eventName,
		IdempotencyKey:    idempotencyKey,
		IdempotencyHash:   requestHash,
		OldValue:          clone(current.Value),
		NewValue:          clone(value),
		Source:            source,
		Actor:             actor,
		RuleID:            ruleID,
		CauseEventID:      causeEventID,
		PrevVersionOffset: current.Version,
	}
	persisted, err := e.log.Append(event)
	if err != nil {
		return storage.Event{}, err
	}
	observe(e.metrics.append, start)
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	e.applyPersistedLocked(persisted)
	if err := e.scheduleMatchingRulesLocked(persisted, true); err != nil {
		return storage.Event{}, err
	}
	return persisted, nil
}

func (e *Engine) Delete(key string) (storage.Event, error) {
	start := time.Now()
	defer observe(e.metrics.set, start)
	unlock := e.lockKey(key)
	defer unlock()

	current, exists := e.loadState(key)
	if !exists {
		return storage.Event{}, ErrNotFound
	}
	event := storage.Event{
		Timestamp:         e.nextTimestampLocked(),
		Key:               key,
		Operation:         "DELETE",
		OldValue:          clone(current.Value),
		Source:            "api",
		Actor:             "user",
		PrevVersionOffset: current.Version,
	}
	persisted, err := e.log.Append(event)
	if err != nil {
		return storage.Event{}, err
	}
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	e.applyPersistedLocked(persisted)
	return persisted, nil
}

func (e *Engine) Get(key string) (Record, error) {
	start := time.Now()
	defer observe(e.metrics.get, start)
	unlock := e.rlockKey(key)
	defer unlock()
	return e.getLocked(key)
}

func (e *Engine) BatchGet(keys []string) []BatchGetItem {
	start := time.Now()
	defer observe(e.metrics.get, start)
	unlock := e.rlockKeys(keys)
	defer unlock()

	items := make([]BatchGetItem, 0, len(keys))
	for _, key := range keys {
		record, err := e.getLocked(key)
		if err != nil {
			items = append(items, BatchGetItem{Key: key, Found: false})
			continue
		}
		updatedAt := record.UpdatedAt
		items = append(items, BatchGetItem{
			Key:       key,
			Found:     true,
			Value:     clone(record.Value),
			Version:   record.Version,
			UpdatedAt: &updatedAt,
		})
	}
	return items
}

func (e *Engine) GetAt(key string, at time.Time) (Record, error) {
	start := time.Now()
	defer observe(e.metrics.getAt, start)
	unlock := e.rlockKey(key)
	defer unlock()

	version := e.loadLatestVersion(key)
	for version != 0 {
		ref, ok := e.loadEventRef(version)
		if !ok {
			break
		}
		if ref.Timestamp.After(at) {
			version = ref.PrevEventID
			continue
		}
		event, ok := e.loadEventLocked(version)
		if !ok {
			break
		}
		if event.Operation == "DELETE" || (event.Operation == "ROLLBACK" && len(event.NewValue) == 0) {
			return Record{}, ErrNotFound
		}
		return Record{Value: clone(event.NewValue), Version: event.EventID, UpdatedAt: event.Timestamp}, nil
	}

	if !e.snapshotTime.IsZero() && !at.Before(e.snapshotTime) {
		if record, ok := e.loadState(key); ok && !record.UpdatedAt.After(at) {
			return cloneRecord(record), nil
		}
	}
	return Record{}, ErrNotFound
}

func (e *Engine) GetLast(key string, steps int) (Record, error) {
	unlock := e.rlockKey(key)
	defer unlock()

	if steps < 1 {
		steps = 1
	}
	version := e.loadLatestVersion(key)
	if version == 0 {
		return Record{}, ErrNotFound
	}
	for i := 0; i < steps; i++ {
		event, ok := e.loadEventLocked(version)
		if !ok || event.PrevVersionOffset == 0 {
			return Record{}, ErrNotFound
		}
		version = event.PrevVersionOffset
	}
	event, ok := e.loadEventLocked(version)
	if !ok {
		return Record{}, ErrNotFound
	}
	if event.Operation == "DELETE" || (event.Operation == "ROLLBACK" && len(event.NewValue) == 0) {
		return Record{}, ErrNotFound
	}
	return Record{Value: clone(event.NewValue), Version: event.EventID, UpdatedAt: event.Timestamp}, nil
}

func (e *Engine) Timeline(key string, withDiff bool) ([]TimelineEntry, error) {
	return e.TimelineWindow(key, withDiff, e.perfCfg.TimelineDefaultLimit, 0, 0)
}

func (e *Engine) TimelineWindow(key string, withDiff bool, limit int, beforeVersion, afterVersion uint64) ([]TimelineEntry, error) {
	start := time.Now()
	defer observe(e.metrics.timeline, start)
	unlock := e.rlockKey(key)
	defer unlock()

	var timeline []TimelineEntry
	startVersion := e.loadLatestVersion(key)
	if beforeVersion != 0 {
		startVersion = beforeVersion
	}
	for version := startVersion; version != 0; {
		event, ok := e.loadEventLocked(version)
		if !ok {
			break
		}
		if afterVersion != 0 && event.EventID <= afterVersion {
			break
		}
		entry := TimelineEntry{
			EventID:           event.EventID,
			Key:               event.Key,
			Timestamp:         event.Timestamp,
			Operation:         event.Operation,
			EventName:         event.EventName,
			IdempotencyKey:    event.IdempotencyKey,
			IdempotencyHash:   event.IdempotencyHash,
			OldValue:          clone(event.OldValue),
			NewValue:          clone(event.NewValue),
			Source:            event.Source,
			Actor:             event.Actor,
			RuleID:            event.RuleID,
			CauseEventID:      event.CauseEventID,
			PrevVersionOffset: event.PrevVersionOffset,
			AuthTag:           event.AuthTag,
		}
		if withDiff {
			entry.Diff = buildDiff(event.OldValue, event.NewValue)
		}
		timeline = append(timeline, entry)
		if limit > 0 && len(timeline) >= limit {
			break
		}
		version = event.PrevVersionOffset
	}
	if len(timeline) == 0 {
		return nil, ErrNotFound
	}
	sort.Slice(timeline, func(i, j int) bool { return timeline[i].EventID < timeline[j].EventID })
	return timeline, nil
}

func (e *Engine) SearchEvents(options SearchOptions) (SearchResultPage, error) {
	if options.Limit <= 0 {
		options.Limit = e.perfCfg.TimelineDefaultLimit
		if options.Limit <= 0 {
			options.Limit = 50
		}
	}
	if options.Page <= 0 {
		options.Page = 1
	}

	e.metaMu.RLock()
	candidates := e.selectSearchCandidatesLocked(options)
	filtered := make([]storage.SearchIndexEntry, 0, len(candidates))
	for _, entry := range candidates {
		if matchesSearchEntry(entry, options) {
			filtered = append(filtered, entry)
		}
	}
	if options.WithSameI {
		keys := map[uint64]struct{}{}
		idempotencyKeys := make([]string, 0, len(filtered))
		for _, entry := range filtered {
			if entry.IdempotencyKey == "" {
				continue
			}
			idempotencyKeys = append(idempotencyKeys, entry.IdempotencyKey)
		}
		for _, idempotencyKey := range idempotencyKeys {
			for _, linked := range e.searchByIDKey[idempotencyKey] {
				keys[linked.EventID] = struct{}{}
			}
		}
		expanded := make([]storage.SearchIndexEntry, 0, len(keys))
		for _, entry := range e.searchAll {
			if _, ok := keys[entry.EventID]; ok {
				expanded = append(expanded, entry)
			}
		}
		filtered = expanded
	}
	total := len(filtered)
	page := options.Page
	limit := options.Limit
	start := (page - 1) * limit
	if start >= total {
		e.metaMu.RUnlock()
		return SearchResultPage{Page: page, Limit: limit, Total: total, Results: []TimelineEntry{}}, nil
	}

	ordered := make([]storage.SearchIndexEntry, 0, limit)
	if options.Desc {
		for i := total - 1 - start; i >= 0 && len(ordered) < limit; i-- {
			ordered = append(ordered, filtered[i])
		}
	} else {
		for i := start; i < total && len(ordered) < limit; i++ {
			ordered = append(ordered, filtered[i])
		}
	}
	e.metaMu.RUnlock()

	results := make([]TimelineEntry, 0, len(ordered))
	for _, entry := range ordered {
		event, ok := e.loadEventLocked(entry.EventID)
		if !ok {
			continue
		}
		results = append(results, timelineEntryFromEvent(event, false))
	}
	if len(results) == 0 && total == 0 {
		return SearchResultPage{Page: page, Limit: limit, Total: 0, Results: []TimelineEntry{}}, nil
	}
	if !options.Desc {
		sort.Slice(results, func(i, j int) bool { return results[i].EventID < results[j].EventID })
	}
	return SearchResultPage{
		Page:    page,
		Limit:   limit,
		Total:   total,
		HasMore: page*limit < total,
		Results: results,
	}, nil
}

func (e *Engine) CreateRule(spec RuleSpec) (storage.Rule, error) {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()

	if spec.ID == "" {
		return storage.Rule{}, errors.New("rule id is required")
	}
	if spec.Pattern == "" || spec.Target == "" || spec.Delay == "" {
		return storage.Rule{}, errors.New("pattern, target and delay are required")
	}
	delay, err := time.ParseDuration(spec.Delay)
	if err != nil {
		return storage.Rule{}, fmt.Errorf("invalid delay: %w", err)
	}
	enabled := true
	if spec.Enabled != nil {
		enabled = *spec.Enabled
	}
	rule := storage.Rule{
		ID:           spec.ID,
		Pattern:      spec.Pattern,
		Target:       spec.Target,
		Delay:        spec.Delay,
		DelaySeconds: int64(delay / time.Second),
		Enabled:      enabled,
		CreatedAt:    time.Now().UTC(),
	}
	if existing, ok := e.rules[rule.ID]; ok {
		rule.CreatedAt = existing.CreatedAt
		rule.MatchCount = existing.MatchCount
		rule.ExecuteCount = existing.ExecuteCount
		rule.SkipCount = existing.SkipCount
	}
	e.rules[rule.ID] = rule
	if err := e.persistRulesLocked(); err != nil {
		return storage.Rule{}, err
	}
	return rule, nil
}

func (e *Engine) ListRules() []storage.Rule {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	rules := make([]storage.Rule, 0, len(e.rules))
	for _, rule := range e.rules {
		rules = append(rules, rule)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	return rules
}

func (e *Engine) GetRule(id string) (storage.Rule, error) {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	rule, ok := e.rules[id]
	if !ok {
		return storage.Rule{}, ErrNotFound
	}
	return rule, nil
}

func (e *Engine) ListTasks() []storage.Task {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	tasks, err := storage.LoadAllTasks(e.dataDir)
	if err != nil {
		return nil
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].BucketTS.Equal(tasks[j].BucketTS) {
			return tasks[i].ID < tasks[j].ID
		}
		return tasks[i].BucketTS.Before(tasks[j].BucketTS)
	})
	return tasks
}

func (e *Engine) ProcessDueTasks(now time.Time) (SchedulerStats, error) {
	start := time.Now()
	defer observe(e.metrics.scheduler, start)
	atomic.AddUint64(&e.schedulerRuns, 1)
	paths, err := storage.DueTaskBucketPaths(e.dataDir, now)
	if err != nil {
		return SchedulerStats{}, err
	}

	var due []*storage.Task
	for _, path := range paths {
		var bucketTasks []storage.Task
		if err := storage.LoadJSON(path, &bucketTasks); err != nil {
			return SchedulerStats{}, err
		}
		for _, task := range bucketTasks {
			if task.Status == "pending" && !task.BucketTS.After(now) {
				taskCopy := task
				due = append(due, &taskCopy)
			}
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].BucketTS.Before(due[j].BucketTS) })

	var executed, skipped uint64
	updatedBuckets := map[time.Time][]storage.Task{}
	var updatedMu sync.Mutex
	errCh := make(chan error, 1)
	workerCount := e.perfCfg.SchedulerWorkers
	if workerCount < 1 {
		workerCount = 1
	}
	taskCh := make(chan *storage.Task)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for pending := range taskCh {
			unlock := e.lockKey(pending.EntityKey)
			e.metaMu.Lock()
			task := *pending
			record, ok := e.loadState(task.EntityKey)
			if !ok || record.Version != task.ExpectedVersion || extractState(record.Value) != task.ExpectedState {
				nowCopy := now
				task.Status = "skipped"
				task.ProcessedAt = &nowCopy
				rule := e.rules[task.RuleID]
				rule.SkipCount++
				e.rules[task.RuleID] = rule
				atomic.AddUint64(&skipped, 1)
				updatedMu.Lock()
				updatedBuckets[task.BucketTS] = upsertTask(updatedBuckets[task.BucketTS], task)
				updatedMu.Unlock()
				e.metaMu.Unlock()
				unlock()
				continue
			}

			nextValue, err := transitionValue(record.Value, task.ToState)
			if err != nil {
				nowCopy := now
				task.Status = "skipped"
				task.ProcessedAt = &nowCopy
				rule := e.rules[task.RuleID]
				rule.SkipCount++
				e.rules[task.RuleID] = rule
				atomic.AddUint64(&skipped, 1)
				updatedMu.Lock()
				updatedBuckets[task.BucketTS] = upsertTask(updatedBuckets[task.BucketTS], task)
				updatedMu.Unlock()
				e.metaMu.Unlock()
				unlock()
				continue
			}

			event := storage.Event{
				Timestamp:         e.nextTimestampLocked(),
				Key:               task.EntityKey,
				Operation:         "AUTO_TRANSITION",
				EventName:         "auto_transition",
				OldValue:          clone(record.Value),
				NewValue:          clone(nextValue),
				Source:            "rule_engine",
				Actor:             "scheduler",
				RuleID:            task.RuleID,
				CauseEventID:      task.CauseEventID,
				PrevVersionOffset: record.Version,
			}
			persisted, err := e.log.Append(event)
			if err != nil {
				e.metaMu.Unlock()
				unlock()
				select {
				case errCh <- err:
				default:
				}
				return
			}
			e.applyPersistedLocked(persisted)
			_ = e.scheduleMatchingRulesLocked(persisted, true)

			nowCopy := now
			task.Status = "done"
			task.ProcessedAt = &nowCopy
			rule := e.rules[task.RuleID]
			rule.ExecuteCount++
			e.rules[task.RuleID] = rule
			atomic.AddUint64(&executed, 1)
			updatedMu.Lock()
			updatedBuckets[task.BucketTS] = upsertTask(updatedBuckets[task.BucketTS], task)
			updatedMu.Unlock()
			e.metaMu.Unlock()
			unlock()
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	for _, pending := range due {
		taskCh <- pending
	}
	close(taskCh)
	wg.Wait()
	select {
	case err := <-errCh:
		return SchedulerStats{}, err
	default:
	}

	e.metaMu.Lock()
	for bucket, changed := range updatedBuckets {
		tasks, err := storage.LoadTaskBucket(e.dataDir, bucket)
		if err != nil {
			e.metaMu.Unlock()
			return SchedulerStats{}, err
		}
		for _, task := range changed {
			tasks = upsertTask(tasks, task)
		}
		if err := storage.SaveTaskBucket(e.dataDir, bucket, tasks); err != nil {
			e.metaMu.Unlock()
			return SchedulerStats{}, err
		}
		meta := summarizeBucket(bucket, storage.TaskBucketPath(e.dataDir, bucket), tasks)
		e.buckets[bucket] = meta
	}
	if err := e.persistRulesLocked(); err != nil {
		e.metaMu.Unlock()
		return SchedulerStats{}, err
	}
	pendingCount := e.pendingTasksLocked()
	e.metaMu.Unlock()
	atomic.AddUint64(&e.schedulerExecs, executed)
	atomic.AddUint64(&e.schedulerSkips, skipped)
	return SchedulerStats{Executed: executed, Skipped: skipped, Pending: pendingCount}, nil
}

func (e *Engine) Snapshot() (storage.Snapshot, error) {
	snapshot := storage.Snapshot{
		CreatedAt:   time.Now().UTC(),
		LastEventID: e.lastEventIDLocked(),
		State:       map[string]storage.SnapshotRecord{},
	}
	e.state.rangeAll(func(key string, record Record) bool {
		snapshot.State[key] = storage.SnapshotRecord{
			Value:     clone(record.Value),
			Version:   record.Version,
			UpdatedAt: record.UpdatedAt,
		}
		return true
	})

	if err := storage.SaveSnapshot(e.dataDir, snapshot); err != nil {
		return storage.Snapshot{}, err
	}
	e.metaMu.Lock()
	e.snapshotTime = snapshot.CreatedAt
	e.metaMu.Unlock()
	return snapshot, nil
}

func (e *Engine) Stats() map[string]any {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	keys := e.state.count()
	eventRefs := e.eventRefs.count()
	loadedEvents := e.eventCache.stats()["event_cache_items"].(int)
	stats := map[string]any{
		"keys":               keys,
		"event_refs":         eventRefs,
		"loaded_events":      loadedEvents,
		"rules":              len(e.rules),
		"tasks":              e.countAllTasksLocked(),
		"pending_tasks":      e.pendingTasksLocked(),
		"last_event_id":      e.lastEventIDLocked(),
		"scheduler_runs":     atomic.LoadUint64(&e.schedulerRuns),
		"scheduler_executed": atomic.LoadUint64(&e.schedulerExecs),
		"scheduler_skipped":  atomic.LoadUint64(&e.schedulerSkips),
	}
	for key, value := range e.log.Stats() {
		stats[key] = value
	}
	return stats
}

func (e *Engine) Rollback(key string, version uint64) (storage.Event, error) {
	unlock := e.lockKey(key)
	defer unlock()

	target, err := e.findVersionEventLocked(key, version)
	if err != nil {
		return storage.Event{}, err
	}
	current, exists := e.loadState(key)
	var oldValue json.RawMessage
	if exists {
		oldValue = clone(current.Value)
	}
	event := storage.Event{
		Timestamp:         e.nextTimestampLocked(),
		Key:               key,
		Operation:         "ROLLBACK",
		EventName:         "rollback",
		OldValue:          oldValue,
		NewValue:          clone(target.NewValue),
		Source:            "api",
		Actor:             "user",
		CauseEventID:      target.EventID,
		PrevVersionOffset: e.loadLatestVersion(key),
	}
	if target.Operation == "DELETE" || (target.Operation == "ROLLBACK" && len(target.NewValue) == 0) {
		event.NewValue = nil
	}
	persisted, err := e.log.Append(event)
	if err != nil {
		return storage.Event{}, err
	}
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	e.applyPersistedLocked(persisted)
	if err := e.scheduleMatchingRulesLocked(persisted, true); err != nil {
		return storage.Event{}, err
	}
	return persisted, nil
}

func (e *Engine) RollbackLast(key string) (storage.Event, error) {
	record, err := e.GetLast(key, 1)
	if err != nil {
		return storage.Event{}, err
	}
	return e.Rollback(key, record.Version)
}

func (e *Engine) CheckAllTime(key string) (CheckReport, error) {
	return e.CheckAllTimeWindow(key, e.perfCfg.TimelineDefaultLimit, 0, 0)
}

func (e *Engine) CheckAllTimeWindow(key string, limit int, beforeVersion, afterVersion uint64) (CheckReport, error) {
	start := time.Now()
	defer observe(e.metrics.check, start)
	unlock := e.rlockKey(key)
	defer unlock()

	versions := make([]storage.Event, 0)
	segments := map[string]struct{}{}
	startVersion := e.loadLatestVersion(key)
	if beforeVersion != 0 {
		startVersion = beforeVersion
	}
	for version := startVersion; version != 0; {
		event, ok := e.loadEventLocked(version)
		if !ok {
			break
		}
		if afterVersion != 0 && event.EventID <= afterVersion {
			break
		}
		versions = append(versions, event)
		if ref, ok := e.loadEventRef(event.EventID); ok && ref.Segment != "" {
			segments[ref.Segment] = struct{}{}
		}
		if limit > 0 && len(versions) >= limit {
			break
		}
		version = event.PrevVersionOffset
	}
	if len(versions) == 0 {
		return CheckReport{}, ErrNotFound
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].EventID < versions[j].EventID })

	report := CheckReport{
		Key:             key,
		CheckedVersions: len(versions),
		ManifestStatus:  map[string][]string{},
		OK:              true,
	}

	for _, event := range versions {
		expectedPreviousTag := ""
		if event.EventID > 1 {
			prevEvent, ok := e.loadEventLocked(event.EventID - 1)
			if !ok {
				report.Issues = append(report.Issues, IntegrityIssue{
					EventID: event.EventID,
					Type:    "previous_event_missing",
					Detail:  fmt.Sprintf("event %d expects previous global event %d", event.EventID, event.EventID-1),
				})
				continue
			}
			expectedPreviousTag = prevEvent.AuthTag
		}

		expectedAuthTag, err := storage.ComputeEventAuthTag(event, expectedPreviousTag)
		if err != nil {
			return CheckReport{}, err
		}
		if event.AuthTag != expectedAuthTag {
			report.Issues = append(report.Issues, IntegrityIssue{
				EventID:  event.EventID,
				Type:     "auth_tag_mismatch",
				Expected: expectedAuthTag,
				Actual:   event.AuthTag,
			})
		}
	}

	segmentList := make([]string, 0, len(segments))
	for segment := range segments {
		segmentList = append(segmentList, segment)
	}
	sort.Strings(segmentList)
	report.Segments = segmentList
	for _, segment := range segmentList {
		issues, err := storage.ValidateSegmentManifest(filepath.Join(e.dataDir, "log"), segment)
		if err != nil {
			return CheckReport{}, err
		}
		if len(issues) > 0 {
			report.ManifestStatus[segment] = issues
			for _, issue := range issues {
				report.Issues = append(report.Issues, IntegrityIssue{
					Segment: segment,
					Type:    "manifest_mismatch",
					Detail:  issue,
				})
			}
		}
	}

	report.OK = len(report.Issues) == 0
	return report, nil
}

func (e *Engine) applyPersistedLocked(event storage.Event) {
	e.storeEvent(event)
	ref, _ := e.loadEventRef(event.EventID)
	ref.EventID = event.EventID
	ref.Key = event.Key
	ref.Timestamp = event.Timestamp
	ref.PrevEventID = event.PrevVersionOffset
	e.storeEventRef(event.EventID, ref)
	e.storeLatestVersion(event.Key, event.EventID)
	e.storeKeyIndex(event.Key, storage.KeyIndexEntry{Key: event.Key, LatestEvent: event.EventID, UpdatedAt: event.Timestamp})
	searchEntry := storage.SearchIndexEntry{
		EventID:        event.EventID,
		Key:            event.Key,
		EventName:      event.EventName,
		IdempotencyKey: event.IdempotencyKey,
		Timestamp:      event.Timestamp,
	}
	e.storeSearchEntryLocked(searchEntry)
	if event.IdempotencyKey != "" {
		e.idempotency[buildIdempotencyCompositeKey(event.Key, event.EventName, event.IdempotencyKey)] = storage.IdempotencyIndexEntry{
			CompositeKey:   buildIdempotencyCompositeKey(event.Key, event.EventName, event.IdempotencyKey),
			Key:            event.Key,
			EventName:      event.EventName,
			IdempotencyKey: event.IdempotencyKey,
			EventID:        event.EventID,
			RequestHash:    event.IdempotencyHash,
			Timestamp:      event.Timestamp,
		}
	}
	e.updateLastEventID(event.EventID)
	e.applyLocked(event)
	_ = e.persistKeyIndexLocked(storage.KeyIndexEntry{Key: event.Key, LatestEvent: event.EventID, UpdatedAt: event.Timestamp})
	_ = e.persistSearchIndexesLocked(searchEntry, event)
}

func (e *Engine) applyLocked(event storage.Event) {
	switch event.Operation {
	case "DELETE":
		e.deleteState(event.Key)
	case "ROLLBACK":
		if len(event.NewValue) == 0 {
			e.deleteState(event.Key)
		} else {
			e.storeState(event.Key, Record{Value: clone(event.NewValue), Version: event.EventID, UpdatedAt: event.Timestamp})
		}
	default:
		e.storeState(event.Key, Record{Value: clone(event.NewValue), Version: event.EventID, UpdatedAt: event.Timestamp})
	}
}

func (e *Engine) scheduleMatchingRulesLocked(event storage.Event, persist bool) error {
	state := extractState(event.NewValue)
	if state == "" {
		return nil
	}
	for id, rule := range e.rules {
		if !rule.Enabled {
			continue
		}
		match, entityKey, _, toState := matchRule(rule, event.Key, state)
		if !match {
			continue
		}
		taskID := e.nextTaskIDLocked()
		task := storage.Task{
			ID:              taskID,
			BucketTS:        event.Timestamp.Add(time.Duration(rule.DelaySeconds) * time.Second).UTC().Truncate(time.Minute),
			RuleID:          id,
			EntityKey:       entityKey,
			ExpectedVersion: event.EventID,
			ExpectedState:   state,
			ToState:         toState,
			CauseEventID:    event.EventID,
			Status:          "pending",
			CreatedAt:       event.Timestamp,
		}
		if err := storage.AppendTaskBucket(e.dataDir, task.BucketTS, task); err != nil {
			return err
		}
		tasks, err := storage.LoadTaskBucket(e.dataDir, task.BucketTS)
		if err != nil {
			return err
		}
		e.buckets[task.BucketTS] = summarizeBucket(task.BucketTS, storage.TaskBucketPath(e.dataDir, task.BucketTS), tasks)
		rule.MatchCount++
		e.rules[id] = rule
	}
	if persist {
		return e.persistRuleStateLocked()
	}
	return nil
}

func (e *Engine) persistRuleStateLocked() error {
	return e.persistRulesLocked()
}

func (e *Engine) persistRulesLocked() error {
	rules := make([]storage.Rule, 0, len(e.rules))
	for _, rule := range e.rules {
		rules = append(rules, rule)
	}
	sort.Slice(rules, func(i, j int) bool { return rules[i].ID < rules[j].ID })
	return storage.SaveJSON(storage.RulesPath(e.dataDir), rules)
}

func (e *Engine) persistKeyIndexLocked(entry storage.KeyIndexEntry) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	start := time.Now()
	defer observe(e.metrics.keyIndexFlush, start)
	if entry.Key != "" {
		if err := storage.AppendKeyIndexDelta(e.dataDir, entry); err != nil {
			return err
		}
	}
	e.keyIndexOps++
	if e.keyIndexOps%uint64(e.perfCfg.KeyIndexCompactOps) != 0 {
		return nil
	}
	entries := map[string]storage.KeyIndexEntry{}
	e.keyIndex.rangeAll(func(key string, value storage.KeyIndexEntry) bool {
		entries[key] = value
		return true
	})
	return storage.CompactKeyIndex(e.dataDir, entries)
}

func (e *Engine) persistSearchIndexesLocked(searchEntry storage.SearchIndexEntry, event storage.Event) error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	if err := storage.AppendSearchIndexEntry(e.dataDir, searchEntry); err != nil {
		return err
	}
	if event.IdempotencyKey == "" {
		return nil
	}
	return storage.AppendIdempotencyIndexEntry(e.dataDir, storage.IdempotencyIndexEntry{
		CompositeKey:   buildIdempotencyCompositeKey(event.Key, event.EventName, event.IdempotencyKey),
		Key:            event.Key,
		EventName:      event.EventName,
		IdempotencyKey: event.IdempotencyKey,
		EventID:        event.EventID,
		RequestHash:    event.IdempotencyHash,
		Timestamp:      event.Timestamp,
	})
}

func (e *Engine) nextTimestampLocked() time.Time {
	now := time.Now().UTC()
	lastEventID := e.lastEventIDLocked()
	if lastEventID == 0 {
		return now
	}
	if event, ok := e.loadEventLocked(lastEventID); ok && !now.After(event.Timestamp) {
		return event.Timestamp.Add(time.Nanosecond)
	}
	return now
}

func (e *Engine) lastEventIDLocked() uint64 {
	return atomic.LoadUint64(&e.lastEventID)
}

func (e *Engine) nextTaskIDLocked() string {
	e.nextTaskID++
	return fmt.Sprintf("task-%06d", e.nextTaskID)
}

func (e *Engine) findVersionEventLocked(key string, version uint64) (storage.Event, error) {
	event, ok := e.loadEventLocked(version)
	if !ok || event.Key != key {
		return storage.Event{}, ErrNotFound
	}
	return event, nil
}

func (e *Engine) pendingTasksLocked() int {
	total := 0
	for _, meta := range e.buckets {
		total += meta.Pending
	}
	return total
}

func (e *Engine) getLocked(key string) (Record, error) {
	record, ok := e.loadState(key)
	if !ok {
		return Record{}, ErrNotFound
	}
	return cloneRecord(record), nil
}

func (e *Engine) selectSearchCandidatesLocked(options SearchOptions) []storage.SearchIndexEntry {
	switch {
	case options.Key != "":
		return append([]storage.SearchIndexEntry(nil), e.searchByKey[options.Key]...)
	case options.IdempotencyKey != "":
		return append([]storage.SearchIndexEntry(nil), e.searchByIDKey[options.IdempotencyKey]...)
	case options.EventName != "":
		return append([]storage.SearchIndexEntry(nil), e.searchByEvent[searchEventNameKey(options.EventName)]...)
	default:
		return append([]storage.SearchIndexEntry(nil), e.searchAll...)
	}
}

func matchesSearchEntry(entry storage.SearchIndexEntry, options SearchOptions) bool {
	if options.Key != "" && entry.Key != options.Key {
		return false
	}
	if options.EventName != "" && !strings.EqualFold(entry.EventName, options.EventName) {
		return false
	}
	if options.IdempotencyKey != "" && entry.IdempotencyKey != options.IdempotencyKey {
		return false
	}
	return true
}

func (e *Engine) storeSearchEntryLocked(entry storage.SearchIndexEntry) {
	e.searchAll = append(e.searchAll, entry)
	e.searchByKey[entry.Key] = append(e.searchByKey[entry.Key], entry)
	if entry.EventName != "" {
		key := searchEventNameKey(entry.EventName)
		e.searchByEvent[key] = append(e.searchByEvent[key], entry)
	}
	if entry.IdempotencyKey != "" {
		e.searchByIDKey[entry.IdempotencyKey] = append(e.searchByIDKey[entry.IdempotencyKey], entry)
	}
}

func (e *Engine) loadEventLocked(eventID uint64) (storage.Event, bool) {
	if event, ok := e.loadEvent(eventID); ok {
		return event, true
	}
	ref, ok := e.loadEventRef(eventID)
	if !ok {
		return storage.Event{}, false
	}
	event, err := storage.ReadEventAt(e.dataDir, ref)
	if err != nil {
		return storage.Event{}, false
	}
	e.storeEvent(event)
	return event, true
}

func (e *Engine) resolveValueLocked(key string, value json.RawMessage, eventName string) (json.RawMessage, error) {
	if eventName == "" {
		return clone(value), nil
	}
	delta, ok := parseNumericRaw(value)
	if !ok {
		return clone(value), nil
	}
	current, exists := e.loadState(key)
	if !exists {
		return normalizeNumber(delta)
	}
	base, ok := parseNumericRaw(current.Value)
	if !ok {
		return nil, fmt.Errorf("key %s current value is not numeric", key)
	}
	return normalizeNumber(base + delta)
}

func (e *Engine) resolveIdempotencyLocked(key, eventName, idempotencyKey, requestHash string) (storage.Event, bool, error) {
	e.metaMu.RLock()
	entry, ok := e.idempotency[buildIdempotencyCompositeKey(key, eventName, idempotencyKey)]
	e.metaMu.RUnlock()
	if !ok {
		return storage.Event{}, false, nil
	}
	if entry.RequestHash != requestHash {
		return storage.Event{}, false, fmt.Errorf("idempotency key conflict for %s", idempotencyKey)
	}
	event, ok := e.loadEventLocked(entry.EventID)
	if !ok {
		return storage.Event{}, false, fmt.Errorf("idempotency event %d not found", entry.EventID)
	}
	return event, true, nil
}

func parseNumericRaw(value json.RawMessage) (float64, bool) {
	if len(value) == 0 {
		return 0, false
	}
	var number float64
	if err := json.Unmarshal(value, &number); err != nil {
		return 0, false
	}
	return number, true
}

func normalizeNumber(value float64) (json.RawMessage, error) {
	return json.RawMessage(strconv.FormatFloat(value, 'f', -1, 64)), nil
}

func (e *Engine) keyLockIndex(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % keyLockCount)
}

func (e *Engine) lockKey(key string) func() {
	idx := e.keyLockIndex(key)
	e.keyLocks[idx].Lock()
	return e.keyLocks[idx].Unlock
}

func (e *Engine) rlockKey(key string) func() {
	idx := e.keyLockIndex(key)
	e.keyLocks[idx].RLock()
	return e.keyLocks[idx].RUnlock
}

func (e *Engine) lockKeys(keys []string) func() {
	if len(keys) == 0 {
		return func() {}
	}
	indexes := uniqueSortedIndexes(keys, e.keyLockIndex)
	for _, idx := range indexes {
		e.keyLocks[idx].Lock()
	}
	return func() {
		for i := len(indexes) - 1; i >= 0; i-- {
			e.keyLocks[indexes[i]].Unlock()
		}
	}
}

func (e *Engine) rlockKeys(keys []string) func() {
	if len(keys) == 0 {
		return func() {}
	}
	indexes := uniqueSortedIndexes(keys, e.keyLockIndex)
	for _, idx := range indexes {
		e.keyLocks[idx].RLock()
	}
	return func() {
		for i := len(indexes) - 1; i >= 0; i-- {
			e.keyLocks[indexes[i]].RUnlock()
		}
	}
}

func uniqueSortedIndexes(keys []string, fn func(string) int) []int {
	seen := map[int]struct{}{}
	indexes := make([]int, 0, len(keys))
	for _, key := range keys {
		idx := fn(key)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		indexes = append(indexes, idx)
	}
	sort.Ints(indexes)
	return indexes
}

func (e *Engine) loadState(key string) (Record, bool) {
	return e.state.load(key)
}

func (e *Engine) storeState(key string, record Record) {
	e.state.store(key, record)
}

func (e *Engine) deleteState(key string) {
	e.state.delete(key)
}

func (e *Engine) loadLatestVersion(key string) uint64 {
	value, ok := e.latestVersion.load(key)
	if !ok {
		return 0
	}
	return value
}

func (e *Engine) storeLatestVersion(key string, version uint64) {
	e.latestVersion.store(key, version)
}

func (e *Engine) storeKeyIndex(key string, entry storage.KeyIndexEntry) {
	e.keyIndex.store(key, entry)
}

func (e *Engine) loadEventRef(eventID uint64) (storage.EventRef, bool) {
	return e.eventRefs.load(eventID)
}

func (e *Engine) storeEventRef(eventID uint64, ref storage.EventRef) {
	e.eventRefs.store(eventID, ref)
}

func (e *Engine) loadEvent(eventID uint64) (storage.Event, bool) {
	return e.eventCache.get(eventID)
}

func (e *Engine) storeEvent(event storage.Event) {
	e.eventCache.add(event)
}

func (e *Engine) rebuildSecondaryIndexesLocked() error {
	logDir := filepath.Join(e.dataDir, "log")
	events, err := storage.ReadAllEvents(logDir)
	if err != nil {
		return err
	}

	e.searchAll = nil
	e.searchByKey = map[string][]storage.SearchIndexEntry{}
	e.searchByEvent = map[string][]storage.SearchIndexEntry{}
	e.searchByIDKey = map[string][]storage.SearchIndexEntry{}
	e.idempotency = map[string]storage.IdempotencyIndexEntry{}

	searchEntries := make([]storage.SearchIndexEntry, 0, len(events))
	idempotencyEntries := make([]storage.IdempotencyIndexEntry, 0)
	for _, event := range events {
		entry := storage.SearchIndexEntry{
			EventID:        event.EventID,
			Key:            event.Key,
			EventName:      event.EventName,
			IdempotencyKey: event.IdempotencyKey,
			Timestamp:      event.Timestamp,
		}
		e.storeSearchEntryLocked(entry)
		searchEntries = append(searchEntries, entry)
		if event.IdempotencyKey == "" {
			continue
		}
		idEntry := storage.IdempotencyIndexEntry{
			CompositeKey:   buildIdempotencyCompositeKey(event.Key, event.EventName, event.IdempotencyKey),
			Key:            event.Key,
			EventName:      event.EventName,
			IdempotencyKey: event.IdempotencyKey,
			EventID:        event.EventID,
			RequestHash:    event.IdempotencyHash,
			Timestamp:      event.Timestamp,
		}
		e.idempotency[idEntry.CompositeKey] = idEntry
		idempotencyEntries = append(idempotencyEntries, idEntry)
	}
	if err := storage.SaveSearchIndex(e.dataDir, searchEntries); err != nil {
		return err
	}
	if err := storage.SaveIdempotencyIndex(e.dataDir, idempotencyEntries); err != nil {
		return err
	}
	return nil
}

func needsSearchRebuild(indexExists bool, lastEventID uint64, entries []storage.SearchIndexEntry) bool {
	if lastEventID == 0 {
		return false
	}
	if !indexExists {
		return true
	}
	if len(entries) == 0 {
		return true
	}
	return entries[len(entries)-1].EventID != lastEventID
}

func needsIdempotencyRebuild(indexExists bool, entries []storage.IdempotencyIndexEntry) bool {
	return !indexExists && len(entries) == 0
}

func timelineEntryFromEvent(event storage.Event, withDiff bool) TimelineEntry {
	entry := TimelineEntry{
		EventID:           event.EventID,
		Key:               event.Key,
		Timestamp:         event.Timestamp,
		Operation:         event.Operation,
		EventName:         event.EventName,
		IdempotencyKey:    event.IdempotencyKey,
		IdempotencyHash:   event.IdempotencyHash,
		OldValue:          clone(event.OldValue),
		NewValue:          clone(event.NewValue),
		Source:            event.Source,
		Actor:             event.Actor,
		RuleID:            event.RuleID,
		CauseEventID:      event.CauseEventID,
		PrevVersionOffset: event.PrevVersionOffset,
		AuthTag:           event.AuthTag,
	}
	if withDiff {
		entry.Diff = buildDiff(event.OldValue, event.NewValue)
	}
	return entry
}

func searchEventNameKey(name string) string {
	return strings.ToUpper(strings.TrimSpace(name))
}

func buildIdempotencyCompositeKey(key, eventName, idempotencyKey string) string {
	return key + "|" + searchEventNameKey(eventName) + "|" + idempotencyKey
}

func computeIdempotencyHash(key, eventName string, value json.RawMessage) (string, error) {
	normalized := bytesForIdempotency(value)
	sum := sha256.Sum256([]byte(key + "\n" + searchEventNameKey(eventName) + "\n" + string(normalized)))
	return base64.RawURLEncoding.EncodeToString(sum[:]), nil
}

func bytesForIdempotency(value json.RawMessage) []byte {
	if len(value) == 0 {
		return nil
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, value); err == nil {
		return compacted.Bytes()
	}
	return bytes.TrimSpace(value)
}

func (e *Engine) updateLastEventID(eventID uint64) {
	for {
		current := atomic.LoadUint64(&e.lastEventID)
		if eventID <= current {
			return
		}
		if atomic.CompareAndSwapUint64(&e.lastEventID, current, eventID) {
			return
		}
	}
}

func summarizeBucket(bucket time.Time, path string, tasks []storage.Task) storage.TaskBucketMeta {
	meta := storage.TaskBucketMeta{
		BucketTS: bucket,
		Path:     path,
		Total:    len(tasks),
	}
	for _, task := range tasks {
		if task.Status != "pending" {
			continue
		}
		meta.Pending++
		if meta.OldestPending == nil || task.CreatedAt.Before(*meta.OldestPending) {
			ts := task.CreatedAt
			meta.OldestPending = &ts
		}
	}
	return meta
}

func bucketTasksOrNil(dir string, bucket time.Time) []storage.Task {
	tasks, err := storage.LoadTaskBucket(dir, bucket)
	if err != nil {
		return nil
	}
	return tasks
}

func upsertTask(tasks []storage.Task, updated storage.Task) []storage.Task {
	for i := range tasks {
		if tasks[i].ID == updated.ID {
			tasks[i] = updated
			return tasks
		}
	}
	return append(tasks, updated)
}

func (e *Engine) countAllTasksLocked() int {
	total := 0
	for _, meta := range e.buckets {
		total += meta.Total
	}
	return total
}

func (e *Engine) countStateKeys() int {
	return e.state.count()
}

func (e *Engine) countEventRefs() int {
	return e.eventRefs.count()
}

func (e *Engine) snapshotLastEventID() uint64 {
	snapshot, err := storage.LoadSnapshot(e.dataDir)
	if err != nil {
		return 0
	}
	return snapshot.LastEventID
}

func clone(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func cloneRecord(record Record) Record {
	record.Value = clone(record.Value)
	return record
}

func extractState(value json.RawMessage) string {
	if len(value) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(value, &obj); err != nil {
		return ""
	}
	if state, ok := obj["state"].(string); ok {
		return state
	}
	return ""
}

func transitionValue(value json.RawMessage, toState string) (json.RawMessage, error) {
	var obj map[string]any
	if err := json.Unmarshal(value, &obj); err != nil {
		return nil, err
	}
	obj["state"] = toState
	return json.Marshal(obj)
}

func matchRule(rule storage.Rule, key, state string) (bool, string, string, string) {
	parts := strings.Split(rule.Pattern, ":")
	if len(parts) < 3 {
		return false, "", "", ""
	}
	if parts[len(parts)-2] != "*" || parts[len(parts)-1] != state {
		return false, "", "", ""
	}
	prefix := strings.Join(parts[:len(parts)-2], ":") + ":"
	if !strings.HasPrefix(key, prefix) {
		return false, "", "", ""
	}
	entityID := strings.TrimPrefix(key, prefix)
	if entityID == "" {
		return false, "", "", ""
	}
	targetParts := strings.Split(rule.Target, ":")
	if len(targetParts) == 0 {
		return false, "", "", ""
	}
	return true, key, entityID, targetParts[len(targetParts)-1]
}

func buildDiff(oldValue, newValue json.RawMessage) any {
	var oldAny any
	var newAny any
	_ = json.Unmarshal(oldValue, &oldAny)
	_ = json.Unmarshal(newValue, &newAny)

	oldMap, oldOK := oldAny.(map[string]any)
	newMap, newOK := newAny.(map[string]any)
	if !oldOK || !newOK {
		if reflect.DeepEqual(oldAny, newAny) {
			return map[string]any{}
		}
		return map[string]any{"before": oldAny, "after": newAny}
	}

	diff := map[string]any{}
	seen := map[string]struct{}{}
	for key, oldField := range oldMap {
		seen[key] = struct{}{}
		newField, ok := newMap[key]
		if !ok || !reflect.DeepEqual(oldField, newField) {
			diff[key] = map[string]any{"before": oldField, "after": newField}
		}
	}
	for key, newField := range newMap {
		if _, ok := seen[key]; ok {
			continue
		}
		diff[key] = map[string]any{"before": nil, "after": newField}
	}
	return diff
}
