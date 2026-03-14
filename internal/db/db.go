package db

import (
	"context"
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
	Timestamp         time.Time       `json:"timestamp"`
	Operation         string          `json:"operation"`
	EventName         string          `json:"event_name,omitempty"`
	OldValue          json.RawMessage `json:"old_value,omitempty"`
	NewValue          json.RawMessage `json:"new_value,omitempty"`
	Source            string          `json:"source,omitempty"`
	Actor             string          `json:"actor,omitempty"`
	RuleID            string          `json:"rule_id,omitempty"`
	CauseEventID      uint64          `json:"cause_event_id,omitempty"`
	PrevVersionOffset uint64          `json:"prev_version_offset,omitempty"`
	Diff              any             `json:"diff,omitempty"`
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
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	EventName string          `json:"event_name,omitempty"`
}

type BatchGetItem struct {
	Key       string          `json:"key"`
	Found     bool            `json:"found"`
	Value     json.RawMessage `json:"value,omitempty"`
	Version   uint64          `json:"version,omitempty"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
}

const keyLockCount = 256

type Engine struct {
	metaMu         sync.RWMutex
	persistMu      sync.Mutex
	dataDir        string
	log            *storage.EventLog
	keyLocks       [keyLockCount]sync.RWMutex
	state          sync.Map
	eventByID      sync.Map
	eventRefs      sync.Map
	latestVersion  sync.Map
	keyIndex       sync.Map
	rules          map[string]storage.Rule
	tasks          map[string]storage.Task
	snapshotTime   time.Time
	nextTaskID     uint64
	lastEventID    uint64
	schedulerRuns  uint64
	schedulerExecs uint64
	schedulerSkips uint64
}

func Open(dataDir string) (*Engine, error) {
	return OpenWithConfig(dataDir, config.Default().Storage.Segment)
}

func OpenWithConfig(dataDir string, segmentCfg config.SegmentConfig) (*Engine, error) {
	log, err := storage.OpenEventLog(filepath.Join(dataDir, "log"), storage.SegmentOptions{
		MaxBytes:   segmentCfg.MaxBytes,
		MaxRecords: segmentCfg.MaxRecords,
	})
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		dataDir:       dataDir,
		log:           log,
		rules:         map[string]storage.Rule{},
		tasks:         map[string]storage.Task{},
	}

	if err := engine.recover(); err != nil {
		_ = log.Close()
		return nil, err
	}
	return engine, nil
}

func (e *Engine) recover() error {
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

	tasks, err := storage.LoadAllTasks(e.dataDir)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		e.tasks[task.ID] = task
		if strings.HasPrefix(task.ID, "task-") {
			var id uint64
			if _, err := fmt.Sscanf(task.ID, "task-%d", &id); err == nil && id > e.nextTaskID {
				e.nextTaskID = id
			}
		}
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

	events, err := storage.ReadAllEvents(filepath.Join(e.dataDir, "log"))
	if err != nil {
		return err
	}
	for _, event := range events {
		e.storeEvent(event)
		e.storeEventRef(event.EventID, storage.EventRef{
			EventID:     event.EventID,
			Key:         event.Key,
			Timestamp:   event.Timestamp,
			PrevEventID: event.PrevVersionOffset,
		})
		e.storeLatestVersion(event.Key, event.EventID)
		e.storeKeyIndex(event.Key, storage.KeyIndexEntry{
			Key:         event.Key,
			LatestEvent: event.EventID,
			UpdatedAt:   event.Timestamp,
		})
		e.updateLastEventID(event.EventID)
		if event.EventID > snapshot.LastEventID {
			e.applyLocked(event)
		}
	}
	return nil
}

func (e *Engine) Close() error {
	return e.log.Close()
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
	return e.setWithMeta(key, value, "api", "user", "", "", 0)
}

func (e *Engine) SetInternal(key string, value json.RawMessage, source, actor, ruleID string, causeEventID uint64) (storage.Event, error) {
	return e.setWithMeta(key, value, source, actor, ruleID, "", causeEventID)
}

func (e *Engine) SetWithEventName(key string, value json.RawMessage, eventName string) (storage.Event, error) {
	return e.setWithMeta(key, value, "api", "user", "", eventName, 0)
}

func (e *Engine) BatchSet(items []BatchSetItem) ([]storage.Event, error) {
	if len(items) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		keys = append(keys, item.Key)
	}
	unlock := e.lockKeys(keys)
	defer unlock()

	events := make([]storage.Event, 0, len(items))
	for _, item := range items {
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
			OldValue:          clone(current.Value),
			NewValue:          clone(value),
			Source:            "api",
			Actor:             "user",
			PrevVersionOffset: current.Version,
		})
	}

	persisted, err := e.log.AppendBatch(events)
	if err != nil {
		return nil, err
	}
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	for _, event := range persisted {
		e.applyPersistedLocked(event)
		if err := e.scheduleMatchingRulesLocked(event, false); err != nil {
			return nil, err
		}
	}
	if err := e.persistRuleStateLocked(); err != nil {
		return nil, err
	}
	return persisted, nil
}

func (e *Engine) setWithMeta(key string, value json.RawMessage, source, actor, ruleID, eventName string, causeEventID uint64) (storage.Event, error) {
	unlock := e.lockKey(key)
	defer unlock()

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
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	e.applyPersistedLocked(persisted)
	if err := e.scheduleMatchingRulesLocked(persisted, true); err != nil {
		return storage.Event{}, err
	}
	return persisted, nil
}

func (e *Engine) Delete(key string) (storage.Event, error) {
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
	unlock := e.rlockKey(key)
	defer unlock()
	return e.getLocked(key)
}

func (e *Engine) BatchGet(keys []string) []BatchGetItem {
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
	unlock := e.rlockKey(key)
	defer unlock()

	version := e.loadLatestVersion(key)
	for version != 0 {
		event, ok := e.loadEventLocked(version)
		if !ok {
			break
		}
		if !event.Timestamp.After(at) {
			if event.Operation == "DELETE" || (event.Operation == "ROLLBACK" && len(event.NewValue) == 0) {
				return Record{}, ErrNotFound
			}
			return Record{Value: clone(event.NewValue), Version: event.EventID, UpdatedAt: event.Timestamp}, nil
		}
		version = event.PrevVersionOffset
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
	unlock := e.rlockKey(key)
	defer unlock()

	var timeline []TimelineEntry
	for version := e.loadLatestVersion(key); version != 0; {
		event, ok := e.loadEventLocked(version)
		if !ok {
			break
		}
		entry := TimelineEntry{
			EventID:           event.EventID,
			Timestamp:         event.Timestamp,
			Operation:         event.Operation,
			EventName:         event.EventName,
			OldValue:          clone(event.OldValue),
			NewValue:          clone(event.NewValue),
			Source:            event.Source,
			Actor:             event.Actor,
			RuleID:            event.RuleID,
			CauseEventID:      event.CauseEventID,
			PrevVersionOffset: event.PrevVersionOffset,
		}
		if withDiff {
			entry.Diff = buildDiff(event.OldValue, event.NewValue)
		}
		timeline = append(timeline, entry)
		version = event.PrevVersionOffset
	}
	if len(timeline) == 0 {
		return nil, ErrNotFound
	}
	sort.Slice(timeline, func(i, j int) bool { return timeline[i].EventID < timeline[j].EventID })
	return timeline, nil
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
	tasks := make([]storage.Task, 0, len(e.tasks))
	for _, task := range e.tasks {
		tasks = append(tasks, task)
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
	for _, pending := range due {
		unlock := e.lockKey(pending.EntityKey)
		e.metaMu.Lock()
		task := e.tasks[pending.ID]
		record, ok := e.loadState(task.EntityKey)
		if !ok || record.Version != task.ExpectedVersion || extractState(record.Value) != task.ExpectedState {
			nowCopy := now
			task.Status = "skipped"
			task.ProcessedAt = &nowCopy
			e.tasks[task.ID] = task
			rule := e.rules[task.RuleID]
			rule.SkipCount++
			e.rules[task.RuleID] = rule
			skipped++
			e.metaMu.Unlock()
			unlock()
			continue
		}

		nextValue, err := transitionValue(record.Value, task.ToState)
		if err != nil {
			nowCopy := now
			task.Status = "skipped"
			task.ProcessedAt = &nowCopy
			e.tasks[task.ID] = task
			rule := e.rules[task.RuleID]
			rule.SkipCount++
			e.rules[task.RuleID] = rule
			skipped++
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
			return SchedulerStats{}, err
		}
		e.applyPersistedLocked(persisted)
		_ = e.scheduleMatchingRulesLocked(persisted, true)

		nowCopy := now
		task.Status = "done"
		task.ProcessedAt = &nowCopy
		e.tasks[task.ID] = task
		rule := e.rules[task.RuleID]
		rule.ExecuteCount++
		e.rules[task.RuleID] = rule
		executed++
		e.metaMu.Unlock()
		unlock()
	}

	e.metaMu.Lock()
	if err := e.persistTasksLocked(); err != nil {
		e.metaMu.Unlock()
		return SchedulerStats{}, err
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
	e.state.Range(func(key, value any) bool {
		record := value.(Record)
		snapshot.State[key.(string)] = storage.SnapshotRecord{
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
	keys := 0
	e.state.Range(func(_, _ any) bool {
		keys++
		return true
	})
	eventRefs := 0
	e.eventRefs.Range(func(_, _ any) bool {
		eventRefs++
		return true
	})
	loadedEvents := 0
	e.eventByID.Range(func(_, _ any) bool {
		loadedEvents++
		return true
	})
	return map[string]any{
		"keys":               keys,
		"event_refs":         eventRefs,
		"loaded_events":      loadedEvents,
		"rules":              len(e.rules),
		"tasks":              len(e.tasks),
		"pending_tasks":      e.pendingTasksLocked(),
		"last_event_id":      e.lastEventIDLocked(),
		"scheduler_runs":     atomic.LoadUint64(&e.schedulerRuns),
		"scheduler_executed": atomic.LoadUint64(&e.schedulerExecs),
		"scheduler_skipped":  atomic.LoadUint64(&e.schedulerSkips),
	}
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
	e.updateLastEventID(event.EventID)
	e.applyLocked(event)
	_ = e.persistKeyIndexLocked()
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
		e.tasks[task.ID] = task
		rule.MatchCount++
		e.rules[id] = rule
	}
	if persist {
		return e.persistRuleStateLocked()
	}
	return nil
}

func (e *Engine) persistRuleStateLocked() error {
	if err := e.persistTasksLocked(); err != nil {
		return err
	}
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

func (e *Engine) persistKeyIndexLocked() error {
	e.persistMu.Lock()
	defer e.persistMu.Unlock()
	entries := map[string]storage.KeyIndexEntry{}
	e.keyIndex.Range(func(key, value any) bool {
		entries[key.(string)] = value.(storage.KeyIndexEntry)
		return true
	})
	return storage.SaveKeyIndex(e.dataDir, entries)
}

func (e *Engine) persistTasksLocked() error {
	buckets := map[time.Time][]storage.Task{}
	for _, task := range e.tasks {
		bucket := task.BucketTS.UTC().Truncate(time.Minute)
		buckets[bucket] = append(buckets[bucket], task)
	}
	for bucket, tasks := range buckets {
		if err := storage.SaveTaskBucket(e.dataDir, bucket, tasks); err != nil {
			return err
		}
	}
	return nil
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
	for _, task := range e.tasks {
		if task.Status == "pending" {
			total++
		}
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
	value, ok := e.state.Load(key)
	if !ok {
		return Record{}, false
	}
	return value.(Record), true
}

func (e *Engine) storeState(key string, record Record) {
	e.state.Store(key, record)
}

func (e *Engine) deleteState(key string) {
	e.state.Delete(key)
}

func (e *Engine) loadLatestVersion(key string) uint64 {
	value, ok := e.latestVersion.Load(key)
	if !ok {
		return 0
	}
	return value.(uint64)
}

func (e *Engine) storeLatestVersion(key string, version uint64) {
	e.latestVersion.Store(key, version)
}

func (e *Engine) storeKeyIndex(key string, entry storage.KeyIndexEntry) {
	e.keyIndex.Store(key, entry)
}

func (e *Engine) loadEventRef(eventID uint64) (storage.EventRef, bool) {
	value, ok := e.eventRefs.Load(eventID)
	if !ok {
		return storage.EventRef{}, false
	}
	return value.(storage.EventRef), true
}

func (e *Engine) storeEventRef(eventID uint64, ref storage.EventRef) {
	e.eventRefs.Store(eventID, ref)
}

func (e *Engine) loadEvent(eventID uint64) (storage.Event, bool) {
	value, ok := e.eventByID.Load(eventID)
	if !ok {
		return storage.Event{}, false
	}
	return value.(storage.Event), true
}

func (e *Engine) storeEvent(event storage.Event) {
	e.eventByID.Store(event.EventID, event)
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
