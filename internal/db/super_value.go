package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrSuperValueCycleDetected = errors.New("super_value_cycle_detected")
	ErrSuperValueMaxDepth      = errors.New("super_value_max_depth_exceeded")
	ErrSuperValueMaxNodes      = errors.New("super_value_max_nodes_exceeded")
	ErrSuperValueFanout        = errors.New("super_value_fanout_exceeded")
	ErrSuperValueReadOnly      = errors.New("super_value_read_only")
)

type readMode int

const (
	readModeLatest readMode = iota
	readModeAt
	readModeLast
)

type readContext struct {
	mode  readMode
	at    time.Time
	steps int
	raw   bool
}

type resolveState struct {
	cache      map[string]Record
	missingRef map[string]struct{}
	active     map[string]struct{}
	nodes      int
	maxDepth   int
	maxFanout  int
	maxNodes   int
}

func newResolveState(cfgMaxDepth, cfgMaxFanout, cfgMaxNodes int) *resolveState {
	return &resolveState{
		cache:      map[string]Record{},
		missingRef: map[string]struct{}{},
		active:     map[string]struct{}{},
		maxDepth:   cfgMaxDepth,
		maxFanout:  cfgMaxFanout,
		maxNodes:   cfgMaxNodes,
	}
}

func (e *Engine) resolveRecordForRead(key string, ctx readContext) (Record, error) {
	record, err := e.getRawByContext(key, ctx)
	if err != nil {
		return Record{}, err
	}
	if ctx.raw {
		return record, nil
	}
	if !hasSuperReference(record.Value) {
		return record, nil
	}

	state := newResolveState(e.perfCfg.SuperValueMaxDepth, e.perfCfg.SuperValueMaxFanout, e.perfCfg.SuperValueMaxNodes)
	resolved, err := e.resolveJSONValue(key, record.Value, ctx, state, 0)
	if err != nil {
		return Record{}, err
	}
	encoded, err := json.Marshal(resolved)
	if err != nil {
		return Record{}, err
	}
	record.Value = encoded
	return record, nil
}

func (e *Engine) getRawByContext(key string, ctx readContext) (Record, error) {
	switch ctx.mode {
	case readModeLatest:
		return e.getLocked(key)
	case readModeAt:
		return e.getAtRawLocked(key, ctx.at)
	case readModeLast:
		return e.getLastRawLocked(key, ctx.steps)
	default:
		return Record{}, ErrNotFound
	}
}

func (e *Engine) getAtRawLocked(key string, at time.Time) (Record, error) {
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

func (e *Engine) getLastRawLocked(key string, steps int) (Record, error) {
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

func (e *Engine) resolveJSONValue(currentKey string, raw json.RawMessage, ctx readContext, state *resolveState, depth int) (any, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return e.resolveAnyValue(currentKey, decoded, ctx, state, depth)
}

func (e *Engine) resolveAnyValue(currentKey string, decoded any, ctx readContext, state *resolveState, depth int) (any, error) {
	switch v := decoded.(type) {
	case string:
		refKey, isRef := parseSuperRef(v)
		if !isRef {
			return v, nil
		}
		return e.resolveReference(currentKey, refKey, ctx, state, depth+1)
	case []any:
		refCount := 0
		for _, item := range v {
			if s, ok := item.(string); ok {
				if _, isRef := parseSuperRef(s); isRef {
					refCount++
				}
			}
		}
		if state.maxFanout > 0 && refCount > state.maxFanout {
			return nil, fmt.Errorf("%w: key=%s fanout=%d max=%d", ErrSuperValueFanout, currentKey, refCount, state.maxFanout)
		}
		out := make([]any, 0, len(v))
		for _, item := range v {
			resolved, err := e.resolveAnyValue(currentKey, item, ctx, state, depth)
			if err != nil {
				return nil, err
			}
			out = append(out, resolved)
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, value := range v {
			resolved, err := e.resolveAnyValue(currentKey, value, ctx, state, depth)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	default:
		return decoded, nil
	}
}

func (e *Engine) resolveReference(currentKey, refKey string, ctx readContext, state *resolveState, depth int) (any, error) {
	if state.maxDepth > 0 && depth > state.maxDepth {
		return nil, fmt.Errorf("%w: key=%s ref=%s depth=%d max=%d", ErrSuperValueMaxDepth, currentKey, refKey, depth, state.maxDepth)
	}
	if state.maxNodes > 0 && state.nodes >= state.maxNodes {
		return nil, fmt.Errorf("%w: key=%s ref=%s nodes=%d max=%d", ErrSuperValueMaxNodes, currentKey, refKey, state.nodes, state.maxNodes)
	}
	if _, ok := state.active[refKey]; ok {
		return nil, fmt.Errorf("%w: key=%s ref=%s", ErrSuperValueCycleDetected, currentKey, refKey)
	}

	cacheKey := refCacheKey(refKey, ctx)
	if cached, ok := state.cache[cacheKey]; ok {
		return map[string]any{
			"key":        refKey,
			"found":      true,
			"version":    cached.Version,
			"updated_at": cached.UpdatedAt,
			"value":      mustDecodeAny(cached.Value),
		}, nil
	}
	state.active[refKey] = struct{}{}
	defer delete(state.active, refKey)

	state.nodes++
	record, err := e.getRawByContext(refKey, ctx)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			state.missingRef[refKey] = struct{}{}
			return map[string]any{
				"key":   refKey,
				"found": false,
				"value": nil,
				"error": "key not found",
			}, nil
		}
		return nil, err
	}
	state.cache[cacheKey] = record
	resolvedValue, err := e.resolveJSONValue(refKey, record.Value, ctx, state, depth)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"key":        refKey,
		"found":      true,
		"version":    record.Version,
		"updated_at": record.UpdatedAt,
		"value":      resolvedValue,
	}, nil
}

func hasSuperReference(raw json.RawMessage) bool {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false
	}
	return containsSuperRef(decoded)
}

func containsSuperRef(value any) bool {
	switch v := value.(type) {
	case string:
		_, ok := parseSuperRef(v)
		return ok
	case []any:
		for _, item := range v {
			if containsSuperRef(item) {
				return true
			}
		}
	case map[string]any:
		for _, item := range v {
			if containsSuperRef(item) {
				return true
			}
		}
	}
	return false
}

func parseSuperRef(input string) (string, bool) {
	if !strings.HasPrefix(input, "*") {
		return "", false
	}
	key := strings.TrimSpace(strings.TrimPrefix(input, "*"))
	if key == "" {
		return "", false
	}
	return key, true
}

func refCacheKey(refKey string, ctx readContext) string {
	switch ctx.mode {
	case readModeAt:
		return "at|" + refKey + "|" + ctx.at.UTC().Format(time.RFC3339Nano)
	case readModeLast:
		return fmt.Sprintf("last|%s|%d", refKey, ctx.steps)
	default:
		return "latest|" + refKey
	}
}

func mustDecodeAny(raw json.RawMessage) any {
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}
