package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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
	mode       readMode
	at         time.Time
	steps      int
	raw        bool
	expandMode SuperValueExpandMode
	paths      *superValuePathMatcher
}

type SuperValueExpandMode string

const (
	SuperValueExpandAll    SuperValueExpandMode = "all"
	SuperValueExpandNone   SuperValueExpandMode = "none"
	SuperValueExpandOnly   SuperValueExpandMode = "only"
	SuperValueExpandExcept SuperValueExpandMode = "except"
)

type ReadOptions struct {
	ExpandMode  SuperValueExpandMode
	ExpandPaths []string
}

type superValuePathNode struct {
	children    map[string]*superValuePathNode
	wildcard    *superValuePathNode
	terminal    bool
	hasTerminal bool
}

type superValuePathMatcher struct {
	root *superValuePathNode
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
	if ctx.raw || ctx.expandMode == SuperValueExpandNone {
		return record, nil
	}
	if !hasSuperReference(record.Value) {
		return record, nil
	}

	start := time.Now()
	defer observe(e.metrics.superResolve, start)
	state := newResolveState(e.perfCfg.SuperValueMaxDepth, e.perfCfg.SuperValueMaxFanout, e.perfCfg.SuperValueMaxNodes)
	resolved, err := e.resolveJSONValue(key, record.Value, ctx, state, 0, nil)
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

func (e *Engine) resolveJSONValue(currentKey string, raw json.RawMessage, ctx readContext, state *resolveState, depth int, path []string) (any, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	return e.resolveAnyValue(currentKey, decoded, ctx, state, depth, path)
}

func (e *Engine) resolveAnyValue(currentKey string, decoded any, ctx readContext, state *resolveState, depth int, path []string) (any, error) {
	switch v := decoded.(type) {
	case string:
		refKey, isRef := parseSuperRef(v)
		if !isRef || !ctx.shouldExpand(path) {
			if isRef {
				atomic.AddUint64(&e.metrics.superPreserved, 1)
			}
			return v, nil
		}
		atomic.AddUint64(&e.metrics.superResolved, 1)
		return e.resolveReference(currentKey, refKey, ctx, state, depth+1, path)
	case []any:
		refCount := 0
		for index, item := range v {
			itemPath := appendJSONPath(path, strconv.Itoa(index))
			if s, ok := item.(string); ok {
				if _, isRef := parseSuperRef(s); isRef && ctx.shouldExpand(itemPath) {
					refCount++
				}
			}
		}
		if state.maxFanout > 0 && refCount > state.maxFanout {
			return nil, fmt.Errorf("%w: key=%s fanout=%d max=%d", ErrSuperValueFanout, currentKey, refCount, state.maxFanout)
		}
		out := make([]any, 0, len(v))
		for index, item := range v {
			itemPath := appendJSONPath(path, strconv.Itoa(index))
			if !ctx.shouldTraverse(itemPath) {
				atomic.AddUint64(&e.metrics.superPreserved, 1)
				out = append(out, item)
				continue
			}
			resolved, err := e.resolveAnyValue(currentKey, item, ctx, state, depth, itemPath)
			if err != nil {
				return nil, err
			}
			out = append(out, resolved)
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, value := range v {
			childPath := appendJSONPath(path, key)
			if !ctx.shouldTraverse(childPath) {
				atomic.AddUint64(&e.metrics.superPreserved, 1)
				out[key] = value
				continue
			}
			resolved, err := e.resolveAnyValue(currentKey, value, ctx, state, depth, childPath)
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

func (e *Engine) resolveReference(currentKey, refKey string, ctx readContext, state *resolveState, depth int, path []string) (any, error) {
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
		atomic.AddUint64(&e.metrics.superCacheHits, 1)
		resolvedValue, err := e.resolveJSONValue(refKey, cached.Value, ctx, state, depth, path)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"key":        refKey,
			"found":      true,
			"version":    cached.Version,
			"updated_at": cached.UpdatedAt,
			"value":      resolvedValue,
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
	resolvedValue, err := e.resolveJSONValue(refKey, record.Value, ctx, state, depth, path)
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

func prepareReadContext(ctx readContext, opts ReadOptions) (readContext, error) {
	mode := opts.ExpandMode
	if mode == "" {
		mode = SuperValueExpandAll
	}
	switch mode {
	case SuperValueExpandAll, SuperValueExpandNone:
		if len(opts.ExpandPaths) > 0 {
			return readContext{}, fmt.Errorf("super value paths require expand mode only or except")
		}
	case SuperValueExpandOnly, SuperValueExpandExcept:
		matcher, err := newSuperValuePathMatcher(opts.ExpandPaths)
		if err != nil {
			return readContext{}, err
		}
		ctx.paths = matcher
	default:
		return readContext{}, fmt.Errorf("invalid super value expand mode: %s", mode)
	}
	ctx.expandMode = mode
	return ctx, nil
}

func (ctx readContext) shouldExpand(path []string) bool {
	switch ctx.expandMode {
	case SuperValueExpandNone:
		return false
	case SuperValueExpandOnly:
		ancestor, descendant := ctx.paths.relation(path)
		return ancestor || descendant
	case SuperValueExpandExcept:
		ancestor, _ := ctx.paths.relation(path)
		return !ancestor
	default:
		return true
	}
}

func (ctx readContext) shouldTraverse(path []string) bool {
	switch ctx.expandMode {
	case SuperValueExpandNone:
		return false
	case SuperValueExpandOnly:
		ancestor, descendant := ctx.paths.relation(path)
		return ancestor || descendant
	case SuperValueExpandExcept:
		ancestor, _ := ctx.paths.relation(path)
		return !ancestor
	default:
		return true
	}
}

func newSuperValuePathMatcher(paths []string) (*superValuePathMatcher, error) {
	root := &superValuePathNode{}
	for _, input := range paths {
		segments, err := parseSuperValuePath(input)
		if err != nil {
			return nil, err
		}
		node := root
		for _, segment := range segments {
			if segment == "*" {
				if node.wildcard == nil {
					node.wildcard = &superValuePathNode{}
				}
				node = node.wildcard
				continue
			}
			if node.children == nil {
				node.children = map[string]*superValuePathNode{}
			}
			child := node.children[segment]
			if child == nil {
				child = &superValuePathNode{}
				node.children[segment] = child
			}
			node = child
		}
		node.terminal = true
	}
	markPathTerminals(root)
	return &superValuePathMatcher{root: root}, nil
}

func parseSuperValuePath(input string) ([]string, error) {
	input = strings.TrimSpace(input)
	if input == "" || input[0] != '/' {
		return nil, fmt.Errorf("invalid super value path %q: path must start with /", input)
	}
	if input == "/" {
		return nil, nil
	}
	rawSegments := strings.Split(strings.TrimPrefix(input, "/"), "/")
	segments := make([]string, 0, len(rawSegments))
	for _, segment := range rawSegments {
		if segment == "" {
			return nil, fmt.Errorf("invalid super value path %q: empty segment", input)
		}
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")
		segments = append(segments, segment)
	}
	return segments, nil
}

func markPathTerminals(node *superValuePathNode) bool {
	if node == nil {
		return false
	}
	hasTerminal := node.terminal
	for _, child := range node.children {
		hasTerminal = markPathTerminals(child) || hasTerminal
	}
	hasTerminal = markPathTerminals(node.wildcard) || hasTerminal
	node.hasTerminal = hasTerminal
	return hasTerminal
}

func (m *superValuePathMatcher) relation(path []string) (ancestor, descendant bool) {
	if m == nil || m.root == nil {
		return false, false
	}
	nodes := []*superValuePathNode{m.root}
	if m.root.terminal {
		ancestor = true
	}
	for _, segment := range path {
		next := make([]*superValuePathNode, 0, len(nodes)*2)
		seen := map[*superValuePathNode]struct{}{}
		for _, node := range nodes {
			if child := node.children[segment]; child != nil {
				if _, ok := seen[child]; !ok {
					seen[child] = struct{}{}
					next = append(next, child)
				}
			}
			if node.wildcard != nil {
				if _, ok := seen[node.wildcard]; !ok {
					seen[node.wildcard] = struct{}{}
					next = append(next, node.wildcard)
				}
			}
		}
		if len(next) == 0 {
			return ancestor, false
		}
		nodes = next
		for _, node := range nodes {
			if node.terminal {
				ancestor = true
			}
		}
	}
	for _, node := range nodes {
		if node.hasTerminal {
			descendant = true
			break
		}
	}
	return ancestor, descendant
}

func appendJSONPath(path []string, segment string) []string {
	child := make([]string, len(path)+1)
	copy(child, path)
	child[len(path)] = segment
	return child
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
