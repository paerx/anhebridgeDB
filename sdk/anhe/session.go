package anhe

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type KeySession struct {
	db          *DB
	key         string
	event       string
	idempotency string
}

func (s *KeySession) Event(name string) *KeySession {
	s.event = strings.TrimSpace(name)
	return s
}

func (s *KeySession) Idempotency(id string) *KeySession {
	s.idempotency = strings.TrimSpace(id)
	return s
}

func (s *KeySession) Set(ctx context.Context, value any) (Event, error) {
	query, err := buildSetQuery(s.key, value, s.event, s.idempotency)
	if err != nil {
		return Event{}, err
	}
	return s.execEvent(ctx, query)
}

func (s *KeySession) SetDelta(ctx context.Context, delta float64) (Event, error) {
	query := fmt.Sprintf("SET %s %s", s.key, strconv.FormatFloat(delta, 'f', -1, 64))
	if s.event != "" {
		query += " " + s.event
	}
	if s.idempotency != "" {
		query += " i=" + s.idempotency
	}
	query += ";"
	return s.execEvent(ctx, query)
}

func (s *KeySession) Delete(ctx context.Context) (Event, error) {
	return s.execEvent(ctx, fmt.Sprintf("DELETE %s;", s.key))
}

func (s *KeySession) Get(ctx context.Context) (Record, error) {
	results, err := s.db.ExecDSL(ctx, fmt.Sprintf("GET %s;", s.key))
	if err != nil {
		return Record{}, err
	}
	return decodeRecordResult(results)
}

func (s *KeySession) At(ctx context.Context, at time.Time) (Record, error) {
	query := fmt.Sprintf("GET %s AT '%s';", s.key, at.UTC().Format(time.RFC3339))
	results, err := s.db.ExecDSL(ctx, query)
	if err != nil {
		return Record{}, err
	}
	return decodeRecordResult(results)
}

func (s *KeySession) Last(ctx context.Context, steps int) (Record, error) {
	query := fmt.Sprintf("GET %s LAST", s.key)
	for i := 1; i < steps; i++ {
		query += " -1"
	}
	query += ";"
	results, err := s.db.ExecDSL(ctx, query)
	if err != nil {
		return Record{}, err
	}
	return decodeRecordResult(results)
}

func (s *KeySession) Timeline(ctx context.Context, opts TimelineOptions) ([]TimelineEntry, error) {
	query := fmt.Sprintf("GET %s ALLTIME", s.key)
	if opts.WithDiff {
		query += " WITH DIFF"
	}
	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}
	if opts.BeforeVersion > 0 {
		query += fmt.Sprintf(" BEFORE VERSION:%d", opts.BeforeVersion)
	}
	if opts.AfterVersion > 0 {
		query += fmt.Sprintf(" AFTER VERSION:%d", opts.AfterVersion)
	}
	query += ";"
	results, err := s.db.ExecDSL(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return decodeTimeline(results[0])
}

func (s *KeySession) RollbackVersion(ctx context.Context, version uint64) (Event, error) {
	return s.execEvent(ctx, fmt.Sprintf("ROLLBACK %s VERSION:%d;", s.key, version))
}

func (s *KeySession) RollbackLast(ctx context.Context) (Event, error) {
	return s.execEvent(ctx, fmt.Sprintf("ROLLBACK %s VERSION:LAST;", s.key))
}

func (s *KeySession) execEvent(ctx context.Context, query string) (Event, error) {
	results, err := s.db.ExecDSL(ctx, query)
	if err != nil {
		return Event{}, err
	}
	if len(results) == 0 {
		return Event{}, fmt.Errorf("empty result")
	}
	return decodeEvent(results[0])
}

type SearchSession struct {
	db       *DB
	key      string
	name     string
	i        string
	withSame bool
	desc     bool
	limit    int
	page     int
}

func (s *SearchSession) Events() *SearchSession { return s }

func (s *SearchSession) Key(key string) *SearchSession {
	s.key = strings.TrimSpace(key)
	return s
}

func (s *SearchSession) Name(name string) *SearchSession {
	s.name = strings.TrimSpace(name)
	return s
}

func (s *SearchSession) I(i string) *SearchSession {
	s.i = strings.TrimSpace(i)
	return s
}

func (s *SearchSession) WithSameI() *SearchSession {
	s.withSame = true
	return s
}

func (s *SearchSession) Desc() *SearchSession {
	s.desc = true
	return s
}

func (s *SearchSession) Asc() *SearchSession {
	s.desc = false
	return s
}

func (s *SearchSession) Limit(limit int) *SearchSession {
	s.limit = limit
	return s
}

func (s *SearchSession) Page(page int) *SearchSession {
	s.page = page
	return s
}

func (s *SearchSession) Find(ctx context.Context) (SearchResultPage, error) {
	query := "SEARCH EVENTS"
	if s.key != "" {
		query += " KEY:" + s.key
	}
	if s.name != "" {
		query += " NAME:" + s.name
	}
	if s.i != "" {
		query += " I:" + s.i
	}
	if s.withSame {
		query += " WITH SAME I"
	}
	if s.desc {
		query += " DESC"
	} else {
		query += " ASC"
	}
	if s.limit > 0 {
		query += fmt.Sprintf(" LIMIT:%d", s.limit)
	}
	if s.page > 0 {
		query += fmt.Sprintf(" PAGE:%d", s.page)
	}
	query += ";"

	results, err := s.db.ExecDSL(ctx, query)
	if err != nil {
		return SearchResultPage{}, err
	}
	if len(results) == 0 {
		return SearchResultPage{}, nil
	}
	return decodeSearchPage(results[0])
}

func buildSetQuery(key string, value any, eventName, idempotency string) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	query := fmt.Sprintf("SET %s %s", key, string(raw))
	if eventName != "" {
		query += " " + eventName
	}
	if idempotency != "" {
		query += " i=" + idempotency
	}
	query += ";"
	return query, nil
}

func decodeEvent(v any) (Event, error) {
	var event Event
	if err := decodeAny(v, &event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func decodeRecordResult(results []any) (Record, error) {
	if len(results) == 0 {
		return Record{}, fmt.Errorf("empty result")
	}
	var record Record
	if err := decodeAny(results[0], &record); err == nil && record.Version > 0 {
		return record, nil
	}

	var arr []Record
	if err := decodeAny(results[0], &arr); err == nil && len(arr) > 0 {
		return arr[0], nil
	}

	var wrapped []map[string]any
	if err := decodeAny(results[0], &wrapped); err == nil && len(wrapped) > 0 {
		if err := decodeAny(wrapped[0], &record); err == nil && record.Version > 0 {
			return record, nil
		}
	}
	return Record{}, fmt.Errorf("invalid GET result shape: %T", results[0])
}

func decodeTimeline(v any) ([]TimelineEntry, error) {
	var out []TimelineEntry
	if err := decodeAny(v, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeSearchPage(v any) (SearchResultPage, error) {
	var out SearchResultPage
	if err := decodeAny(v, &out); err != nil {
		return SearchResultPage{}, err
	}
	return out, nil
}

func decodeAny(input any, out any) error {
	bytes, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return json.Unmarshal(bytes, out)
}

type batchSetItem struct {
	key         string
	value       any
	event       string
	idempotency string
}

type BatchSession struct {
	db       *DB
	setItems []batchSetItem
	getKeys  []string
}

func (b *BatchSession) Set(key string, value any) *BatchSession {
	b.setItems = append(b.setItems, batchSetItem{key: strings.TrimSpace(key), value: value})
	return b
}

func (b *BatchSession) SetWithEvent(key string, value any, event string) *BatchSession {
	b.setItems = append(b.setItems, batchSetItem{
		key:   strings.TrimSpace(key),
		value: value,
		event: strings.TrimSpace(event),
	})
	return b
}

func (b *BatchSession) SetWithEventAndI(key string, value any, event, idempotency string) *BatchSession {
	b.setItems = append(b.setItems, batchSetItem{
		key:         strings.TrimSpace(key),
		value:       value,
		event:       strings.TrimSpace(event),
		idempotency: strings.TrimSpace(idempotency),
	})
	return b
}

func (b *BatchSession) Get(keys ...string) *BatchSession {
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		b.getKeys = append(b.getKeys, key)
	}
	return b
}

func (b *BatchSession) ExecSet(ctx context.Context) ([]Event, error) {
	if len(b.setItems) == 0 {
		return nil, nil
	}
	parts := make([]string, 0, len(b.setItems))
	for _, item := range b.setItems {
		raw, err := json.Marshal(item.value)
		if err != nil {
			return nil, err
		}
		part := item.key + " " + string(raw)
		if item.event != "" {
			part += " " + item.event
		}
		if item.idempotency != "" {
			part += " i=" + item.idempotency
		}
		parts = append(parts, part)
	}
	query := "ASET " + strings.Join(parts, ", ") + ";"
	results, err := b.db.ExecDSL(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	var out []Event
	if err := decodeAny(results[0], &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (b *BatchSession) ExecGet(ctx context.Context) ([]BatchGetItem, error) {
	if len(b.getKeys) == 0 {
		return nil, nil
	}
	query := "AGET " + strings.Join(b.getKeys, " ") + ";"
	results, err := b.db.ExecDSL(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	var out []BatchGetItem
	if err := decodeAny(results[0], &out); err != nil {
		return nil, err
	}
	return out, nil
}
