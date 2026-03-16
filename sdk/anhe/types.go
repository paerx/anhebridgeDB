package anhe

import (
	"encoding/json"
	"time"
)

type Config struct {
	Addr       string
	Token      string
	Username   string
	Password   string
	Timeout    time.Duration
	WSPoolSize int
}

type Record struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   uint64          `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
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

type SearchResultPage struct {
	Page    int             `json:"page"`
	Limit   int             `json:"limit"`
	Total   int             `json:"total"`
	HasMore bool            `json:"has_more"`
	Results []TimelineEntry `json:"results"`
}

type TimelineOptions struct {
	WithDiff      bool
	Limit         int
	BeforeVersion uint64
	AfterVersion  uint64
}

type BatchGetItem struct {
	Key       string          `json:"key"`
	Found     bool            `json:"found"`
	Value     json.RawMessage `json:"value,omitempty"`
	Version   uint64          `json:"version,omitempty"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
}
