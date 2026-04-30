package config

import (
	"encoding/json"
	"errors"
	"os"
)

type Config struct {
	Auth        AuthConfig        `json:"auth"`
	Storage     StorageConfig     `json:"storage"`
	Performance PerformanceConfig `json:"performance"`
	Transport   TransportConfig   `json:"transport"`
}

type AuthConfig struct {
	Enabled         bool       `json:"enabled"`
	Secret          string     `json:"secret,omitempty"`
	TokenTTLMinutes int        `json:"token_ttl_minutes"`
	Users           []AuthUser `json:"users"`
}

type AuthUser struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type StorageConfig struct {
	Mode           string           `json:"mode"`
	Durability     string           `json:"durability"`
	ReplicaWAL     ReplicaWALConfig `json:"replica_wal"`
	Segment        SegmentConfig    `json:"segment"`
	StrictRecovery bool             `json:"strict_recovery"`
}

type ReplicaWALConfig struct {
	Addr      string `json:"addr"`
	TimeoutMS int    `json:"timeout_ms"`
}

type PerformanceConfig struct {
	EventCacheMaxItems           int    `json:"event_cache_max_items"`
	EventCacheMaxBytes           int    `json:"event_cache_max_bytes"`
	KeyIndexCompactOps           int    `json:"key_index_compact_ops"`
	IndexFlushMode               string `json:"index_flush_mode"`
	IngressMaxInFlight           int    `json:"ingress_max_inflight"`
	IngressMaxQueue              int    `json:"ingress_max_queue"`
	IngressQueueTimeoutMS        int    `json:"ingress_queue_timeout_ms"`
	SchedulerWorkers             int    `json:"scheduler_workers"`
	MetricsSampleIntervalSeconds int    `json:"metrics_sample_interval_seconds"`
	SnapshotBatchSize            int    `json:"snapshot_batch_size"`
	TimelineDefaultLimit         int    `json:"timeline_default_limit"`
	SuperValueMaxDepth           int    `json:"super_value_max_depth"`
	SuperValueMaxFanout          int    `json:"super_value_max_fanout"`
	SuperValueMaxNodes           int    `json:"super_value_max_nodes"`
}

type SegmentConfig struct {
	MaxBytes   int64 `json:"max_bytes"`
	MaxRecords int   `json:"max_records"`
}

type TransportConfig struct {
	WSOnlyMode bool `json:"ws_only_mode"`
}

func Default() Config {
	return Config{
		Auth: AuthConfig{
			Enabled:         false,
			TokenTTLMinutes: 1440,
			Users:           []AuthUser{},
		},
		Storage: StorageConfig{
			Mode:       "normal",
			Durability: "safe",
			ReplicaWAL: ReplicaWALConfig{
				Addr:      "",
				TimeoutMS: 1500,
			},
			Segment: SegmentConfig{
				MaxBytes:   64 * 1024 * 1024,
				MaxRecords: 30000,
			},
		},
		Performance: PerformanceConfig{
			EventCacheMaxItems:           50000,
			EventCacheMaxBytes:           128 * 1024 * 1024,
			KeyIndexCompactOps:           10000,
			IndexFlushMode:               "sync",
			IngressMaxInFlight:           256,
			IngressMaxQueue:              1024,
			IngressQueueTimeoutMS:        1500,
			SchedulerWorkers:             4,
			MetricsSampleIntervalSeconds: 10,
			SnapshotBatchSize:            10000,
			TimelineDefaultLimit:         1000,
			SuperValueMaxDepth:           5,
			SuperValueMaxFanout:          200,
			SuperValueMaxNodes:           1000,
		},
		Transport: TransportConfig{
			WSOnlyMode: false,
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()
	bytes, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return Config{}, err
	}
	if len(bytes) == 0 {
		return cfg, nil
	}
	if err := json.Unmarshal(bytes, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.Storage.Segment.MaxBytes < 0 {
		cfg.Storage.Segment.MaxBytes = 0
	}
	if cfg.Storage.Segment.MaxRecords < 0 {
		cfg.Storage.Segment.MaxRecords = 0
	}
	cfg.Storage.Mode = normalizeStorageMode(cfg.Storage.Mode)
	cfg.Storage.Durability = normalizeDurability(cfg.Storage.Durability)
	if cfg.Storage.ReplicaWAL.TimeoutMS <= 0 {
		cfg.Storage.ReplicaWAL.TimeoutMS = Default().Storage.ReplicaWAL.TimeoutMS
	}
	if cfg.Performance.EventCacheMaxItems <= 0 {
		cfg.Performance.EventCacheMaxItems = Default().Performance.EventCacheMaxItems
	}
	if cfg.Performance.EventCacheMaxBytes <= 0 {
		cfg.Performance.EventCacheMaxBytes = Default().Performance.EventCacheMaxBytes
	}
	if cfg.Performance.KeyIndexCompactOps <= 0 {
		cfg.Performance.KeyIndexCompactOps = Default().Performance.KeyIndexCompactOps
	}
	cfg.Performance.IndexFlushMode = normalizeIndexFlushMode(cfg.Performance.IndexFlushMode)
	if cfg.Performance.IngressMaxInFlight <= 0 {
		cfg.Performance.IngressMaxInFlight = Default().Performance.IngressMaxInFlight
	}
	if cfg.Performance.IngressMaxQueue <= 0 {
		cfg.Performance.IngressMaxQueue = Default().Performance.IngressMaxQueue
	}
	if cfg.Performance.IngressQueueTimeoutMS <= 0 {
		cfg.Performance.IngressQueueTimeoutMS = Default().Performance.IngressQueueTimeoutMS
	}
	if cfg.Performance.SchedulerWorkers <= 0 {
		cfg.Performance.SchedulerWorkers = Default().Performance.SchedulerWorkers
	}
	if cfg.Performance.MetricsSampleIntervalSeconds < 10 {
		cfg.Performance.MetricsSampleIntervalSeconds = Default().Performance.MetricsSampleIntervalSeconds
	}
	if cfg.Performance.SnapshotBatchSize <= 0 {
		cfg.Performance.SnapshotBatchSize = Default().Performance.SnapshotBatchSize
	}
	if cfg.Performance.TimelineDefaultLimit <= 0 {
		cfg.Performance.TimelineDefaultLimit = Default().Performance.TimelineDefaultLimit
	}
	if cfg.Performance.SuperValueMaxDepth <= 0 {
		cfg.Performance.SuperValueMaxDepth = Default().Performance.SuperValueMaxDepth
	}
	if cfg.Performance.SuperValueMaxFanout <= 0 {
		cfg.Performance.SuperValueMaxFanout = Default().Performance.SuperValueMaxFanout
	}
	if cfg.Performance.SuperValueMaxNodes <= 0 {
		cfg.Performance.SuperValueMaxNodes = Default().Performance.SuperValueMaxNodes
	}
	if cfg.Auth.TokenTTLMinutes <= 0 {
		cfg.Auth.TokenTTLMinutes = Default().Auth.TokenTTLMinutes
	}
	return cfg, nil
}

func normalizeIndexFlushMode(mode string) string {
	switch mode {
	case "sync", "SYNC":
		return "sync"
	case "async", "ASYNC":
		return "async"
	default:
		return "sync"
	}
}

func normalizeStorageMode(mode string) string {
	switch mode {
	case "normal", "NORMAL":
		return "normal"
	case "mem_only", "MEM_ONLY", "mem-only", "MEM-ONLY":
		return "mem_only"
	default:
		return "normal"
	}
}

func normalizeDurability(level string) string {
	switch level {
	case "fast", "FAST":
		return "fast"
	case "safe", "SAFE":
		return "safe"
	case "strict", "STRICT":
		return "strict"
	default:
		return "safe"
	}
}
