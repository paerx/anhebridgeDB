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
	Backup      BackupConfig      `json:"backup"`
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

type BackupConfig struct {
	Enabled             bool                `json:"enabled"`
	Mode                string              `json:"mode"`
	RunOnStart          bool                `json:"run_on_start"`
	IntervalSeconds     int                 `json:"interval_seconds"`
	TimeoutSeconds      int                 `json:"timeout_seconds"`
	SpoolDir            string              `json:"spool_dir"`
	KeepLocal           bool                `json:"keep_local"`
	LocalRetentionCount int                 `json:"local_retention_count"`
	Upload              BackupUploadConfig  `json:"upload"`
	Lark                BackupLarkConfig    `json:"lark"`
	Monitor             BackupMonitorConfig `json:"monitor"`
}

type BackupUploadConfig struct {
	Enabled            bool   `json:"enabled"`
	Endpoint           string `json:"endpoint"`
	Bucket             string `json:"bucket"`
	Prefix             string `json:"prefix"`
	AccessKeyEnv       string `json:"access_key_env"`
	SecretAccessKeyEnv string `json:"secret_access_key_env"`
	MaxObjectBytes     int64  `json:"max_object_bytes"`
	RetryCount         int    `json:"retry_count"`
	RetryBackoffMS     int    `json:"retry_backoff_ms"`
}

type BackupLarkConfig struct {
	Enabled       bool   `json:"enabled"`
	WebhookEnv    string `json:"webhook_env"`
	SecretEnv     string `json:"secret_env"`
	NotifySuccess bool   `json:"notify_success"`
}

type BackupMonitorConfig struct {
	Enabled               bool    `json:"enabled"`
	IntervalSeconds       int     `json:"interval_seconds"`
	VerifyIntervalSeconds int     `json:"verify_interval_seconds"`
	ConsecutiveBreaches   int     `json:"consecutive_breaches"`
	AlertCooldownSeconds  int     `json:"alert_cooldown_seconds"`
	HeapAllocMaxBytes     uint64  `json:"heap_alloc_max_bytes"`
	PendingTasksMax       int     `json:"pending_tasks_max"`
	OverdueTasksMax       int     `json:"overdue_tasks_max"`
	WriteQueueMax         int     `json:"write_queue_max"`
	AppendP95MaxMS        float64 `json:"append_p95_max_ms"`
	GetP95MaxMS           float64 `json:"get_p95_max_ms"`
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
		Backup: BackupConfig{
			Enabled:             false,
			Mode:                "incremental",
			RunOnStart:          true,
			IntervalSeconds:     2 * 60 * 60,
			TimeoutSeconds:      2 * 60 * 60,
			SpoolDir:            "./data/backups",
			KeepLocal:           true,
			LocalRetentionCount: 3,
			Upload: BackupUploadConfig{
				Enabled:            false,
				Prefix:             "anhebridgedb",
				AccessKeyEnv:       "ANHEBRIDGE_R2_ACCESS_KEY_ID",
				SecretAccessKeyEnv: "ANHEBRIDGE_R2_SECRET_ACCESS_KEY",
				MaxObjectBytes:     5 * 1024 * 1024 * 1024,
				RetryCount:         3,
				RetryBackoffMS:     1000,
			},
			Lark: BackupLarkConfig{
				Enabled:       false,
				WebhookEnv:    "ANHEBRIDGE_LARK_WEBHOOK",
				SecretEnv:     "ANHEBRIDGE_LARK_SECRET",
				NotifySuccess: true,
			},
			Monitor: BackupMonitorConfig{
				Enabled:               true,
				IntervalSeconds:       60,
				VerifyIntervalSeconds: 24 * 60 * 60,
				ConsecutiveBreaches:   3,
				AlertCooldownSeconds:  30 * 60,
				HeapAllocMaxBytes:     0,
				PendingTasksMax:       0,
				OverdueTasksMax:       0,
				WriteQueueMax:         0,
				AppendP95MaxMS:        0,
				GetP95MaxMS:           0,
			},
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
	normalizeBackupConfig(&cfg.Backup)
	return cfg, nil
}

func normalizeBackupConfig(cfg *BackupConfig) {
	defaults := Default().Backup
	switch cfg.Mode {
	case "full", "FULL":
		cfg.Mode = "full"
	case "incremental", "INCREMENTAL", "":
		cfg.Mode = "incremental"
	default:
		cfg.Mode = defaults.Mode
	}
	if cfg.IntervalSeconds <= 0 {
		cfg.IntervalSeconds = defaults.IntervalSeconds
	}
	if cfg.TimeoutSeconds <= 0 {
		cfg.TimeoutSeconds = defaults.TimeoutSeconds
	}
	if cfg.SpoolDir == "" {
		cfg.SpoolDir = defaults.SpoolDir
	}
	if cfg.LocalRetentionCount < 0 {
		cfg.LocalRetentionCount = 0
	}
	if cfg.Upload.Prefix == "" {
		cfg.Upload.Prefix = defaults.Upload.Prefix
	}
	if cfg.Upload.AccessKeyEnv == "" {
		cfg.Upload.AccessKeyEnv = defaults.Upload.AccessKeyEnv
	}
	if cfg.Upload.SecretAccessKeyEnv == "" {
		cfg.Upload.SecretAccessKeyEnv = defaults.Upload.SecretAccessKeyEnv
	}
	if cfg.Upload.MaxObjectBytes <= 0 {
		cfg.Upload.MaxObjectBytes = defaults.Upload.MaxObjectBytes
	}
	if cfg.Upload.RetryCount < 0 {
		cfg.Upload.RetryCount = 0
	}
	if cfg.Upload.RetryCount > 10 {
		cfg.Upload.RetryCount = 10
	}
	if cfg.Upload.RetryBackoffMS <= 0 {
		cfg.Upload.RetryBackoffMS = defaults.Upload.RetryBackoffMS
	}
	if cfg.Lark.WebhookEnv == "" {
		cfg.Lark.WebhookEnv = defaults.Lark.WebhookEnv
	}
	if cfg.Lark.SecretEnv == "" {
		cfg.Lark.SecretEnv = defaults.Lark.SecretEnv
	}
	if cfg.Monitor.IntervalSeconds < 10 {
		cfg.Monitor.IntervalSeconds = defaults.Monitor.IntervalSeconds
	}
	if cfg.Monitor.VerifyIntervalSeconds < 0 {
		cfg.Monitor.VerifyIntervalSeconds = 0
	}
	if cfg.Monitor.ConsecutiveBreaches <= 0 {
		cfg.Monitor.ConsecutiveBreaches = defaults.Monitor.ConsecutiveBreaches
	}
	if cfg.Monitor.AlertCooldownSeconds <= 0 {
		cfg.Monitor.AlertCooldownSeconds = defaults.Monitor.AlertCooldownSeconds
	}
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
