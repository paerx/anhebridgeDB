package config

import (
	"encoding/json"
	"errors"
	"os"
)

type Config struct {
	Storage StorageConfig `json:"storage"`
}

type StorageConfig struct {
	Segment SegmentConfig `json:"segment"`
}

type SegmentConfig struct {
	MaxBytes   int64 `json:"max_bytes"`
	MaxRecords int   `json:"max_records"`
}

func Default() Config {
	return Config{
		Storage: StorageConfig{
			Segment: SegmentConfig{
				MaxBytes:   64 * 1024 * 1024,
				MaxRecords: 30000,
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
	return cfg, nil
}
