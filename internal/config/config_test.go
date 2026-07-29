package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupDefaultsRemainDisabled(t *testing.T) {
	cfg := Default()
	if cfg.Backup.Enabled {
		t.Fatal("automatic backup must be opt-in")
	}
	if cfg.Backup.IntervalSeconds <= 0 || cfg.Backup.TimeoutSeconds <= 0 {
		t.Fatal("backup defaults must have bounded scheduling values")
	}
	if cfg.Backup.Mode != "incremental" || cfg.Backup.IntervalSeconds != 7200 {
		t.Fatalf("unexpected incremental defaults: mode=%s interval=%d", cfg.Backup.Mode, cfg.Backup.IntervalSeconds)
	}
}

func TestLoadNormalizesPartialBackupConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"backup":{"enabled":true,"interval_seconds":0,"upload":{"enabled":false}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Backup.Enabled {
		t.Fatal("expected backup enabled")
	}
	if cfg.Backup.IntervalSeconds != Default().Backup.IntervalSeconds {
		t.Fatalf("interval = %d, want default", cfg.Backup.IntervalSeconds)
	}
	if cfg.Backup.Upload.Prefix == "" {
		t.Fatal("expected default R2 object prefix")
	}
}

func TestLoadBackupCredentialsFromConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"backup": {
			"upload": {
				"access_key_id": "access",
				"secret_access_key": "secret"
			},
			"lark": {
				"webhook": "https://example.test/hook",
				"secret": "signing-secret"
			}
		}
	}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backup.Upload.AccessKeyID != "access" || cfg.Backup.Upload.SecretAccessKey != "secret" {
		t.Fatal("R2 credentials were not loaded from config")
	}
	if cfg.Backup.Lark.Webhook != "https://example.test/hook" || cfg.Backup.Lark.Secret != "signing-secret" {
		t.Fatal("Lark credentials were not loaded from config")
	}
}
