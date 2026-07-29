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
	if cfg.Backup.BootstrapRestore.Enabled {
		t.Fatal("bootstrap restore must be opt-in")
	}
	if cfg.Backup.BootstrapRestore.Manifest != "latest" ||
		cfg.Backup.BootstrapRestore.Workers != 4 ||
		cfg.Backup.BootstrapRestore.TimeoutSeconds != 7200 {
		t.Fatalf("unexpected bootstrap restore defaults: %+v", cfg.Backup.BootstrapRestore)
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

func TestLoadNormalizesBootstrapRestoreConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	content := `{
		"backup": {
			"bootstrap_restore": {
				"enabled": true,
				"manifest": "",
				"workers": 100,
				"timeout_seconds": 0
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
	if !cfg.Backup.BootstrapRestore.Enabled {
		t.Fatal("expected bootstrap restore enabled")
	}
	if cfg.Backup.BootstrapRestore.Manifest != "latest" {
		t.Fatalf("manifest = %q, want latest", cfg.Backup.BootstrapRestore.Manifest)
	}
	if cfg.Backup.BootstrapRestore.Workers != 32 {
		t.Fatalf("workers = %d, want capped value 32", cfg.Backup.BootstrapRestore.Workers)
	}
	if cfg.Backup.BootstrapRestore.TimeoutSeconds != 7200 {
		t.Fatalf("timeout = %d, want default", cfg.Backup.BootstrapRestore.TimeoutSeconds)
	}
}
