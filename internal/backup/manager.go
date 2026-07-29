package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/storage"
)

type Manager struct {
	engine    *db.Engine
	cfg       config.BackupConfig
	uploader  *r2Uploader
	notifier  *larkNotifier
	uploadErr error
	notifyErr error
	notifyCh  chan string
	wg        sync.WaitGroup
	runMu     sync.Mutex

	monitorMu      sync.Mutex
	breachCounts   map[string]int
	lastAlertTimes map[string]time.Time
}

func NewManager(engine *db.Engine, cfg config.BackupConfig) (*Manager, error) {
	if engine == nil {
		return nil, fmt.Errorf("backup engine is required")
	}
	manager := &Manager{
		engine:         engine,
		cfg:            cfg,
		notifyCh:       make(chan string, 64),
		breachCounts:   map[string]int{},
		lastAlertTimes: map[string]time.Time{},
	}
	if cfg.Upload.Enabled {
		uploader, err := newR2Uploader(cfg.Upload)
		if err != nil {
			manager.uploadErr = err
		} else {
			manager.uploader = uploader
		}
	}
	if cfg.Lark.Enabled {
		notifier, err := newLarkNotifier(cfg.Lark)
		if err != nil {
			manager.notifyErr = err
		} else {
			manager.notifier = notifier
		}
	}
	return manager, nil
}

func (m *Manager) Start(ctx context.Context) {
	if !m.cfg.Enabled {
		return
	}
	if m.uploadErr != nil {
		log.Printf("automatic backup R2 initialization failed; local backups remain active: %v", m.uploadErr)
	}
	if m.notifyErr != nil {
		log.Printf("automatic backup Lark initialization failed; backup remains active: %v", m.notifyErr)
	}
	if m.notifier != nil {
		m.startWorker(ctx, "backup notifier", m.notificationLoop)
	}
	m.startWorker(ctx, "backup scheduler", m.backupLoop)
	if m.cfg.Monitor.Enabled {
		m.startWorker(ctx, "backup monitor", m.monitorLoop)
		if m.cfg.Monitor.VerifyIntervalSeconds > 0 {
			m.startWorker(ctx, "storage verifier", m.verifyLoop)
		}
	}
}

func (m *Manager) Wait(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (m *Manager) startWorker(ctx context.Context, name string, worker func(context.Context)) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("%s panic recovered: %v", name, recovered)
				m.notify(fmt.Sprintf("[AnheBridgeDB] %s panic: %v", name, recovered))
			}
		}()
		worker(ctx)
	}()
}

func (m *Manager) backupLoop(ctx context.Context) {
	if m.cfg.RunOnStart {
		m.runBackupSafely(ctx)
	}
	ticker := time.NewTicker(time.Duration(m.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runBackupSafely(ctx)
		}
	}
}

func (m *Manager) runBackupSafely(parent context.Context) {
	timeout := time.Duration(m.cfg.TimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	start := time.Now()
	filename, objectKey, size, lastEventID, err := m.runBackup(ctx)
	m.engine.RecordBackupResult(time.Since(start), size, err)
	if err != nil {
		log.Printf("automatic backup failed: %v", err)
		m.notify(fmt.Sprintf("[AnheBridgeDB] backup FAILED\nerror: %v\nduration: %s", err, time.Since(start).Round(time.Millisecond)))
		return
	}
	log.Printf("automatic backup completed: file=%s object=%s bytes=%d last_event_id=%d duration=%s",
		filename, objectKey, size, lastEventID, time.Since(start).Round(time.Millisecond))
	if m.cfg.Lark.NotifySuccess {
		m.notify(fmt.Sprintf(
			"[AnheBridgeDB] backup SUCCESS\nfile: %s\nobject: %s\nbytes: %d\nlast_event_id: %d\nduration: %s",
			filepath.Base(filename), objectKey, size, lastEventID, time.Since(start).Round(time.Millisecond),
		))
	}
}

func (m *Manager) runBackup(ctx context.Context) (filename, objectKey string, size int64, lastEventID uint64, err error) {
	m.runMu.Lock()
	defer m.runMu.Unlock()

	if m.cfg.Mode == "incremental" && m.uploader != nil {
		return m.runIncrementalBackup(ctx)
	}
	return m.runFullBackup(ctx)
}

func (m *Manager) runFullBackup(ctx context.Context) (filename, objectKey string, size int64, lastEventID uint64, err error) {
	if err := os.MkdirAll(m.cfg.SpoolDir, 0o755); err != nil {
		return "", "", 0, 0, err
	}
	viewParent := filepath.Join(m.cfg.SpoolDir, ".views")
	view, err := m.engine.CreateBackupView(viewParent)
	if err != nil {
		return "", "", 0, 0, err
	}
	defer view.Remove()

	stamp := view.CreatedAt.UTC().Format("20060102T150405Z")
	base := fmt.Sprintf("anhebridgedb-full-%s-e%d.tar.gz", stamp, view.LastEventID)
	finalPath := filepath.Join(m.cfg.SpoolDir, base)
	partialPath := finalPath + ".partial"
	_ = os.Remove(partialPath)
	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", "", 0, view.LastEventID, err
	}
	exportErr := storage.ExportBackup(file, view.Path, "full", nil)
	if syncErr := file.Sync(); exportErr == nil {
		exportErr = syncErr
	}
	if closeErr := file.Close(); exportErr == nil {
		exportErr = closeErr
	}
	if exportErr != nil {
		_ = os.Remove(partialPath)
		return "", "", 0, view.LastEventID, exportErr
	}
	if err := os.Rename(partialPath, finalPath); err != nil {
		_ = os.Remove(partialPath)
		return "", "", 0, view.LastEventID, err
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		return finalPath, "", 0, view.LastEventID, err
	}
	size = info.Size()

	checksum, err := fileSHA256(finalPath)
	if err != nil {
		return finalPath, "", size, view.LastEventID, err
	}
	if err := os.WriteFile(finalPath+".sha256", []byte(checksum+"  "+base+"\n"), 0o600); err != nil {
		return finalPath, "", size, view.LastEventID, err
	}

	if m.uploader != nil {
		objectKey = m.uploader.objectKey(base, view.CreatedAt)
		err = m.retry(ctx, func() error {
			return m.uploader.upload(ctx, finalPath, objectKey)
		})
		if err != nil {
			return finalPath, objectKey, size, view.LastEventID, err
		}
	} else if m.cfg.Upload.Enabled {
		return finalPath, "", size, view.LastEventID, m.uploadErr
	}
	if !m.cfg.KeepLocal && m.uploader != nil {
		_ = os.Remove(finalPath)
		_ = os.Remove(finalPath + ".sha256")
	} else {
		m.pruneLocalBackups()
	}
	return finalPath, objectKey, size, view.LastEventID, nil
}

func (m *Manager) retry(ctx context.Context, operation func() error) error {
	var lastErr error
	for attempt := 0; attempt <= m.cfg.Upload.RetryCount; attempt++ {
		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == m.cfg.Upload.RetryCount {
			break
		}
		delay := time.Duration(m.cfg.Upload.RetryBackoffMS) * time.Millisecond * time.Duration(1<<attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (m *Manager) pruneLocalBackups() {
	if m.cfg.LocalRetentionCount <= 0 {
		return
	}
	matches, _ := filepath.Glob(filepath.Join(m.cfg.SpoolDir, "anhebridgedb-full-*.tar.gz"))
	sort.Slice(matches, func(i, j int) bool {
		left, _ := os.Stat(matches[i])
		right, _ := os.Stat(matches[j])
		if left == nil || right == nil {
			return matches[i] > matches[j]
		}
		return left.ModTime().After(right.ModTime())
	})
	if len(matches) <= m.cfg.LocalRetentionCount {
		return
	}
	for _, filename := range matches[m.cfg.LocalRetentionCount:] {
		_ = os.Remove(filename)
		_ = os.Remove(filename + ".sha256")
	}
}

func fileSHA256(filename string) (string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
