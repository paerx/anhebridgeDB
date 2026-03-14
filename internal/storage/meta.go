package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Rule struct {
	ID           string    `json:"id"`
	Pattern      string    `json:"pattern"`
	Target       string    `json:"target"`
	Delay        string    `json:"delay"`
	DelaySeconds int64     `json:"delay_seconds"`
	Enabled      bool      `json:"enabled"`
	CreatedAt    time.Time `json:"created_at"`
	MatchCount   uint64    `json:"match_count"`
	ExecuteCount uint64    `json:"execute_count"`
	SkipCount    uint64    `json:"skip_count"`
}

type Task struct {
	ID              string     `json:"id"`
	BucketTS        time.Time  `json:"bucket_ts"`
	RuleID          string     `json:"rule_id"`
	EntityKey       string     `json:"entity_key"`
	ExpectedVersion uint64     `json:"expected_version"`
	ExpectedState   string     `json:"expected_state"`
	ToState         string     `json:"to_state"`
	CauseEventID    uint64     `json:"cause_event_id"`
	Status          string     `json:"status"`
	CreatedAt       time.Time  `json:"created_at"`
	ProcessedAt     *time.Time `json:"processed_at,omitempty"`
}

type FastEntry struct {
	Key        string    `json:"key"`
	Value      []byte    `json:"value"`
	Version    uint64    `json:"version"`
	UpdatedAt  time.Time `json:"updated_at"`
	AccessedAt time.Time `json:"accessed_at"`
}

func RulesPath(dir string) string {
	return filepath.Join(dir, "system", "rules.json")
}

func TaskBucketDir(dir string) string {
	return filepath.Join(dir, "system", "task_buckets")
}

func TaskBucketPath(dir string, bucket time.Time) string {
	return filepath.Join(TaskBucketDir(dir), bucket.UTC().Format("200601021504")+".anhe")
}

func LoadAllTasks(dir string) ([]Task, error) {
	buckets, err := listTaskBucketPaths(dir)
	if err != nil {
		return nil, err
	}

	var tasks []Task
	for _, path := range buckets {
		var bucketTasks []Task
		if err := LoadJSON(path, &bucketTasks); err != nil {
			return nil, err
		}
		tasks = append(tasks, bucketTasks...)
	}
	return tasks, nil
}

func SaveTaskBucket(dir string, bucket time.Time, tasks []Task) error {
	path := TaskBucketPath(dir, bucket)
	if len(tasks) == 0 {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].ID < tasks[j].ID
	})
	return SaveJSON(path, tasks)
}

func DueTaskBucketPaths(dir string, now time.Time) ([]string, error) {
	paths, err := listTaskBucketPaths(dir)
	if err != nil {
		return nil, err
	}

	var due []string
	for _, path := range paths {
		bucket, err := parseBucketTime(filepath.Base(path))
		if err != nil {
			return nil, err
		}
		if !bucket.After(now.UTC()) {
			due = append(due, path)
		}
	}
	return due, nil
}

func parseBucketTime(name string) (time.Time, error) {
	base := strings.TrimSuffix(name, ".anhe")
	return time.Parse("200601021504", base)
}

func listTaskBucketPaths(dir string) ([]string, error) {
	root := TaskBucketDir(dir)
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".anhe") {
			continue
		}
		paths = append(paths, filepath.Join(root, entry.Name()))
	}
	sort.Strings(paths)
	return paths, nil
}
