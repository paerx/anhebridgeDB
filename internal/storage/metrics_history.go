package storage

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type MetricSample struct {
	Timestamp time.Time          `json:"ts"`
	Metrics   map[string]float64 `json:"metrics"`
}

type MetricOHLC struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	Open  float64   `json:"open"`
	High  float64   `json:"high"`
	Low   float64   `json:"low"`
	Close float64   `json:"close"`
	Count int       `json:"count"`
}

func MetricsHistoryDir(dir string) string {
	return filepath.Join(dir, "system", "metrics")
}

func MetricsHistoryPath(dir string, at time.Time) string {
	return filepath.Join(MetricsHistoryDir(dir), at.UTC().Format("2006010215")+".anhe")
}

func AppendMetricSample(dir string, sample MetricSample) error {
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now().UTC()
	}
	path := MetricsHistoryPath(dir, sample.Timestamp)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := json.Marshal(sample)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(bytes, '\n')); err != nil {
		return err
	}
	return nil
}

func ReadMetricSamples(dir string, from, to time.Time) ([]MetricSample, error) {
	root := MetricsHistoryDir(dir)
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
		hour, err := time.Parse("2006010215", strings.TrimSuffix(entry.Name(), ".anhe"))
		if err != nil {
			continue
		}
		if !from.IsZero() && hour.Add(time.Hour).Before(from.UTC()) {
			continue
		}
		if !to.IsZero() && hour.After(to.UTC()) {
			continue
		}
		paths = append(paths, filepath.Join(root, entry.Name()))
	}
	sort.Strings(paths)

	samples := make([]MetricSample, 0)
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var sample MetricSample
			if err := json.Unmarshal(scanner.Bytes(), &sample); err != nil {
				_ = file.Close()
				return nil, err
			}
			if !from.IsZero() && sample.Timestamp.Before(from.UTC()) {
				continue
			}
			if !to.IsZero() && sample.Timestamp.After(to.UTC()) {
				continue
			}
			samples = append(samples, sample)
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		_ = file.Close()
	}
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Timestamp.Before(samples[j].Timestamp)
	})
	return samples, nil
}

func AggregateMetricOHLC(samples []MetricSample, metric string, bucket time.Duration) []MetricOHLC {
	if bucket <= 0 {
		bucket = 10 * time.Second
	}
	type bucketState struct {
		MetricOHLC
	}
	series := make([]MetricOHLC, 0)
	var current *bucketState
	for _, sample := range samples {
		value, ok := sample.Metrics[metric]
		if !ok {
			continue
		}
		bucketStart := sample.Timestamp.UTC().Truncate(bucket)
		if current == nil || !current.Start.Equal(bucketStart) {
			if current != nil {
				series = append(series, current.MetricOHLC)
			}
			current = &bucketState{
				MetricOHLC: MetricOHLC{
					Start: bucketStart,
					End:   bucketStart.Add(bucket),
					Open:  value,
					High:  value,
					Low:   value,
					Close: value,
					Count: 1,
				},
			}
			continue
		}
		if value > current.High {
			current.High = value
		}
		if value < current.Low {
			current.Low = value
		}
		current.Close = value
		current.Count++
	}
	if current != nil {
		series = append(series, current.MetricOHLC)
	}
	return series
}

func ParseMetricHistoryWindow(fromRaw, toRaw string) (time.Time, time.Time, error) {
	var from time.Time
	var to time.Time
	var err error
	if strings.TrimSpace(fromRaw) != "" {
		from, err = time.Parse(time.RFC3339, strings.TrimSpace(fromRaw))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid from")
		}
	}
	if strings.TrimSpace(toRaw) != "" {
		to, err = time.Parse(time.RFC3339, strings.TrimSpace(toRaw))
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid to")
		}
	}
	if from.IsZero() && to.IsZero() {
		to = time.Now().UTC()
		from = to.Add(-time.Hour)
	}
	if from.IsZero() {
		from = to.Add(-time.Hour)
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}
	return from.UTC(), to.UTC(), nil
}
