package db

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paerx/anhebridgedb/internal/storage"
)

type opMetric struct {
	mu      sync.Mutex
	samples []float64
	next    int
	count   uint64
}

func newOpMetric(size int) *opMetric {
	return &opMetric{samples: make([]float64, size)}
}

func (m *opMetric) observe(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.samples) == 0 {
		return
	}
	m.samples[m.next%len(m.samples)] = float64(d.Microseconds()) / 1000.0
	m.next++
	atomic.AddUint64(&m.count, 1)
}

func (m *opMetric) snapshot(prefix string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	values := make([]float64, 0, len(m.samples))
	limit := len(m.samples)
	if m.next < limit {
		limit = m.next
	}
	for i := 0; i < limit; i++ {
		if m.samples[i] > 0 {
			values = append(values, m.samples[i])
		}
	}
	sort.Float64s(values)
	p50, p95, avg := 0.0, 0.0, 0.0
	if len(values) > 0 {
		p50 = percentile(values, 0.50)
		p95 = percentile(values, 0.95)
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		avg = sum / float64(len(values))
	}
	return map[string]any{
		prefix + "_count":  atomic.LoadUint64(&m.count),
		prefix + "_avg_ms": avg,
		prefix + "_p50_ms": p50,
		prefix + "_p95_ms": p95,
	}
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(values))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

type perfMetrics struct {
	get           *opMetric
	getAt         *opMetric
	timeline      *opMetric
	check         *opMetric
	set           *opMetric
	aset          *opMetric
	keyLockWait   *opMetric
	append        *opMetric
	keyIndexFlush *opMetric
	scheduler     *opMetric
	recovery      *opMetric
	deriveMu      sync.Mutex
	lastSampleAt  time.Time
	lastCPUSec    float64
	lastSegBytes  float64
	lastReadOps   uint64
	lastWriteOps  uint64
}

func newPerfMetrics() *perfMetrics {
	return &perfMetrics{
		get:           newOpMetric(2048),
		getAt:         newOpMetric(2048),
		timeline:      newOpMetric(1024),
		check:         newOpMetric(1024),
		set:           newOpMetric(2048),
		aset:          newOpMetric(1024),
		keyLockWait:   newOpMetric(4096),
		append:        newOpMetric(4096),
		keyIndexFlush: newOpMetric(1024),
		scheduler:     newOpMetric(1024),
		recovery:      newOpMetric(32),
	}
}

func observe(metric *opMetric, start time.Time) {
	if metric != nil {
		metric.observe(time.Since(start))
	}
}

func (e *Engine) Metrics() map[string]any {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	cpuSec := runtimeCPUTotalSeconds()
	goroutines := runtime.NumGoroutine()

	metrics := map[string]any{
		"heap_alloc_bytes":     mem.HeapAlloc,
		"heap_inuse_bytes":     mem.HeapInuse,
		"heap_objects":         mem.HeapObjects,
		"memory_alloc_mb":      float64(mem.HeapAlloc) / (1024.0 * 1024.0),
		"memory_inuse_mb":      float64(mem.HeapInuse) / (1024.0 * 1024.0),
		"memory_sys_mb":        float64(mem.Sys) / (1024.0 * 1024.0),
		"gc_pause_total_ms":    float64(mem.PauseTotalNs) / 1_000_000.0,
		"goroutines":           goroutines,
		"state_keys":           e.countStateKeys(),
		"loaded_event_refs":    e.countEventRefs(),
		"segment_count":        e.log.Stats()["segments"],
		"segment_total_bytes":  e.log.Stats()["segment_total_bytes"],
		"latest_index_bytes":   storageFileSize(storage.KeyIndexCurrentPath(e.dataDir)) + storageFileSize(storage.KeyIndexDeltaPath(e.dataDir)),
		"position_index_bytes": storageFileSize(storage.PositionIndexPath(e.dataDir)),
		"snapshot_bytes":       storageFileSize(storage.SnapshotPath(e.dataDir)),
		"manifest_bytes":       totalManifestBytes(filepath.Join(e.dataDir, "log")),
	}
	now := time.Now().UTC()
	if mem.Sys > 0 {
		metrics["memory_heap_usage_percent"] = (float64(mem.HeapAlloc) / float64(mem.Sys)) * 100.0
	} else {
		metrics["memory_heap_usage_percent"] = 0.0
	}
	dueTasks, overdueTasks, oldestAge := e.bucketBacklog(now)
	metrics["pending_tasks"] = e.pendingTasksLocked()
	metrics["due_tasks"] = dueTasks
	metrics["overdue_tasks"] = overdueTasks
	metrics["oldest_pending_age_sec"] = oldestAge
	for k, v := range e.metrics.get.snapshot("get") {
		metrics[k] = v
	}
	for k, v := range e.metrics.getAt.snapshot("get_at") {
		metrics[k] = v
	}
	for k, v := range e.metrics.timeline.snapshot("timeline") {
		metrics[k] = v
	}
	for k, v := range e.metrics.check.snapshot("check") {
		metrics[k] = v
	}
	for k, v := range e.metrics.set.snapshot("set") {
		metrics[k] = v
	}
	for k, v := range e.metrics.aset.snapshot("aset") {
		metrics[k] = v
	}
	for k, v := range e.metrics.keyLockWait.snapshot("key_lock_wait") {
		metrics[k] = v
	}
	for k, v := range e.metrics.append.snapshot("append") {
		metrics[k] = v
	}
	for k, v := range e.metrics.keyIndexFlush.snapshot("latest_index_flush") {
		metrics[k] = v
	}
	for k, v := range e.metrics.scheduler.snapshot("scheduler_run") {
		metrics[k] = v
	}
	for k, v := range e.metrics.recovery.snapshot("recovery_duration") {
		metrics[k] = v
	}
	for k, v := range e.eventCache.stats() {
		metrics[k] = v
	}
	e.decorateDerivedRuntimeMetrics(metrics, now, cpuSec)
	return metrics
}

func (e *Engine) decorateDerivedRuntimeMetrics(metrics map[string]any, now time.Time, cpuSec float64) {
	readOps := opCount(e.metrics.get) + opCount(e.metrics.getAt) + opCount(e.metrics.timeline) + opCount(e.metrics.check)
	writeOps := opCount(e.metrics.set) + opCount(e.metrics.aset) + opCount(e.metrics.append)
	segBytes := anyToFloat64(metrics["segment_total_bytes"])

	e.metrics.deriveMu.Lock()
	defer e.metrics.deriveMu.Unlock()

	metrics["cpu_percent"] = 0.0
	metrics["cpu_cores_used"] = 0.0
	metrics["io_read_ops_per_sec"] = 0.0
	metrics["io_write_ops_per_sec"] = 0.0
	metrics["io_write_bytes_per_sec"] = 0.0
	metrics["io_write_mb_per_sec"] = 0.0

	if !e.metrics.lastSampleAt.IsZero() {
		elapsed := now.Sub(e.metrics.lastSampleAt).Seconds()
		if elapsed > 0 {
			dCPU := cpuSec - e.metrics.lastCPUSec
			if dCPU < 0 {
				dCPU = 0
			}
			coresUsed := dCPU / elapsed
			cpuPercent := 0.0
			if runtime.NumCPU() > 0 {
				cpuPercent = coresUsed * 100.0 / float64(runtime.NumCPU())
			}
			if cpuPercent < 0 {
				cpuPercent = 0
			}
			metrics["cpu_cores_used"] = coresUsed
			metrics["cpu_percent"] = cpuPercent

			dSeg := segBytes - e.metrics.lastSegBytes
			if dSeg < 0 {
				dSeg = 0
			}
			writeBps := dSeg / elapsed
			metrics["io_write_bytes_per_sec"] = writeBps
			metrics["io_write_mb_per_sec"] = writeBps / (1024.0 * 1024.0)

			if readOps >= e.metrics.lastReadOps {
				metrics["io_read_ops_per_sec"] = float64(readOps-e.metrics.lastReadOps) / elapsed
			}
			if writeOps >= e.metrics.lastWriteOps {
				metrics["io_write_ops_per_sec"] = float64(writeOps-e.metrics.lastWriteOps) / elapsed
			}
		}
	}

	e.metrics.lastSampleAt = now
	e.metrics.lastCPUSec = cpuSec
	e.metrics.lastSegBytes = segBytes
	e.metrics.lastReadOps = readOps
	e.metrics.lastWriteOps = writeOps
}

func (e *Engine) StartMetricsSampler(ctx context.Context, interval time.Duration) {
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = storage.AppendMetricSample(e.dataDir, storage.MetricSample{
					Timestamp: time.Now().UTC(),
					Metrics:   numericMetrics(e.Metrics()),
				})
			}
		}
	}()
}

func (e *Engine) MetricsHistory(metrics []string, from, to time.Time, bucket time.Duration) (map[string][]storage.MetricOHLC, error) {
	samples, err := storage.ReadMetricSamples(e.dataDir, from, to)
	if err != nil {
		return nil, err
	}
	series := make(map[string][]storage.MetricOHLC, len(metrics))
	for _, metric := range metrics {
		series[metric] = storage.AggregateMetricOHLC(samples, metric, bucket)
	}
	return series, nil
}

func (e *Engine) DebugPerf() map[string]any {
	buckets := make([]map[string]any, 0, len(e.buckets))
	for bucket, meta := range e.buckets {
		buckets = append(buckets, map[string]any{
			"bucket":         bucket,
			"path":           meta.Path,
			"total":          meta.Total,
			"pending":        meta.Pending,
			"oldest_pending": meta.OldestPending,
		})
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i]["bucket"].(time.Time).Before(buckets[j]["bucket"].(time.Time))
	})
	stateShards := e.state.shardSizes()
	refShards := e.eventRefs.shardSizes()
	keyIndexShards := e.keyIndex.shardSizes()
	shards := make([]map[string]any, 0, keyLockCount)
	for idx := 0; idx < keyLockCount; idx++ {
		shards = append(shards, map[string]any{
			"shard":          idx,
			"state_keys":     stateShards[idx],
			"event_refs":     refShards[idx],
			"key_index_keys": keyIndexShards[idx],
		})
	}
	manifests, _ := storage.LoadSegmentManifests(filepath.Join(e.dataDir, "log"))
	return map[string]any{
		"metrics":        e.Metrics(),
		"bucket_backlog": buckets,
		"event_cache":    e.eventCache.stats(),
		"segments":       manifests,
		"shards":         shards,
	}
}

func numericMetrics(snapshot map[string]any) map[string]float64 {
	out := make(map[string]float64, len(snapshot))
	for key, value := range snapshot {
		switch v := value.(type) {
		case int:
			out[key] = float64(v)
		case int64:
			out[key] = float64(v)
		case uint64:
			out[key] = float64(v)
		case float64:
			out[key] = v
		case float32:
			out[key] = float64(v)
		}
	}
	return out
}

func runtimeCPUTotalSeconds() float64 {
	samples := []runtimemetrics.Sample{
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
	}
	runtimemetrics.Read(samples)
	total := sampleFloat64(samples[0])
	if total > 0 {
		return total
	}
	return sampleFloat64(samples[1]) + sampleFloat64(samples[2])
}

func sampleFloat64(sample runtimemetrics.Sample) float64 {
	if sample.Value.Kind() == runtimemetrics.KindFloat64 {
		return sample.Value.Float64()
	}
	return 0
}

func opCount(metric *opMetric) uint64 {
	if metric == nil {
		return 0
	}
	return atomic.LoadUint64(&metric.count)
}

func anyToFloat64(value any) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case uint64:
		return float64(v)
	case float64:
		return v
	case float32:
		return float64(v)
	default:
		return 0
	}
}

func storageFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func totalManifestBytes(logDir string) int64 {
	manifests, err := storage.LoadSegmentManifests(logDir)
	if err != nil {
		return 0
	}
	var total int64
	for _, manifest := range manifests {
		size := storageFileSize(storage.SegmentManifestPath(logDir, manifest.Segment))
		if size == 0 {
			size = storageFileSize(storage.ArchiveSegmentManifestPath(logDir, manifest.Segment))
		}
		total += size
	}
	total += storageFileSize(storage.ArchiveManifestPath(logDir))
	return total
}

func (e *Engine) bucketBacklog(now time.Time) (int, int, int64) {
	due := 0
	overdue := 0
	var oldest int64
	for bucket, meta := range e.buckets {
		if meta.Pending == 0 {
			continue
		}
		if !bucket.After(now) {
			due += meta.Pending
			if now.Sub(bucket) > time.Minute {
				overdue += meta.Pending
			}
		}
		if meta.OldestPending != nil {
			age := int64(now.Sub(*meta.OldestPending).Seconds())
			if age > oldest {
				oldest = age
			}
		}
	}
	return due, overdue, oldest
}
