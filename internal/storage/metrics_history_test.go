package storage

import (
	"testing"
	"time"
)

func TestMetricHistoryRoundTripAndOHLC(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	samples := []MetricSample{
		{Timestamp: base.Add(1 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 10}},
		{Timestamp: base.Add(5 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 20}},
		{Timestamp: base.Add(11 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 15}},
		{Timestamp: base.Add(18 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 30}},
	}
	for _, sample := range samples {
		if err := AppendMetricSample(dir, sample); err != nil {
			t.Fatalf("append sample: %v", err)
		}
	}
	loaded, err := ReadMetricSamples(dir, base, base.Add(30*time.Second))
	if err != nil {
		t.Fatalf("read samples: %v", err)
	}
	if len(loaded) != len(samples) {
		t.Fatalf("expected %d samples, got %d", len(samples), len(loaded))
	}
	series := AggregateMetricOHLC(loaded, "heap_alloc_bytes", 10*time.Second)
	if len(series) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(series))
	}
	if series[0].Open != 10 || series[0].High != 20 || series[0].Low != 10 || series[0].Close != 20 {
		t.Fatalf("unexpected first bucket: %+v", series[0])
	}
	if series[1].Open != 15 || series[1].High != 30 || series[1].Low != 15 || series[1].Close != 30 {
		t.Fatalf("unexpected second bucket: %+v", series[1])
	}
}
