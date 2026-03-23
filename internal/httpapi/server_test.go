package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
	"github.com/paerx/anhebridgedb/internal/db"
	"github.com/paerx/anhebridgedb/internal/storage"
)

func TestMetricsHistoryEndpoint(t *testing.T) {
	dir := t.TempDir()
	engine, err := db.Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	base := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	for _, sample := range []storage.MetricSample{
		{Timestamp: base.Add(1 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 10}},
		{Timestamp: base.Add(5 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 20}},
		{Timestamp: base.Add(11 * time.Second), Metrics: map[string]float64{"heap_alloc_bytes": 15}},
	} {
		if err := storage.AppendMetricSample(dir, sample); err != nil {
			t.Fatalf("append metric sample: %v", err)
		}
	}

	req, err := http.NewRequest(http.MethodGet, "/metrics/history?metrics=heap_alloc_bytes&from=2026-03-15T12:00:00Z&to=2026-03-15T12:00:20Z&bucket=10s", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	rec := httptest.NewRecorder()
	New(engine, nil, config.Default().Performance).Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Metrics map[string][]storage.MetricOHLC `json:"metrics"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Metrics["heap_alloc_bytes"]) != 2 {
		t.Fatalf("expected 2 history buckets, got %d", len(payload.Metrics["heap_alloc_bytes"]))
	}
}
