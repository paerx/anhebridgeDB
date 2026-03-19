package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paerx/anhebridgedb/sdk/anhe"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:8080", "anhe server address")
	username := flag.String("username", "", "auth username")
	password := flag.String("password", "", "auth password")
	token := flag.String("token", "", "auth bearer token")
	writeWorkers := flag.Int("write-workers", 16, "concurrent write workers")
	readWorkers := flag.Int("read-workers", 32, "concurrent read workers")
	readDuration := flag.Duration("read-duration", 15*time.Second, "high frequency read duration")
	flag.Parse()

	db, err := anhe.Open(anhe.Config{
		Addr:       *addr,
		Username:   *username,
		Password:   *password,
		Token:      *token,
		WSPoolSize: 8,
		Timeout:    10 * time.Second,
	})
	if err != nil {
		log.Fatalf("open sdk: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Key("sdkdemo:balance:base").Set(ctx, 1000); err != nil {
		log.Fatalf("seed base key: %v", err)
	}

	runIdempotencyTest(ctx, db)
	runBatchDemo(ctx, db)
	keys := runConcurrentWriteTest(ctx, db, *writeWorkers)
	runHighFrequencyReadTest(ctx, db, keys, *readWorkers, *readDuration)
}

func runIdempotencyTest(ctx context.Context, db *anhe.DB) {
	fmt.Println("== idempotency test ==")
	const idemKey = "sdkdemo-idem-001"
	const goroutines = 40
	var wg sync.WaitGroup
	errCount := int64(0)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Key("sdkdemo:balance:idem").Event("SEND").Idempotency(idemKey).SetDelta(ctx, -10)
			if err != nil {
				atomic.AddInt64(&errCount, 1)
			}
		}()
	}
	wg.Wait()

	page, err := db.Search().
		Events().
		Key("sdkdemo:balance:idem").
		Name("SEND").
		I(idemKey).
		Desc().
		Limit(100).
		Page(1).
		Find(ctx)
	if err != nil {
		log.Fatalf("search idempotency result: %v", err)
	}
	fmt.Printf("idempotency writes=%d errors=%d indexed_events=%d\n", goroutines, errCount, page.Total)
}

func runBatchDemo(ctx context.Context, db *anhe.DB) {
	fmt.Println("== aset/aget demo ==")
	events, err := db.Batch().
		SetWithEventAndI("sdkdemo:batch:from", -10, "SEND", "sdkdemo-batch-001").
		SetWithEventAndI("sdkdemo:batch:to", 10, "RECEIVED", "sdkdemo-batch-001").
		ExecSet(ctx)
	if err != nil {
		log.Fatalf("aset demo: %v", err)
	}
	items, err := db.Batch().
		Get("sdkdemo:batch:from", "sdkdemo:batch:to", "sdkdemo:batch:missing").
		ExecGet(ctx)
	if err != nil {
		log.Fatalf("aget demo: %v", err)
	}
	fmt.Printf("aset events=%d aget items=%d\n", len(events), len(items))
	for _, item := range items {
		if item.Found {
			fmt.Printf("  key=%s found=true value=%s version=%d\n", item.Key, item.Value, item.Version)
		} else {
			fmt.Printf("  key=%s found=false\n", item.Key)
		}
	}
}

func runConcurrentWriteTest(ctx context.Context, db *anhe.DB, workers int) []string {
	fmt.Println("== concurrent write test ==")
	if workers < 1 {
		workers = 1
	}
	keys := make([]string, 0, workers*50)
	for i := 0; i < workers*50; i++ {
		keys = append(keys, fmt.Sprintf("sdkdemo:balance:%06d", i))
	}

	start := time.Now()
	stats := newPhaseStats(start, 32)
	var wg sync.WaitGroup
	okCount := int64(0)
	errCount := int64(0)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := workerID; i < len(keys); i += workers {
				key := keys[i]
				id := fmt.Sprintf("tx-%d-%d", workerID, i)
				opStart := time.Now()
				_, err := db.Key(key).Event("RECEIVED").Idempotency(id).SetDelta(ctx, 10)
				stats.observeLatency(time.Since(opStart))
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					stats.observeError(time.Since(start), err)
					continue
				}
				atomic.AddInt64(&okCount, 1)
				stats.observeSuccess(time.Since(start))
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	if elapsed == 0 {
		elapsed = 1
	}
	fmt.Printf("concurrent writes ok=%d err=%d qps=%.1f\n", okCount, errCount, float64(okCount)/elapsed)
	stats.print("concurrent writes")
	return keys
}

func runHighFrequencyReadTest(ctx context.Context, db *anhe.DB, keys []string, workers int, duration time.Duration) {
	fmt.Println("== high frequency read test ==")
	if workers < 1 {
		workers = 1
	}
	if duration <= 0 {
		duration = 10 * time.Second
	}

	deadline := time.Now().Add(duration)
	start := time.Now()
	stats := newPhaseStats(start, 32)
	var wg sync.WaitGroup
	okCount := int64(0)
	errCount := int64(0)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + seed))
			for time.Now().Before(deadline) {
				key := keys[r.Intn(len(keys))]
				opStart := time.Now()
				_, err := db.Key(key).Get(ctx)
				stats.observeLatency(time.Since(opStart))
				if err != nil {
					atomic.AddInt64(&errCount, 1)
					stats.observeError(time.Since(start), err)
					continue
				}
				atomic.AddInt64(&okCount, 1)
				stats.observeSuccess(time.Since(start))
			}
		}(int64(worker))
	}
	wg.Wait()
	seconds := duration.Seconds()
	if seconds == 0 {
		seconds = 1
	}
	fmt.Printf("high-frequency reads ok=%d err=%d qps=%.1f duration=%s\n", okCount, errCount, float64(okCount)/seconds, duration)
	stats.print("high-frequency reads")
}

type phaseBucket struct {
	ok  int64
	err int64
}

type phaseStats struct {
	start      time.Time
	sampleRate uint64
	counter    uint64
	mu         sync.Mutex
	buckets    map[int64]*phaseBucket
	errKinds   map[string]int64
	latencyUS  []int64
}

func newPhaseStats(start time.Time, sampleRate uint64) *phaseStats {
	if sampleRate == 0 {
		sampleRate = 1
	}
	return &phaseStats{
		start:      start,
		sampleRate: sampleRate,
		buckets:    map[int64]*phaseBucket{},
		errKinds:   map[string]int64{},
		latencyUS:  make([]int64, 0, 2048),
	}
}

func (s *phaseStats) observeLatency(d time.Duration) {
	n := atomic.AddUint64(&s.counter, 1)
	if n%s.sampleRate != 0 {
		return
	}
	s.mu.Lock()
	s.latencyUS = append(s.latencyUS, d.Microseconds())
	s.mu.Unlock()
}

func (s *phaseStats) observeSuccess(sinceStart time.Duration) {
	second := int64(sinceStart / time.Second)
	s.mu.Lock()
	b := s.getBucketLocked(second)
	b.ok++
	s.mu.Unlock()
}

func (s *phaseStats) observeError(sinceStart time.Duration, err error) {
	second := int64(sinceStart / time.Second)
	kind := classifyError(err)
	s.mu.Lock()
	b := s.getBucketLocked(second)
	b.err++
	s.errKinds[kind]++
	s.mu.Unlock()
}

func (s *phaseStats) getBucketLocked(second int64) *phaseBucket {
	b, ok := s.buckets[second]
	if !ok {
		b = &phaseBucket{}
		s.buckets[second] = b
	}
	return b
}

func (s *phaseStats) print(phase string) {
	s.mu.Lock()
	lat := append([]int64(nil), s.latencyUS...)
	buckets := make(map[int64]phaseBucket, len(s.buckets))
	seconds := make([]int64, 0, len(s.buckets))
	for sec, bucket := range s.buckets {
		buckets[sec] = *bucket
		seconds = append(seconds, sec)
	}
	errKinds := make(map[string]int64, len(s.errKinds))
	for k, v := range s.errKinds {
		errKinds[k] = v
	}
	s.mu.Unlock()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p50 := percentileUS(lat, 50)
	p95 := percentileUS(lat, 95)
	p99 := percentileUS(lat, 99)
	fmt.Printf("%s latency(sampled): p50=%s p95=%s p99=%s samples=%d\n", phase, p50, p95, p99, len(lat))

	if len(errKinds) > 0 {
		keys := make([]string, 0, len(errKinds))
		for k := range errKinds {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		fmt.Printf("%s error-categories:", phase)
		for _, k := range keys {
			fmt.Printf(" %s=%d", k, errKinds[k])
		}
		fmt.Println()
	}

	sort.Slice(seconds, func(i, j int) bool { return seconds[i] < seconds[j] })
	fmt.Printf("%s per-second throughput:\n", phase)
	for _, sec := range seconds {
		b := buckets[sec]
		fmt.Printf("  t+%02ds ok=%d err=%d qps=%.1f\n", sec, b.ok, b.err, float64(b.ok))
	}
}

func percentileUS(sorted []int64, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return time.Duration(sorted[0]) * time.Microsecond
	}
	if p >= 100 {
		return time.Duration(sorted[len(sorted)-1]) * time.Microsecond
	}
	index := (len(sorted) - 1) * p / 100
	return time.Duration(sorted[index]) * time.Microsecond
}

func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout"), strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "broken pipe"),
		strings.Contains(msg, "eof"):
		return "network"
	case strings.Contains(msg, "server error (5"):
		return "server_5xx"
	case strings.Contains(msg, "server error (4"), strings.Contains(msg, "bad request"), strings.Contains(msg, "conflict"):
		return "business_4xx"
	default:
		return "other"
	}
}
