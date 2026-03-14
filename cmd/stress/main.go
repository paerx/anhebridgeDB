package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dslRequest struct {
	Query string `json:"query"`
}

type kvResponse struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   uint64          `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type ruleSpec struct {
	ID      string `json:"id"`
	Pattern string `json:"pattern"`
	Delay   string `json:"delay"`
	Target  string `json:"target"`
}

type meter struct {
	ok    atomic.Uint64
	fail  atomic.Uint64
	bytes atomic.Uint64
	start time.Time
}

func newMeter() *meter {
	return &meter{start: time.Now()}
}

func (m *meter) addOK(n int) {
	m.ok.Add(1)
	m.bytes.Add(uint64(n))
}

func (m *meter) addFail() {
	m.fail.Add(1)
}

func (m *meter) snapshot() string {
	elapsed := time.Since(m.start).Seconds()
	if elapsed == 0 {
		elapsed = 1
	}
	ok := m.ok.Load()
	fail := m.fail.Load()
	bytes := m.bytes.Load()
	return fmt.Sprintf("ok=%d fail=%d ops/s=%.1f throughput=%.1f KB/s", ok, fail, float64(ok)/elapsed, float64(bytes)/1024.0/elapsed)
}

func main() {
	var (
		addr           = flag.String("addr", "http://127.0.0.1:8080", "server base url")
		mode           = flag.String("mode", "mixed", "rw|orders|mixed")
		duration       = flag.Duration("duration", 60*time.Second, "stress duration")
		workers        = flag.Int("workers", 16, "concurrent workers")
		batch          = flag.Int("batch", 64, "batch size for ASET/AGET")
		keys           = flag.Int("keys", 20000, "logical key space for rw mode")
		orderCount     = flag.Int("orders", 20000, "total orders to create")
		orderBatch     = flag.Int("order-batch", 200, "orders per DSL batch")
		ruleDelay      = flag.Duration("rule-delay", 15*time.Second, "auto transition delay for stress orders")
		orderPrefix    = flag.String("order-prefix", "OrderStatus:stress", "order key prefix")
		readRatio      = flag.Int("read-ratio", 70, "read ratio in percent for rw mode")
		requestTimeout = flag.Duration("timeout", 20*time.Second, "http timeout")
		progressEvery  = flag.Duration("progress", 5*time.Second, "progress print interval")
		seed           = flag.Int64("seed", time.Now().UnixNano(), "random seed")
	)
	flag.Parse()

	client := &http.Client{Timeout: *requestTimeout}
	base := strings.TrimRight(*addr, "/")
	rng := rand.New(rand.NewSource(*seed))

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	switch *mode {
	case "rw":
		if err := runRW(ctx, client, base, *workers, *batch, *keys, *readRatio, rng, *progressEvery); err != nil {
			log.Fatal(err)
		}
	case "orders":
		if err := runOrders(ctx, client, base, *orderCount, *orderBatch, *workers, *orderPrefix, *ruleDelay, *progressEvery); err != nil {
			log.Fatal(err)
		}
	case "mixed":
		if err := ensureStressRule(client, base, *ruleDelay); err != nil {
			log.Fatal(err)
		}
		var wg sync.WaitGroup
		var rwErr, orderErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			rwErr = runRW(ctx, client, base, *workers, *batch, *keys, *readRatio, rng, *progressEvery)
		}()
		go func() {
			defer wg.Done()
			orderErr = runOrders(ctx, client, base, *orderCount, *orderBatch, *workers/2+1, *orderPrefix, *ruleDelay, *progressEvery)
		}()
		wg.Wait()
		if rwErr != nil {
			log.Fatal(rwErr)
		}
		if orderErr != nil {
			log.Fatal(orderErr)
		}
	default:
		log.Fatalf("unsupported mode: %s", *mode)
	}
}

func runRW(ctx context.Context, client *http.Client, base string, workers, batch, keys, readRatio int, rng *rand.Rand, progress time.Duration) error {
	log.Printf("rw stress: workers=%d batch=%d keys=%d read_ratio=%d%%", workers, batch, keys, readRatio)
	m := newMeter()

	progressCtx, cancelProgress := context.WithCancel(context.Background())
	defer cancelProgress()
	go printProgress(progressCtx, "rw", m, progress)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerSeed := rng.Int63()
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := rand.New(rand.NewSource(workerSeed))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if local.Intn(100) < readRatio {
					query := buildAGET(batch, keys, local)
					if err := postDSL(client, base, query); err != nil {
						m.addFail()
						continue
					}
					m.addOK(len(query))
				} else {
					query := buildASET(batch, keys, local)
					if err := postDSL(client, base, query); err != nil {
						m.addFail()
						continue
					}
					m.addOK(len(query))
				}
			}
		}()
	}

	wg.Wait()
	cancelProgress()
	log.Printf("rw done: %s", m.snapshot())
	return nil
}

func runOrders(ctx context.Context, client *http.Client, base string, orders, batch, workers int, prefix string, delay time.Duration, progress time.Duration) error {
	if err := ensureStressRule(client, base, delay); err != nil {
		return err
	}
	log.Printf("order stress: orders=%d batch=%d workers=%d delay=%s", orders, batch, workers, delay)

	created := newMeter()
	progressCtx, cancelProgress := context.WithCancel(context.Background())
	defer cancelProgress()
	go printProgress(progressCtx, "orders-create", created, progress)

	jobs := make(chan [2]int, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				query := buildOrderBatch(prefix, job[0], job[1])
				if err := postDSL(client, base, query); err != nil {
					created.addFail()
					continue
				}
				created.addOK(len(query))
			}
		}()
	}

	for start := 0; start < orders; start += batch {
		end := start + batch
		if end > orders {
			end = orders
		}
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return ctx.Err()
		case jobs <- [2]int{start, end}:
		}
	}
	close(jobs)
	wg.Wait()
	cancelProgress()
	log.Printf("orders created: %s", created.snapshot())

	verifyCtx, cancelVerify := context.WithTimeout(context.Background(), delay+2*time.Minute)
	defer cancelVerify()
	return waitForTransitions(verifyCtx, client, base, prefix, orders, progress)
}

func waitForTransitions(ctx context.Context, client *http.Client, base, prefix string, total int, progress time.Duration) error {
	ticker := time.NewTicker(progress)
	defer ticker.Stop()
	log.Printf("waiting for auto transitions...")

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("transition wait timeout; context ended")
		case <-ticker.C:
			_ = triggerTasks(client, base)
			done, err := countTimeoutOrders(client, base, prefix, total)
			if err != nil {
				log.Printf("count timeout orders: %v", err)
				continue
			}
			log.Printf("orders transitioned: %d/%d", done, total)
			if done >= total {
				log.Printf("all orders transitioned")
				return nil
			}
		}
	}
}

func ensureStressRule(client *http.Client, base string, delay time.Duration) error {
	body, err := json.Marshal(ruleSpec{
		ID:      "auto_timeout_order_stress",
		Pattern: "OrderStatus:*:wait_for_paid",
		Delay:   delay.String(),
		Target:  "OrderStatus:{id}:timeout",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/rules", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusOK {
		return nil
	}
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "already") || strings.Contains(string(raw), "invalid") {
		return nil
	}
	return fmt.Errorf("ensure rule failed: %s %s", resp.Status, strings.TrimSpace(string(raw)))
}

func postDSL(client *http.Client, base, query string) error {
	payload, err := json.Marshal(dslRequest{Query: query})
	if err != nil {
		return err
	}
	resp, err := client.Post(base+"/dsl", "application/json", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("dsl failed: %s %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func triggerTasks(client *http.Client, base string) error {
	req, err := http.NewRequest(http.MethodPost, base+"/tasks", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func countTimeoutOrders(client *http.Client, base, prefix string, total int) (int, error) {
	done := 0
	for i := 0; i < total; i += 200 {
		end := i + 200
		if end > total {
			end = total
		}
		query := buildOrderProbe(prefix, i, end)
		payload, err := json.Marshal(dslRequest{Query: query})
		if err != nil {
			return 0, err
		}
		resp, err := client.Post(base+"/dsl", "application/json", bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		raw, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return 0, err
		}
		if resp.StatusCode >= 400 {
			return 0, fmt.Errorf("aget failed: %s %s", resp.Status, strings.TrimSpace(string(raw)))
		}

		var wrapper struct {
			Results []json.RawMessage `json:"results"`
		}
		if err := json.Unmarshal(raw, &wrapper); err != nil {
			return 0, err
		}
		if len(wrapper.Results) == 0 {
			continue
		}
		var items []map[string]any
		if err := json.Unmarshal(wrapper.Results[0], &items); err != nil {
			return 0, err
		}
		for _, item := range items {
			if !item["found"].(bool) {
				continue
			}
			value, ok := item["value"].(map[string]any)
			if !ok {
				continue
			}
			if state, _ := value["state"].(string); state == "timeout" {
				done++
			}
		}
	}
	return done, nil
}

func buildASET(batch, keys int, rng *rand.Rand) string {
	var b strings.Builder
	b.WriteString("ASET ")
	for i := 0; i < batch; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		keyID := rng.Intn(keys)
		delta := (rng.Float64() * 20) - 10
		event := "TRANSFER"
		if delta < 0 {
			event = "WITHDRAW"
		}
		fmt.Fprintf(&b, "balance:%d %.4f %s", keyID, delta, event)
	}
	b.WriteString(";")
	return b.String()
}

func buildAGET(batch, keys int, rng *rand.Rand) string {
	var b strings.Builder
	b.WriteString("AGET ")
	for i := 0; i < batch; i++ {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "balance:%d", rng.Intn(keys))
	}
	b.WriteString(";")
	return b.String()
}

func buildOrderBatch(prefix string, start, end int) string {
	var b strings.Builder
	b.WriteString("ASET ")
	for i := start; i < end; i++ {
		if i > start {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s:%d {\"state\":\"wait_for_paid\",\"amount\":%d,\"created_by\":\"stress\"}", prefix, i, 100+i%500)
	}
	b.WriteString(";")
	return b.String()
}

func buildOrderProbe(prefix string, start, end int) string {
	var b strings.Builder
	b.WriteString("AGET ")
	for i := start; i < end; i++ {
		if i > start {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s:%d", prefix, i)
	}
	b.WriteString(";")
	return b.String()
}

func printProgress(ctx context.Context, label string, m *meter, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Printf("%s: %s", label, m.snapshot())
		}
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) == 1 {
		_ = os.Stderr.Sync()
	}
}
