package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/paerx/anhebridgedb/sdk/anhe"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dslRequest struct {
	Query string `json:"query"`
}

var (
	stressDB        *anhe.DB
	stressReqTimout time.Duration
)

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
		mode           = flag.String("mode", "mixed", "rw|orders|mixed|transfer")
		transport      = flag.String("transport", "ws", "dsl transport: ws|http")
		token          = flag.String("token", "", "auth token for ws/http")
		username       = flag.String("username", "", "username for login when token is empty")
		password       = flag.String("password", "", "password for login when token is empty")
		wsPool         = flag.Int("ws-pool", 8, "websocket connection pool size for ws transport")
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
		mainKey        = flag.String("main-key", "mainAccount", "main account key for transfer mode")
		userPrefix     = flag.String("user-prefix", "userAccount", "user account key prefix for transfer mode")
		userCount      = flag.Int("users", 5000, "number of user accounts for transfer mode")
		initialMain    = flag.Float64("initial-main", 1000000, "initial main account balance in transfer mode")
		txAmount       = flag.Float64("tx-amount", 1, "amount per transfer tx in transfer mode")
		traceSamples   = flag.Int("trace-samples", 20, "sample idempotency keys to trace by SEARCH EVENTS")
		skipSeed       = flag.Bool("skip-seed", false, "skip seeding main/user accounts in transfer mode")
	)
	flag.Parse()
	if *duration <= 0 {
		*duration = 60 * time.Second
	}
	stressReqTimout = *requestTimeout

	client := &http.Client{Timeout: *requestTimeout}
	base := strings.TrimRight(*addr, "/")
	rng := rand.New(rand.NewSource(*seed))

	if strings.EqualFold(*transport, "ws") {
		db, err := anhe.Open(anhe.Config{
			Addr:       base,
			Token:      strings.TrimSpace(*token),
			Username:   strings.TrimSpace(*username),
			Password:   *password,
			Timeout:    *requestTimeout,
			WSPoolSize: *wsPool,
		})
		if err != nil {
			log.Fatalf("open ws transport failed: %v", err)
		}
		stressDB = db
		defer func() {
			_ = stressDB.Close()
		}()
		log.Printf("dsl transport: ws/wss pool=%d addr=%s", *wsPool, base)
	} else {
		log.Printf("dsl transport: http addr=%s", base)
	}

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
	case "transfer":
		if err := runTransfers(client, base, *duration, *workers, *batch, *userCount, *mainKey, *userPrefix, *initialMain, *txAmount, *skipSeed, rng, *progressEvery, *traceSamples); err != nil {
			log.Fatal(err)
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

func runTransfers(client *http.Client, base string, duration time.Duration, workers, batch, users int, mainKey, userPrefix string, initialMain, amount float64, skipSeed bool, rng *rand.Rand, progress time.Duration, traceSamples int) error {
	if workers <= 0 || batch <= 0 || users <= 0 {
		return fmt.Errorf("invalid transfer params: workers=%d batch=%d users=%d", workers, batch, users)
	}
	if amount <= 0 {
		return fmt.Errorf("tx-amount must be > 0")
	}
	if !skipSeed {
		if err := seedTransferAccounts(client, base, mainKey, userPrefix, users, initialMain); err != nil {
			return err
		}
	}
	baseMain, baseUserTotal, _, err := verifyTransferLedger(client, base, mainKey, userPrefix, users)
	if err != nil {
		return fmt.Errorf("read transfer baseline failed: %w", err)
	}
	baseTotal := baseMain + baseUserTotal
	log.Printf("transfer baseline: main=%.4f users_total=%.4f total=%.4f", baseMain, baseUserTotal, baseTotal)
	transferCtx, cancelTransfer := context.WithTimeout(context.Background(), duration)
	defer cancelTransfer()
	runID := fmt.Sprintf("run%d", time.Now().UTC().UnixNano())

	log.Printf("transfer stress: workers=%d batch=%d users=%d amount=%.4f main_key=%s user_prefix=%s run_id=%s", workers, batch, users, amount, mainKey, userPrefix, runID)
	m := newMeter()
	progressCtx, cancelProgress := context.WithCancel(context.Background())
	defer cancelProgress()
	go printProgress(progressCtx, "transfer", m, progress)

	var txSeq atomic.Uint64
	var outCount atomic.Uint64
	var inCount atomic.Uint64
	seenMainEvent := map[uint64]struct{}{}
	var seenMu sync.Mutex
	samples := make([]string, 0, traceSamples)
	var sampleMu sync.Mutex

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		seed := rng.Int63()
		workerID := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-transferCtx.Done():
					return
				default:
				}

				query, ids := buildTransferASET(batch, users, mainKey, userPrefix, amount, runID, workerID, &txSeq, local)
				results, err := execDSL(client, base, query)
				if err != nil {
					m.addFail()
					continue
				}
				mainOut, mainIn := summarizeMainDirection(results, mainKey, seenMainEvent, &seenMu)
				outCount.Add(uint64(mainOut))
				inCount.Add(uint64(mainIn))
				m.addOK(len(query))

				if traceSamples > 0 {
					sampleMu.Lock()
					if len(samples) < traceSamples {
						need := traceSamples - len(samples)
						if need > len(ids) {
							need = len(ids)
						}
						samples = append(samples, ids[:need]...)
					}
					sampleMu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	cancelProgress()
	log.Printf("transfer done: %s tx_out=%d tx_in=%d", m.snapshot(), outCount.Load(), inCount.Load())

	mainBalance, userTotal, missingUsers, err := verifyTransferLedger(client, base, mainKey, userPrefix, users)
	if err != nil {
		return err
	}
	expectedMain := baseMain - float64(outCount.Load())*amount + float64(inCount.Load())*amount
	expectedTotal := baseTotal
	actualTotal := mainBalance + userTotal
	log.Printf("ledger verify: expected_main=%.4f actual_main=%.4f delta=%.4f", expectedMain, mainBalance, mainBalance-expectedMain)
	log.Printf("ledger verify: expected_total=%.4f actual_total=%.4f delta=%.4f user_total=%.4f missing_users=%d", expectedTotal, actualTotal, actualTotal-expectedTotal, userTotal, missingUsers)

	sampleMu.Lock()
	traceIDs := append([]string(nil), samples...)
	sampleMu.Unlock()
	if len(traceIDs) > 0 {
		missing, err := verifyTraceability(client, base, traceIDs)
		if err != nil {
			return err
		}
		log.Printf("trace verify: sampled=%d missing=%d", len(traceIDs), missing)
	}
	return nil
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

func seedTransferAccounts(client *http.Client, base, mainKey, userPrefix string, users int, initialMain float64) error {
	query := fmt.Sprintf("SET %s %.4f INIT_MAIN;", mainKey, initialMain)
	if err := postDSL(client, base, query); err != nil {
		return fmt.Errorf("seed main account: %w", err)
	}
	const chunk = 400
	for start := 1; start <= users; start += chunk {
		end := start + chunk - 1
		if end > users {
			end = users
		}
		var b strings.Builder
		b.WriteString("ASET ")
		for i := start; i <= end; i++ {
			if i > start {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s:%d 0 INIT_USER", userPrefix, i)
		}
		b.WriteString(";")
		if err := postDSL(client, base, b.String()); err != nil {
			return fmt.Errorf("seed user accounts [%d,%d]: %w", start, end, err)
		}
	}
	return nil
}

func postDSL(client *http.Client, base, query string) error {
	if stressDB != nil {
		ctx, cancel := context.WithTimeout(context.Background(), stressReqTimout)
		defer cancel()
		_, err := stressDB.ExecDSL(ctx, query)
		return err
	}
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

func execDSL(client *http.Client, base, query string) ([]json.RawMessage, error) {
	if stressDB != nil {
		ctx, cancel := context.WithTimeout(context.Background(), stressReqTimout)
		defer cancel()
		results, err := stressDB.ExecDSL(ctx, query)
		if err != nil {
			return nil, err
		}
		out := make([]json.RawMessage, 0, len(results))
		for _, item := range results {
			b, err := json.Marshal(item)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
		}
		return out, nil
	}
	payload, err := json.Marshal(dslRequest{Query: query})
	if err != nil {
		return nil, err
	}
	resp, err := client.Post(base+"/dsl", "application/json", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("dsl failed: %s %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var out struct {
		Results []json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Results, nil
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

func buildTransferASET(batch, users int, mainKey, userPrefix string, amount float64, runID string, workerID int, seq *atomic.Uint64, rng *rand.Rand) (string, []string) {
	var b strings.Builder
	b.WriteString("ASET ")
	ids := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		userID := rng.Intn(users) + 1
		txID := fmt.Sprintf("tx:%s:%d:%d", runID, workerID, seq.Add(1))
		userKey := fmt.Sprintf("%s:%d", userPrefix, userID)
		mainToUser := rng.Intn(2) == 0
		if mainToUser {
			// main sends out, user receives
			fmt.Fprintf(&b, "%s -%.4f OUT i=%s, %s %.4f OUT i=%s", mainKey, amount, txID, userKey, amount, txID)
		} else {
			// user sends back to main
			fmt.Fprintf(&b, "%s -%.4f INPUT i=%s, %s %.4f INPUT i=%s", userKey, amount, txID, mainKey, amount, txID)
		}
		ids = append(ids, txID)
	}
	b.WriteString(";")
	return b.String(), ids
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

func verifyTransferLedger(client *http.Client, base, mainKey, userPrefix string, users int) (mainBalance float64, userTotal float64, missingUsers int, err error) {
	mainBalance, found, err := fetchNumericKey(client, base, mainKey)
	if err != nil {
		return 0, 0, 0, err
	}
	if !found {
		return 0, 0, 0, fmt.Errorf("main account not found: %s", mainKey)
	}

	const chunk = 400
	for start := 1; start <= users; start += chunk {
		end := start + chunk - 1
		if end > users {
			end = users
		}
		var q strings.Builder
		q.WriteString("AGET ")
		for i := start; i <= end; i++ {
			if i > start {
				q.WriteByte(' ')
			}
			fmt.Fprintf(&q, "%s:%d", userPrefix, i)
		}
		q.WriteString(";")
		results, err := execDSL(client, base, q.String())
		if err != nil {
			return 0, 0, 0, err
		}
		if len(results) == 0 {
			continue
		}
		var items []map[string]any
		if err := json.Unmarshal(results[0], &items); err != nil {
			return 0, 0, 0, err
		}
		for _, item := range items {
			found, _ := item["found"].(bool)
			if !found {
				missingUsers++
				continue
			}
			v, ok := toFloat64(item["value"])
			if !ok {
				continue
			}
			userTotal += v
		}
	}
	return mainBalance, userTotal, missingUsers, nil
}

func verifyTraceability(client *http.Client, base string, ids []string) (missing int, err error) {
	for _, id := range ids {
		results, err := execDSL(client, base, fmt.Sprintf("SEARCH EVENTS I:%s LIMIT:1;", id))
		if err != nil {
			return 0, err
		}
		if len(results) == 0 {
			missing++
			continue
		}
		var page struct {
			Total int `json:"total"`
		}
		if err := json.Unmarshal(results[0], &page); err != nil {
			return 0, err
		}
		if page.Total == 0 {
			missing++
		}
	}
	return missing, nil
}

func fetchNumericKey(client *http.Client, base, key string) (float64, bool, error) {
	results, err := execDSL(client, base, fmt.Sprintf("GET %s;", key))
	if err != nil {
		return 0, false, err
	}
	if len(results) == 0 {
		return 0, false, nil
	}
	var row map[string]any
	if err := json.Unmarshal(results[0], &row); err != nil {
		// Backward/compat shape: array of records.
		var rows []map[string]any
		if err2 := json.Unmarshal(results[0], &rows); err2 != nil {
			return 0, false, err
		}
		if len(rows) == 0 {
			return 0, false, nil
		}
		row = rows[0]
	}
	v, ok := toFloat64(row["value"])
	if !ok {
		return 0, false, nil
	}
	return v, true, nil
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func summarizeMainDirection(results []json.RawMessage, mainKey string, seen map[uint64]struct{}, mu *sync.Mutex) (outCount, inCount int) {
	if len(results) == 0 {
		return 0, 0
	}
	var events []map[string]any
	if err := json.Unmarshal(results[0], &events); err != nil {
		return 0, 0
	}
	mu.Lock()
	defer mu.Unlock()
	for _, event := range events {
		key, _ := event["key"].(string)
		if key != mainKey {
			continue
		}
		eventID, ok := toUint64(event["event_id"])
		if !ok {
			continue
		}
		if _, exists := seen[eventID]; exists {
			continue
		}
		seen[eventID] = struct{}{}
		name, _ := event["event_name"].(string)
		switch strings.ToUpper(strings.TrimSpace(name)) {
		case "OUT":
			outCount++
		case "INPUT":
			inCount++
		}
	}
	return outCount, inCount
}

func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case uint32:
		return uint64(n), true
	case uint:
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case float64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case json.Number:
		u, err := strconv.ParseUint(string(n), 10, 64)
		return u, err == nil
	default:
		return 0, false
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) == 1 {
		_ = os.Stderr.Sync()
	}
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
