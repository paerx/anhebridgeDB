package db

import (
	"bufio"
	bytespkg "bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/paerx/anhebridgedb/internal/config"
)

func TestTimelineAndDiff(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	first, err := engine.Set("user:1", json.RawMessage(`{"name":"tom","balance":100}`))
	if err != nil {
		t.Fatalf("set value: %v", err)
	}
	if _, err := engine.Set("user:1", json.RawMessage(`{"name":"jack","balance":80}`)); err != nil {
		t.Fatalf("update value: %v", err)
	}

	atFirst, err := engine.GetAt("user:1", first.Timestamp)
	if err != nil {
		t.Fatalf("get at first: %v", err)
	}
	if string(atFirst.Value) != `{"name":"tom","balance":100}` {
		t.Fatalf("unexpected historical value: %s", atFirst.Value)
	}

	timeline, err := engine.Timeline("user:1", true)
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	if len(timeline) != 2 {
		t.Fatalf("unexpected timeline length: %d", len(timeline))
	}
	if timeline[1].Diff == nil {
		t.Fatal("expected diff to be present")
	}
	if timeline[0].AuthTag == "" || timeline[1].AuthTag == "" {
		t.Fatal("expected auth tags in timeline")
	}
}

func TestCustomEventLastAndRollback(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("profile.0xdhwqiasui", json.RawMessage(`{"name":"tom"}`)); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	version2, err := engine.SetWithEventName("profile.0xdhwqiasui", json.RawMessage(`{"name":"jack"}`), "rename")
	if err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if _, err := engine.Set("profile.0xdhwqiasui", json.RawMessage(`{"name":"alice"}`)); err != nil {
		t.Fatalf("set v3: %v", err)
	}

	last, err := engine.GetLast("profile.0xdhwqiasui", 1)
	if err != nil {
		t.Fatalf("get last: %v", err)
	}
	if string(last.Value) != `{"name":"jack"}` {
		t.Fatalf("unexpected last value: %s", last.Value)
	}

	rollback, err := engine.Rollback("profile.0xdhwqiasui", version2.EventID)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rollback.Operation != "ROLLBACK" {
		t.Fatalf("unexpected rollback operation: %s", rollback.Operation)
	}

	current, err := engine.Get("profile.0xdhwqiasui")
	if err != nil {
		t.Fatalf("get current after rollback: %v", err)
	}
	if string(current.Value) != `{"name":"jack"}` {
		t.Fatalf("unexpected rollback value: %s", current.Value)
	}

	timeline, err := engine.Timeline("profile.0xdhwqiasui", false)
	if err != nil {
		t.Fatalf("timeline after rollback: %v", err)
	}
	if timeline[1].EventName != "rename" {
		t.Fatalf("expected custom event name, got %q", timeline[1].EventName)
	}
}

func TestRollbackLast(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("balance:12", json.RawMessage(`100`)); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if _, err := engine.SetWithEventName("balance:12", json.RawMessage(`-10`), "WITHDRAW"); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if _, err := engine.SetWithEventName("balance:12", json.RawMessage(`20`), "TRANSFER"); err != nil {
		t.Fatalf("set v3: %v", err)
	}

	rollback, err := engine.RollbackLast("balance:12")
	if err != nil {
		t.Fatalf("rollback last: %v", err)
	}
	if rollback.Operation != "ROLLBACK" {
		t.Fatalf("unexpected rollback operation: %s", rollback.Operation)
	}

	current, err := engine.Get("balance:12")
	if err != nil {
		t.Fatalf("get current after rollback last: %v", err)
	}
	if string(current.Value) != `90` {
		t.Fatalf("unexpected rollback last value: %s", current.Value)
	}
}

func TestNumericDeltaAndBatchOps(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("balance:718231", json.RawMessage(`998`)); err != nil {
		t.Fatalf("seed numeric value: %v", err)
	}
	if _, err := engine.SetWithEventName("balance:718231", json.RawMessage(`-10.01`), "WITHDRAW"); err != nil {
		t.Fatalf("apply delta: %v", err)
	}

	record, err := engine.Get("balance:718231")
	if err != nil {
		t.Fatalf("get after delta: %v", err)
	}
	if string(record.Value) != `987.99` {
		t.Fatalf("unexpected numeric delta result: %s", record.Value)
	}

	events, err := engine.BatchSet([]BatchSetItem{
		{Key: "balance:1", Value: json.RawMessage(`10`)},
		{Key: "balance:2", Value: json.RawMessage(`20`)},
		{Key: "balance:3", Value: json.RawMessage(`11.11`), EventName: "TRANSFER"},
	})
	if err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("unexpected batch event count: %d", len(events))
	}

	items := engine.BatchGet([]string{"balance:718231", "balance:1", "balance:2", "missing"})
	if len(items) != 4 {
		t.Fatalf("unexpected batch get count: %d", len(items))
	}
	if !items[0].Found || string(items[0].Value) != `987.99` {
		t.Fatalf("unexpected batch get item 0: %+v", items[0])
	}
	if items[3].Found {
		t.Fatalf("expected missing item, got %+v", items[3])
	}
}

func TestIdempotencyKeyDeduplicatesAndConflicts(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	first, err := engine.SetWithIdempotencyKey("balance:1234", json.RawMessage(`-10`), "SEND", "diweuhasads")
	if err != nil {
		t.Fatalf("first idempotent set: %v", err)
	}
	second, err := engine.SetWithIdempotencyKey("balance:1234", json.RawMessage(`-10`), "SEND", "diweuhasads")
	if err != nil {
		t.Fatalf("second idempotent set: %v", err)
	}
	if first.EventID != second.EventID {
		t.Fatalf("expected deduplicated event id, got %d and %d", first.EventID, second.EventID)
	}

	record, err := engine.Get("balance:1234")
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	if string(record.Value) != `-10` {
		t.Fatalf("unexpected deduplicated value: %s", record.Value)
	}

	if _, err := engine.SetWithIdempotencyKey("balance:1234", json.RawMessage(`-20`), "SEND", "diweuhasads"); err == nil {
		t.Fatalf("expected idempotency conflict")
	}
}

func TestSuperValueExpandRawAndReadOnlyProtection(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("uid-hksn10", json.RawMessage(`{"uid":"uid-hksn10","owner":"0xabc","activated":true}`)); err != nil {
		t.Fatalf("seed uid-hksn10: %v", err)
	}
	if _, err := engine.Set("uid-asan10", json.RawMessage(`{"uid":"uid-asan10","owner":"0xdef","activated":false}`)); err != nil {
		t.Fatalf("seed uid-asan10: %v", err)
	}
	if _, err := engine.Set("usersVape", json.RawMessage(`["*uid-hksn10","*uid-asan10"]`)); err != nil {
		t.Fatalf("seed usersVape: %v", err)
	}

	raw, err := engine.GetRaw("usersVape")
	if err != nil {
		t.Fatalf("get raw usersVape: %v", err)
	}
	if string(raw.Value) != `["*uid-hksn10","*uid-asan10"]` {
		t.Fatalf("unexpected raw value: %s", raw.Value)
	}

	expanded, err := engine.Get("usersVape")
	if err != nil {
		t.Fatalf("get expanded usersVape: %v", err)
	}
	var expandedArr []map[string]any
	if err := json.Unmarshal(expanded.Value, &expandedArr); err != nil {
		t.Fatalf("decode expanded value: %v", err)
	}
	if len(expandedArr) != 2 {
		t.Fatalf("unexpected expanded refs len: %d", len(expandedArr))
	}
	if expandedArr[0]["key"] != "uid-hksn10" || expandedArr[1]["key"] != "uid-asan10" {
		t.Fatalf("unexpected expanded refs: %+v", expandedArr)
	}

	if _, err := engine.SetWithEventName("usersVape", json.RawMessage(`-1`), "WITHDRAW"); !errors.Is(err, ErrSuperValueReadOnly) {
		t.Fatalf("expected super value read-only error, got: %v", err)
	}
}

func TestSuperValueCycleDetection(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("A", json.RawMessage(`"*B"`)); err != nil {
		t.Fatalf("seed A: %v", err)
	}
	if _, err := engine.Set("B", json.RawMessage(`"*A"`)); err != nil {
		t.Fatalf("seed B: %v", err)
	}

	if _, err := engine.Get("A"); !errors.Is(err, ErrSuperValueCycleDetected) {
		t.Fatalf("expected cycle error, got: %v", err)
	}
}

func TestSearchEventsUsesSecondaryIndexesAndPaging(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	for i := 0; i < 5; i++ {
		key := "balance:1234"
		if i%2 == 1 {
			key = "balance:5678"
		}
		if _, err := engine.SetWithIdempotencyKey(key, json.RawMessage(strconv.Itoa(i+1)), "RECEIVED", fmt.Sprintf("idem-%d", i)); err != nil {
			t.Fatalf("seed event %d: %v", i, err)
		}
	}

	page1, err := engine.SearchEvents(SearchOptions{
		Key:       "balance:1234",
		EventName: "RECEIVED",
		Desc:      true,
		Limit:     2,
		Page:      1,
	})
	if err != nil {
		t.Fatalf("search page1: %v", err)
	}
	if page1.Total != 3 || len(page1.Results) != 2 || !page1.HasMore {
		t.Fatalf("unexpected page1: %+v", page1)
	}
	if page1.Results[0].Key != "balance:1234" || page1.Results[0].EventName != "RECEIVED" {
		t.Fatalf("unexpected page1 first result: %+v", page1.Results[0])
	}

	page2, err := engine.SearchEvents(SearchOptions{
		Key:       "balance:1234",
		EventName: "RECEIVED",
		Desc:      true,
		Limit:     2,
		Page:      2,
	})
	if err != nil {
		t.Fatalf("search page2: %v", err)
	}
	if len(page2.Results) != 1 || page2.HasMore {
		t.Fatalf("unexpected page2: %+v", page2)
	}

	idemPage, err := engine.SearchEvents(SearchOptions{IdempotencyKey: "idem-3", Limit: 10, Page: 1})
	if err != nil {
		t.Fatalf("search by idempotency key: %v", err)
	}
	if idemPage.Total != 1 || len(idemPage.Results) != 1 {
		t.Fatalf("unexpected idempotency page: %+v", idemPage)
	}
	if idemPage.Results[0].IdempotencyKey != "idem-3" {
		t.Fatalf("unexpected idempotency result: %+v", idemPage.Results[0])
	}
}

func TestSearchEventsWithSameIChain(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.SetWithIdempotencyKey("balance:1234", json.RawMessage(`-10`), "SEND", "abcd"); err != nil {
		t.Fatalf("seed send: %v", err)
	}
	if _, err := engine.SetWithIdempotencyKey("balance:5678", json.RawMessage(`10`), "RECEIVED", "abcd"); err != nil {
		t.Fatalf("seed received: %v", err)
	}
	if _, err := engine.SetWithIdempotencyKey("balance:9999", json.RawMessage(`1`), "RECEIVED", "other"); err != nil {
		t.Fatalf("seed unrelated: %v", err)
	}

	page, err := engine.SearchEvents(SearchOptions{
		Key:       "balance:1234",
		EventName: "SEND",
		WithSameI: true,
		Desc:      true,
		Limit:     10,
		Page:      1,
	})
	if err != nil {
		t.Fatalf("search with same i: %v", err)
	}
	if page.Total != 2 || len(page.Results) != 2 {
		t.Fatalf("unexpected same i page: %+v", page)
	}
	keys := []string{page.Results[0].Key, page.Results[1].Key}
	sort.Strings(keys)
	if keys[0] != "balance:1234" || keys[1] != "balance:5678" {
		t.Fatalf("unexpected linked keys: %+v", keys)
	}
}

func TestRuleSchedulerAndRecovery(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	if _, err := engine.CreateRule(RuleSpec{
		ID:      "auto_timeout_order",
		Pattern: "OrderStatus:*:wait_for_paid",
		Delay:   "1s",
		Target:  "OrderStatus:{id}:timeout",
	}); err != nil {
		t.Fatalf("create rule: %v", err)
	}

	event, err := engine.Set("OrderStatus:123", json.RawMessage(`{"state":"wait_for_paid","amount":100}`))
	if err != nil {
		t.Fatalf("set order: %v", err)
	}

	if pending := len(engine.ListTasks()); pending != 1 {
		t.Fatalf("expected 1 task, got %d", pending)
	}

	stats, err := engine.ProcessDueTasks(event.Timestamp.Add(2 * time.Second))
	if err != nil {
		t.Fatalf("process due tasks: %v", err)
	}
	if stats.Executed != 1 {
		t.Fatalf("expected 1 executed task, got %d", stats.Executed)
	}

	record, err := engine.Get("OrderStatus:123")
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if extractState(record.Value) != "timeout" {
		t.Fatalf("expected timeout state, got %s", record.Value)
	}

	if _, err := engine.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	recovered, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer recovered.Close()

	recoveredRecord, err := recovered.Get("OrderStatus:123")
	if err != nil {
		t.Fatalf("get recovered order: %v", err)
	}
	if extractState(recoveredRecord.Value) != "timeout" {
		t.Fatalf("expected timeout after recovery, got %s", recoveredRecord.Value)
	}

	rule, err := recovered.GetRule("auto_timeout_order")
	if err != nil {
		t.Fatalf("get recovered rule: %v", err)
	}
	if rule.ExecuteCount != 1 {
		t.Fatalf("expected execute_count 1, got %d", rule.ExecuteCount)
	}
}

func TestRuleSchedulerSupportsCustomField(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.CreateRule(RuleSpec{
		ID:      "auto_analysis_voucher",
		Field:   "status",
		Pattern: "campaign_voucher:*:OPENED",
		Delay:   "1s",
		ToValue: "In AI analysis",
	}); err != nil {
		t.Fatalf("create rule: %v", err)
	}

	event, err := engine.Set("campaign_voucher:1", json.RawMessage(`{"status":"OPENED","reward_type":"BTC"}`))
	if err != nil {
		t.Fatalf("set voucher: %v", err)
	}

	stats, err := engine.ProcessDueTasks(event.Timestamp.Add(2 * time.Second))
	if err != nil {
		t.Fatalf("process due tasks: %v", err)
	}
	if stats.Executed != 1 {
		t.Fatalf("expected 1 executed task, got %d", stats.Executed)
	}

	record, err := engine.Get("campaign_voucher:1")
	if err != nil {
		t.Fatalf("get voucher: %v", err)
	}
	if got := extractFieldString(record.Value, "status"); got != "In AI analysis" {
		t.Fatalf("expected status to become In AI analysis, got %q", got)
	}
}

func TestCheckAllTimeDetectsTamper(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	if _, err := engine.Set("balance:123", json.RawMessage(`100`)); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if _, err := engine.SetWithEventName("balance:123", json.RawMessage(`-10`), "WITHDRAW"); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	segmentPath := filepath.Join(dir, "log", "segment_000001.anhe")
	fileBytes, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	scanner := bufio.NewScanner(bytespkg.NewReader(fileBytes))
	var lines []string
	mutatedIndex := -1
	for scanner.Scan() {
		line := scanner.Bytes()
		var env struct {
			CRC32 uint32          `json:"crc32"`
			Data  json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(line, &env); err != nil {
			t.Fatalf("decode envelope: %v", err)
		}
		text := strings.Replace(string(env.Data), `"new_value":90`, `"new_value":91`, 1)
		encoded, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("encode envelope: %v", err)
		}
		lines = append(lines, string(encoded))
		if text != string(env.Data) {
			mutatedIndex = len(lines) - 1
		}
	}
	if mutatedIndex < 0 {
		t.Fatal("expected to mutate segment payload")
	}
	var env struct {
		CRC32 uint32          `json:"crc32"`
		Data  json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal([]byte(lines[mutatedIndex]), &env); err != nil {
		t.Fatalf("decode target envelope: %v", err)
	}
	env.Data = json.RawMessage(strings.Replace(string(env.Data), `"new_value":90`, `"new_value":91`, 1))
	env.CRC32 = crc32.ChecksumIEEE(env.Data)
	encoded, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("encode target envelope: %v", err)
	}
	lines[mutatedIndex] = string(encoded)
	if err := os.WriteFile(segmentPath, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write mutated segment: %v", err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	report, err := reopened.CheckAllTime("balance:123")
	if err != nil {
		t.Fatalf("check alltime: %v", err)
	}
	if report.OK {
		t.Fatalf("expected tamper report, got ok")
	}
	if len(report.Issues) == 0 {
		t.Fatalf("expected integrity issues")
	}
}

func TestVerifyAndCompactStorage(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("user:1", json.RawMessage(`{"name":"tom"}`)); err != nil {
		t.Fatalf("set user:1: %v", err)
	}
	if _, err := engine.Set("user:2", json.RawMessage(`{"name":"jack"}`)); err != nil {
		t.Fatalf("set user:2: %v", err)
	}

	verify, err := engine.VerifyStorage()
	if err != nil {
		t.Fatalf("verify storage: %v", err)
	}
	if !verify.OK {
		t.Fatalf("expected verify ok, got issues: %+v", verify.Issues)
	}

	report, err := engine.CompactStorage()
	if err != nil {
		t.Fatalf("compact storage: %v", err)
	}
	if !report.KeyIndexCompacted {
		t.Fatalf("expected key index compacted")
	}
}

func TestCompactStorageArchivesColdSegments(t *testing.T) {
	dir := t.TempDir()
	engine, err := OpenWithConfig(dir, config.SegmentConfig{MaxRecords: 1}, config.Default().Performance, false)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("user:1", json.RawMessage(`{"name":"tom"}`)); err != nil {
		t.Fatalf("set user:1: %v", err)
	}
	if _, err := engine.Set("user:2", json.RawMessage(`{"name":"jack"}`)); err != nil {
		t.Fatalf("set user:2: %v", err)
	}
	if _, err := engine.Set("user:3", json.RawMessage(`{"name":"lucy"}`)); err != nil {
		t.Fatalf("set user:3: %v", err)
	}
	if _, err := engine.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	report, err := engine.CompactStorage()
	if err != nil {
		t.Fatalf("compact storage: %v", err)
	}
	if report.ArchivedSegments < 1 {
		t.Fatalf("expected archived segments, got %d", report.ArchivedSegments)
	}
	if report.CompactedSegments < 1 {
		t.Fatalf("expected compacted archive segments, got %d", report.CompactedSegments)
	}
	if _, err := os.Stat(filepath.Join(dir, "log", "archive", "archive.manifest.json")); err != nil {
		t.Fatalf("expected archive manifest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "log", "archive", "segment_900001.anhe")); err != nil {
		t.Fatalf("expected compacted archive segment: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "log", "archive", "segment_000001.anhe")); !os.IsNotExist(err) {
		t.Fatalf("expected old archived segment removed after compaction")
	}
	record, err := engine.Get("user:1")
	if err != nil {
		t.Fatalf("get after archive: %v", err)
	}
	if string(record.Value) != `{"name":"tom"}` {
		t.Fatalf("unexpected archived read value: %s", record.Value)
	}
}

func TestCompactStorageRewritesPositionIndexToCompactedArchive(t *testing.T) {
	dir := t.TempDir()
	engine, err := OpenWithConfig(dir, config.SegmentConfig{MaxRecords: 1}, config.Default().Performance, false)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := engine.Set("balance:1", json.RawMessage(`1`)); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if _, err := engine.Set("balance:1", json.RawMessage(`2`)); err != nil {
		t.Fatalf("set v2: %v", err)
	}
	if _, err := engine.Set("balance:1", json.RawMessage(`3`)); err != nil {
		t.Fatalf("set v3: %v", err)
	}
	if _, err := engine.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	report, err := engine.CompactStorage()
	if err != nil {
		t.Fatalf("compact storage: %v", err)
	}
	if report.CompactedSegments < 1 {
		t.Fatalf("expected compacted archive segments")
	}

	ref, ok := engine.loadEventRef(1)
	if !ok {
		t.Fatalf("missing ref for event 1")
	}
	if ref.Segment != "segment_900001.anhe" {
		t.Fatalf("expected compacted segment ref, got %s", ref.Segment)
	}
	record, err := engine.GetLast("balance:1", 2)
	if err != nil {
		t.Fatalf("get historical value from compacted archive: %v", err)
	}
	if string(record.Value) != "1" {
		t.Fatalf("unexpected historical value: %s", record.Value)
	}
}
