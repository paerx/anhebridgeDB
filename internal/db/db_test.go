package db

import (
	"encoding/json"
	"testing"
	"time"
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
