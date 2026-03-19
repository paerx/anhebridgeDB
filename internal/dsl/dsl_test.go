package dsl

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/paerx/anhebridgedb/internal/db"
)

func TestExecuteEndToEnd(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	if _, err := New(engine).Execute(`
		SET user:1 {"name":"tom","state":"active"};
		GET user:1;
	`); err != nil {
		t.Fatalf("execute basic dsl: %v", err)
	}

	if _, err := New(engine).Execute(`
		CREATE RULE auto_timeout_order
		ON PATTERN "OrderStatus:*:wait_for_paid"
		IF UNCHANGED FOR 1s
		THEN TRANSITION TO "OrderStatus:{id}:timeout";
		SET OrderStatus:9 {"state":"wait_for_paid"};
	`); err != nil {
		t.Fatalf("execute rule dsl: %v", err)
	}

	if _, err := engine.ProcessDueTasks(time.Now().UTC().Add(2 * time.Second)); err != nil {
		t.Fatalf("process tasks: %v", err)
	}

	results, err := New(engine).Execute(`GET OrderStatus:9 ALLTIME WITH DIFF;`)
	if err != nil {
		t.Fatalf("execute timeline dsl: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected single result, got %d", len(results))
	}
}

func TestExecuteCustomEventLastAndRollback(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	exec := New(engine)
	if _, err := exec.Execute(`
		SET profile.0xdhwqiasui {"name":"tom"};
		SET profile.0xdhwqiasui {"name":"jack"} rename;
		SET profile.0xdhwqiasui {"name":"alice"};
	`); err != nil {
		t.Fatalf("seed versions: %v", err)
	}

	results, err := exec.Execute(`GET profile.0xdhwqiasui LAST -1;`)
	if err != nil {
		t.Fatalf("get last chain: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected one result, got %d", len(results))
	}

	if _, err := exec.Execute(`ROLLBACK profile.0xdhwqiasui VERSION:2;`); err != nil {
		t.Fatalf("rollback dsl: %v", err)
	}

	if _, err := exec.Execute(`
		SET balance:12 100;
		SET balance:12 -10 WITHDRAW;
		SET balance:12 20 TRANSFER;
	`); err != nil {
		t.Fatalf("seed numeric versions: %v", err)
	}

	if _, err := exec.Execute(`ROLLBACK balance:12 VERSION:LAST;`); err != nil {
		t.Fatalf("rollback last dsl: %v", err)
	}

	record, err := engine.Get("balance:12")
	if err != nil {
		t.Fatalf("get balance after rollback last: %v", err)
	}
	if string(record.Value) != `90` {
		t.Fatalf("unexpected rollback last value: %s", record.Value)
	}

	checkResults, err := exec.Execute(`CHECK balance:12 ALLTIME;`)
	if err != nil {
		t.Fatalf("check alltime dsl: %v", err)
	}
	if len(checkResults) != 1 {
		t.Fatalf("expected single check result, got %d", len(checkResults))
	}
}

func TestExecuteNumericDeltaAndBatchDSL(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	exec := New(engine)
	if _, err := exec.Execute(`
		SET balance:718231 998;
		SET balance:718231 -10.01 WITHDRAW;
		ASET balance:1 10, balance:2 20, balance:3 11.11 TRANSFER;
	`); err != nil {
		t.Fatalf("execute batch dsl: %v", err)
	}

	results, err := exec.Execute(`AGET balance:718231 balance:1 balance:2 balance:3;`)
	if err != nil {
		t.Fatalf("aget dsl: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected single batch result, got %d", len(results))
	}

	verifyResults, err := exec.Execute(`VERIFY STORAGE; COMPACT STORAGE;`)
	if err != nil {
		t.Fatalf("verify/compact dsl: %v", err)
	}
	if len(verifyResults) != 2 {
		t.Fatalf("expected two maintenance results, got %d", len(verifyResults))
	}

	snapshotResults, err := exec.Execute(`SNAPSHOT;`)
	if err != nil {
		t.Fatalf("snapshot dsl: %v", err)
	}
	if len(snapshotResults) != 1 {
		t.Fatalf("expected one snapshot result, got %d", len(snapshotResults))
	}

	perfResults, err := exec.Execute(`SHOW METRICS; SHOW PERF;`)
	if err != nil {
		t.Fatalf("show metrics/perf dsl: %v", err)
	}
	if len(perfResults) != 2 {
		t.Fatalf("expected two perf results, got %d", len(perfResults))
	}
}

func TestExecuteSearchAndIdempotencyDSL(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	exec := New(engine)
	results, err := exec.Execute(`
		SET balance:1234 -10 SEND i=diweuhasads;
		SET balance:1234 -10 SEND i=diweuhasads;
		SEARCH EVENTS KEY:balance:1234 NAME:send DESC LIMIT:10 PAGE:1;
	`)
	if err != nil {
		t.Fatalf("execute search/idempotency dsl: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected three results, got %d", len(results))
	}
	searchPage, ok := results[2].(db.SearchResultPage)
	if !ok {
		t.Fatalf("expected search result page, got %T", results[2])
	}
	if searchPage.Total != 1 || len(searchPage.Results) != 1 {
		t.Fatalf("unexpected search page: %+v", searchPage)
	}
	if searchPage.Results[0].IdempotencyKey != "diweuhasads" {
		t.Fatalf("unexpected idempotency key in result: %+v", searchPage.Results[0])
	}
}

func TestExecuteSearchWithSameIDSL(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	exec := New(engine)
	results, err := exec.Execute(`
		ASET balance:1234 -10 SEND i=abcd, balance:9999 10 RECEIVED i=abcd;
		SEARCH EVENTS KEY:balance:1234 NAME:SEND WITH SAME I DESC LIMIT:10 PAGE:1;
	`)
	if err != nil {
		t.Fatalf("execute search same i dsl: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected two results, got %d", len(results))
	}
	searchPage, ok := results[1].(db.SearchResultPage)
	if !ok {
		t.Fatalf("expected search result page, got %T", results[1])
	}
	if searchPage.Total != 2 || len(searchPage.Results) != 2 {
		t.Fatalf("unexpected search page: %+v", searchPage)
	}
}

func TestExecuteGetRawAndSuperValueExpansion(t *testing.T) {
	engine, err := db.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	exec := New(engine)
	if _, err := exec.Execute(`
		SET uid-hksn10 {"uid":"uid-hksn10","owner":"0xabc","activated":true};
		SET uid-asan10 {"uid":"uid-asan10","owner":"0xdef","activated":false};
		SET usersVape ["*uid-hksn10","*uid-asan10"];
	`); err != nil {
		t.Fatalf("seed super values: %v", err)
	}

	rawResults, err := exec.Execute(`GET usersVape RAW;`)
	if err != nil {
		t.Fatalf("get raw: %v", err)
	}
	rawRecord, ok := rawResults[0].(db.Record)
	if !ok {
		t.Fatalf("expected db.Record for raw result, got %T", rawResults[0])
	}
	if string(rawRecord.Value) != `["*uid-hksn10","*uid-asan10"]` {
		t.Fatalf("unexpected raw value: %s", rawRecord.Value)
	}

	expandedResults, err := exec.Execute(`GET usersVape;`)
	if err != nil {
		t.Fatalf("get expanded: %v", err)
	}
	expandedRecord, ok := expandedResults[0].(db.Record)
	if !ok {
		t.Fatalf("expected db.Record for expanded result, got %T", expandedResults[0])
	}
	var expanded []map[string]any
	if err := json.Unmarshal(expandedRecord.Value, &expanded); err != nil {
		t.Fatalf("decode expanded value: %v", err)
	}
	if len(expanded) != 2 {
		t.Fatalf("unexpected expanded refs count: %d", len(expanded))
	}
}
