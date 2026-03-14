package dsl

import (
	"testing"
	"time"

	"anhebridgedb/internal/db"
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
}
