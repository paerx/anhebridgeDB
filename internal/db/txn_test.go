package db

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/paerx/anhebridgedb/internal/storage"
)

func setTxnHooks(t *testing.T, appendHook func(string, storage.TxnRecord) error, applyHook func(string, txnOp) error) {
	t.Helper()
	oldAppend := appendTxnRecord
	oldApply := txnApplyHook
	if appendHook != nil {
		appendTxnRecord = appendHook
	} else {
		appendTxnRecord = storage.AppendTxnRecord
	}
	txnApplyHook = applyHook
	t.Cleanup(func() {
		appendTxnRecord = oldAppend
		txnApplyHook = oldApply
	})
}

func TestTxnSetWALFailureReleasesKeyLock(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	setTxnHooks(t, func(dir string, rec storage.TxnRecord) error {
		if rec.Type == storage.TxnRecordOpSet {
			return errors.New("forced txn wal failure")
		}
		return storage.AppendTxnRecord(dir, rec)
	}, nil)

	txnID, err := engine.BeginTxn()
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := engine.TxnSet(txnID, "txn:key", json.RawMessage(`"value"`), "", ""); err == nil {
		t.Fatal("expected txn set to fail")
	}
	if _, err := engine.Set("txn:key", json.RawMessage(`"outside"`)); err != nil {
		t.Fatalf("expected key lock to be released after txn set failure: %v", err)
	}
}

func TestCommitTxnWalFailurePreservesStateAndAllowsRetry(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer engine.Close()

	var commitFailures int
	setTxnHooks(t, func(dir string, rec storage.TxnRecord) error {
		if rec.Type == storage.TxnRecordCommit && commitFailures == 0 {
			commitFailures++
			return errors.New("forced commit wal failure")
		}
		return storage.AppendTxnRecord(dir, rec)
	}, nil)

	txnID, err := engine.BeginTxn()
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := engine.TxnSet(txnID, "txn:retry", json.RawMessage(`"committed"`), "", ""); err != nil {
		t.Fatalf("txn set: %v", err)
	}

	if _, err := engine.CommitTxn(txnID); err == nil {
		t.Fatal("expected commit wal failure")
	}
	if _, err := engine.Get("txn:retry"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("value should not be visible before successful commit, got %v", err)
	}
	if _, err := engine.Set("txn:retry", json.RawMessage(`"outside"`)); err == nil {
		t.Fatal("expected active txn lock to block outside write after commit wal failure")
	}

	committed, err := engine.CommitTxn(txnID)
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("unexpected committed event count: %d", len(committed))
	}

	record, err := engine.Get("txn:retry")
	if err != nil {
		t.Fatalf("get after successful commit: %v", err)
	}
	if string(record.Value) != `"committed"` {
		t.Fatalf("unexpected committed value: %s", record.Value)
	}
}

func TestCommitTxnRecoveryIsIdempotentAfterApplyInterruption(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	setTxnHooks(t, nil, func(txnID string, op txnOp) error {
		if txnID != "" && op.Seq == 2 {
			return errors.New("forced apply interruption")
		}
		return nil
	})

	txnID, err := engine.BeginTxn()
	if err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := engine.TxnSet(txnID, "txn:recover:1", json.RawMessage(`"a"`), "", ""); err != nil {
		t.Fatalf("txn set 1: %v", err)
	}
	if err := engine.TxnSet(txnID, "txn:recover:2", json.RawMessage(`"b"`), "", ""); err != nil {
		t.Fatalf("txn set 2: %v", err)
	}

	if _, err := engine.CommitTxn(txnID); err == nil {
		t.Fatal("expected apply interruption")
	}
	if len(engine.txKeyOwner) != 0 {
		t.Fatalf("expected txn key locks to be released after apply interruption, got %d", len(engine.txKeyOwner))
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	setTxnHooks(t, nil, nil)

	reopened, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer reopened.Close()

	first, err := reopened.Get("txn:recover:1")
	if err != nil {
		t.Fatalf("get first key after recovery: %v", err)
	}
	if string(first.Value) != `"a"` {
		t.Fatalf("unexpected first key value: %s", first.Value)
	}
	second, err := reopened.Get("txn:recover:2")
	if err != nil {
		t.Fatalf("get second key after recovery: %v", err)
	}
	if string(second.Value) != `"b"` {
		t.Fatalf("unexpected second key value: %s", second.Value)
	}

	firstTimeline, err := reopened.Timeline("txn:recover:1", false)
	if err != nil {
		t.Fatalf("timeline first key: %v", err)
	}
	if len(firstTimeline) != 1 {
		t.Fatalf("expected one applied event for first key, got %d", len(firstTimeline))
	}
	secondTimeline, err := reopened.Timeline("txn:recover:2", false)
	if err != nil {
		t.Fatalf("timeline second key: %v", err)
	}
	if len(secondTimeline) != 1 {
		t.Fatalf("expected one applied event for second key, got %d", len(secondTimeline))
	}

	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened engine: %v", err)
	}

	again, err := Open(dir)
	if err != nil {
		t.Fatalf("open engine second time: %v", err)
	}
	defer again.Close()

	firstTimeline, err = again.Timeline("txn:recover:1", false)
	if err != nil {
		t.Fatalf("timeline first key second reopen: %v", err)
	}
	if len(firstTimeline) != 1 {
		t.Fatalf("expected idempotent recovery for first key, got %d events", len(firstTimeline))
	}
	secondTimeline, err = again.Timeline("txn:recover:2", false)
	if err != nil {
		t.Fatalf("timeline second key second reopen: %v", err)
	}
	if len(secondTimeline) != 1 {
		t.Fatalf("expected idempotent recovery for second key, got %d events", len(secondTimeline))
	}
}
