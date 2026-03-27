package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/paerx/anhebridgedb/internal/storage"
)

var appendTxnRecord = storage.AppendTxnRecord

var txnApplyHook func(txnID string, op txnOp) error

var (
	ErrTxnNotFound    = errors.New("transaction not found")
	ErrTxnCommitted   = errors.New("transaction already committed")
	ErrTxnKeyLocked   = errors.New("key locked by another transaction")
	ErrTxnWriteDenied = errors.New("write denied by active transaction lock")
)

type txnOpKind string

const (
	txnOpSet    txnOpKind = "SET"
	txnOpDelete txnOpKind = "DELETE"
)

type txnOp struct {
	Seq            uint64
	Kind           txnOpKind
	Key            string
	Value          json.RawMessage
	EventName      string
	IdempotencyKey string
}

type txnState struct {
	ID         string
	CreatedAt  time.Time
	Committed  bool
	NextSeq    uint64
	Writes     map[string]txnOp
	Ops        []txnOp
	LockedKeys map[string]struct{}
}

type txnRecoveryState struct {
	Committed bool
	Applied   bool
	Aborted   bool
	Ops       []txnOp
}

func (e *Engine) BeginTxn() (string, error) {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	next := atomic.AddUint64(&e.txCounter, 1)
	id := fmt.Sprintf("tx-%d-%d", time.Now().UTC().UnixNano(), next)
	state := &txnState{
		ID:         id,
		CreatedAt:  time.Now().UTC(),
		Writes:     map[string]txnOp{},
		Ops:        make([]txnOp, 0, 8),
		LockedKeys: map[string]struct{}{},
	}
	e.txns[id] = state
	if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
		Type:  storage.TxnRecordBegin,
		TxnID: id,
	}); err != nil {
		delete(e.txns, id)
		return "", err
	}
	return id, nil
}

func (e *Engine) AbortTxn(txnID string) error {
	e.txMu.Lock()
	state, ok := e.txns[txnID]
	if !ok {
		e.txMu.Unlock()
		return ErrTxnNotFound
	}
	if state.Committed {
		e.txMu.Unlock()
		return ErrTxnCommitted
	}
	if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
		Type:  storage.TxnRecordAbort,
		TxnID: txnID,
	}); err != nil {
		e.txMu.Unlock()
		return err
	}
	e.releaseTxnLocked(txnID)
	e.txMu.Unlock()
	return nil
}

func (e *Engine) CommitTxn(txnID string) ([]storage.Event, error) {
	e.txMu.Lock()
	state, ok := e.txns[txnID]
	if !ok {
		e.txMu.Unlock()
		return nil, ErrTxnNotFound
	}
	if !state.Committed {
		if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
			Type:  storage.TxnRecordCommit,
			TxnID: txnID,
		}); err != nil {
			e.txMu.Unlock()
			return nil, err
		}
		state.Committed = true
	}
	ops := append([]txnOp(nil), state.Ops...)
	e.txMu.Unlock()

	persisted, err := e.applyTxnOps(txnID, ops)
	if err != nil {
		e.txMu.Lock()
		e.releaseTxnKeysLocked(txnID)
		e.txMu.Unlock()
		return persisted, err
	}
	if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
		Type:  storage.TxnRecordApplied,
		TxnID: txnID,
	}); err != nil {
		e.txMu.Lock()
		e.releaseTxnKeysLocked(txnID)
		e.txMu.Unlock()
		return persisted, err
	}

	e.txMu.Lock()
	e.releaseTxnLocked(txnID)
	e.txMu.Unlock()
	return persisted, nil
}

func (e *Engine) TxnSet(txnID, key string, value json.RawMessage, eventName, idempotencyKey string) error {
	e.txMu.Lock()
	state, ok := e.txns[txnID]
	if !ok {
		e.txMu.Unlock()
		return ErrTxnNotFound
	}
	if state.Committed {
		e.txMu.Unlock()
		return ErrTxnCommitted
	}
	owner, locked := e.txKeyOwner[key]
	if locked && owner != txnID {
		e.txMu.Unlock()
		return ErrTxnKeyLocked
	}
	newLock := !locked
	if newLock {
		e.txKeyOwner[key] = txnID
		state.LockedKeys[key] = struct{}{}
	}
	state.NextSeq++
	op := txnOp{
		Seq:            state.NextSeq,
		Kind:           txnOpSet,
		Key:            key,
		Value:          clone(value),
		EventName:      eventName,
		IdempotencyKey: idempotencyKey,
	}
	if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
		Type:           storage.TxnRecordOpSet,
		TxnID:          txnID,
		Seq:            op.Seq,
		Key:            key,
		Value:          clone(value),
		EventName:      eventName,
		IdempotencyKey: idempotencyKey,
	}); err != nil {
		if newLock {
			e.releaseTxnKeyLocked(txnID, key)
		}
		e.txMu.Unlock()
		return err
	}
	state.Writes[key] = op
	state.Ops = append(state.Ops, op)
	e.txMu.Unlock()
	return nil
}

func (e *Engine) TxnDelete(txnID, key string) error {
	e.txMu.Lock()
	state, ok := e.txns[txnID]
	if !ok {
		e.txMu.Unlock()
		return ErrTxnNotFound
	}
	if state.Committed {
		e.txMu.Unlock()
		return ErrTxnCommitted
	}
	owner, locked := e.txKeyOwner[key]
	if locked && owner != txnID {
		e.txMu.Unlock()
		return ErrTxnKeyLocked
	}
	newLock := !locked
	if newLock {
		e.txKeyOwner[key] = txnID
		state.LockedKeys[key] = struct{}{}
	}
	state.NextSeq++
	op := txnOp{
		Seq:  state.NextSeq,
		Kind: txnOpDelete,
		Key:  key,
	}
	if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
		Type:  storage.TxnRecordOpDelete,
		TxnID: txnID,
		Seq:   op.Seq,
		Key:   key,
	}); err != nil {
		if newLock {
			e.releaseTxnKeyLocked(txnID, key)
		}
		e.txMu.Unlock()
		return err
	}
	state.Writes[key] = op
	state.Ops = append(state.Ops, op)
	e.txMu.Unlock()
	return nil
}

func (e *Engine) GetInTxn(txnID, key string) (Record, error) {
	e.txMu.Lock()
	state, ok := e.txns[txnID]
	if !ok {
		e.txMu.Unlock()
		return Record{}, ErrTxnNotFound
	}
	op, has := state.Writes[key]
	e.txMu.Unlock()
	if has {
		if op.Kind == txnOpDelete {
			return Record{}, ErrNotFound
		}
		return Record{
			Value:     clone(op.Value),
			Version:   0,
			UpdatedAt: time.Now().UTC(),
		}, nil
	}
	return e.Get(key)
}

func (e *Engine) ensureWriteAllowed(key, txnID string) error {
	e.txMu.Lock()
	defer e.txMu.Unlock()
	owner, ok := e.txKeyOwner[key]
	if !ok || owner == "" || owner == txnID {
		return nil
	}
	if txnID == "" {
		return ErrTxnWriteDenied
	}
	return ErrTxnKeyLocked
}

func (e *Engine) lockTxnKeyLocked(txnID, key string) error {
	owner, ok := e.txKeyOwner[key]
	if ok && owner != "" && owner != txnID {
		return ErrTxnKeyLocked
	}
	e.txKeyOwner[key] = txnID
	e.txns[txnID].LockedKeys[key] = struct{}{}
	return nil
}

func (e *Engine) releaseTxnLocked(txnID string) {
	state, ok := e.txns[txnID]
	if !ok {
		return
	}
	for key := range state.LockedKeys {
		if owner, ok := e.txKeyOwner[key]; ok && owner == txnID {
			delete(e.txKeyOwner, key)
		}
	}
	delete(e.txnAppliedOps, txnID)
	delete(e.txns, txnID)
}

func (e *Engine) releaseTxnKeyLocked(txnID, key string) {
	state, ok := e.txns[txnID]
	if !ok {
		return
	}
	if owner, ok := e.txKeyOwner[key]; ok && owner == txnID {
		delete(e.txKeyOwner, key)
	}
	delete(state.LockedKeys, key)
}

func (e *Engine) releaseTxnKeysLocked(txnID string) {
	state, ok := e.txns[txnID]
	if !ok {
		return
	}
	for key := range state.LockedKeys {
		if owner, ok := e.txKeyOwner[key]; ok && owner == txnID {
			delete(e.txKeyOwner, key)
		}
		delete(state.LockedKeys, key)
	}
}

func (e *Engine) applyTxnOps(txnID string, ops []txnOp) ([]storage.Event, error) {
	sort.Slice(ops, func(i, j int) bool { return ops[i].Seq < ops[j].Seq })
	results := make([]storage.Event, 0, len(ops))
	for _, op := range ops {
		if e.isTxnOpApplied(txnID, op.Seq) {
			continue
		}
		if txnApplyHook != nil {
			if err := txnApplyHook(txnID, op); err != nil {
				return results, err
			}
		}
		switch op.Kind {
		case txnOpSet:
			persisted, err := e.setWithMetaInTxn(op.Key, op.Value, "txn", "txn", "", op.EventName, op.IdempotencyKey, txnID, op.Seq, 0)
			if err != nil {
				return results, err
			}
			results = append(results, persisted)
		case txnOpDelete:
			persisted, err := e.deleteInTxn(op.Key, txnID, op.Seq)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					e.markTxnOpApplied(txnID, op.Seq)
					continue
				}
				return results, err
			}
			results = append(results, persisted)
		}
	}
	return results, nil
}

func (e *Engine) markTxnOpApplied(txnID string, seq uint64) {
	if txnID == "" || seq == 0 {
		return
	}
	e.txMu.Lock()
	defer e.txMu.Unlock()
	if _, ok := e.txnAppliedOps[txnID]; !ok {
		e.txnAppliedOps[txnID] = map[uint64]struct{}{}
	}
	e.txnAppliedOps[txnID][seq] = struct{}{}
}

func (e *Engine) isTxnOpApplied(txnID string, seq uint64) bool {
	if txnID == "" || seq == 0 {
		return false
	}
	e.txMu.Lock()
	defer e.txMu.Unlock()
	ops, ok := e.txnAppliedOps[txnID]
	if !ok {
		return false
	}
	_, ok = ops[seq]
	return ok
}

func (e *Engine) recoverCommittedTransactions() error {
	records, err := storage.ReadTxnRecords(e.dataDir)
	if err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	stateByTxn := make(map[string]*txnRecoveryState)
	for _, rec := range records {
		if rec.TxnID == "" {
			continue
		}
		state, ok := stateByTxn[rec.TxnID]
		if !ok {
			state = &txnRecoveryState{Ops: make([]txnOp, 0)}
			stateByTxn[rec.TxnID] = state
		}
		switch rec.Type {
		case storage.TxnRecordCommit:
			state.Committed = true
		case storage.TxnRecordApplied:
			state.Applied = true
		case storage.TxnRecordAbort:
			state.Aborted = true
		case storage.TxnRecordOpSet:
			state.Ops = append(state.Ops, txnOp{
				Seq:            rec.Seq,
				Kind:           txnOpSet,
				Key:            rec.Key,
				Value:          clone(rec.Value),
				EventName:      rec.EventName,
				IdempotencyKey: rec.IdempotencyKey,
			})
		case storage.TxnRecordOpDelete:
			state.Ops = append(state.Ops, txnOp{
				Seq:  rec.Seq,
				Kind: txnOpDelete,
				Key:  rec.Key,
			})
		}
	}
	for txnID, state := range stateByTxn {
		if !state.Committed || state.Applied || state.Aborted {
			continue
		}
		if _, err := e.applyTxnOps(txnID, state.Ops); err != nil {
			return fmt.Errorf("replay committed txn %s: %w", txnID, err)
		}
		if err := appendTxnRecord(e.dataDir, storage.TxnRecord{
			Type:  storage.TxnRecordApplied,
			TxnID: txnID,
		}); err != nil {
			return err
		}
	}
	return nil
}
