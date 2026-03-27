package storage

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type TxnRecordType string

const (
	TxnRecordBegin    TxnRecordType = "BEGIN"
	TxnRecordOpSet    TxnRecordType = "OP_SET"
	TxnRecordOpDelete TxnRecordType = "OP_DELETE"
	TxnRecordCommit   TxnRecordType = "COMMIT"
	TxnRecordApplied  TxnRecordType = "APPLIED"
	TxnRecordAbort    TxnRecordType = "ABORT"
)

type TxnRecord struct {
	Type           TxnRecordType   `json:"type"`
	TxnID          string          `json:"txn_id"`
	Seq            uint64          `json:"seq,omitempty"`
	Key            string          `json:"key,omitempty"`
	Value          json.RawMessage `json:"value,omitempty"`
	EventName      string          `json:"event_name,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Timestamp      time.Time       `json:"ts"`
}

func TxnLogDir(dir string) string {
	return filepath.Join(dir, "system", "txn")
}

func TxnLogPath(dir string) string {
	return filepath.Join(TxnLogDir(dir), "txn.wal")
}

func AppendTxnRecord(dir string, rec TxnRecord) error {
	if rec.Timestamp.IsZero() {
		rec.Timestamp = time.Now().UTC()
	}
	path := TxnLogPath(dir)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	bytes, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(bytes, '\n')); err != nil {
		return err
	}
	return file.Sync()
}

func ReadTxnRecords(dir string) ([]TxnRecord, error) {
	path := TxnLogPath(dir)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	out := make([]TxnRecord, 0)
	decoder := json.NewDecoder(file)
	for {
		var rec TxnRecord
		if err := decoder.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Timestamp.Equal(out[j].Timestamp) {
			if out[i].TxnID == out[j].TxnID {
				return out[i].Seq < out[j].Seq
			}
			return out[i].TxnID < out[j].TxnID
		}
		return out[i].Timestamp.Before(out[j].Timestamp)
	})
	return out, nil
}
