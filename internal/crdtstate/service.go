package crdtstate

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// schema holds the service's own tables. No other service reads or writes
// them.
const schema = `
CREATE TABLE IF NOT EXISTS crdt_documents (
	document_id TEXT NOT NULL PRIMARY KEY,
	type        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS crdt_ops (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	value       BLOB,
	PRIMARY KEY (document_id, id)
);
CREATE TABLE IF NOT EXISTS crdt_counter_values (
	document_id TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	value       INTEGER NOT NULL,
	PRIMARY KEY (document_id, device_id)
);
CREATE TABLE IF NOT EXISTS crdt_set_elements (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	PRIMARY KEY (document_id, element)
);
CREATE TABLE IF NOT EXISTS crdt_register_ops (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	version     INTEGER NOT NULL,
	value       BLOB NOT NULL,
	PRIMARY KEY (document_id, id)
);
CREATE TABLE IF NOT EXISTS crdt_orset_tags (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	op_id       TEXT NOT NULL,
	PRIMARY KEY (document_id, element, op_id)
);
CREATE TABLE IF NOT EXISTS crdt_orset_tombstones (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	op_id       TEXT NOT NULL,
	removed_by  TEXT NOT NULL,
	PRIMARY KEY (document_id, element, op_id)
);
-- crdt_op_identities keeps, for each operation id whose merge row compaction
-- trimmed away, the only information idempotency and conflict decisions still
-- need: the originating device and a digest of the type-specific comparison
-- content. Compaction never trims this table; it does not participate in any
-- merge and holds a fixed-size digest rather than the operation payload, so
-- retaining one small row per trimmed id cannot grow the state back toward its
-- pre-compaction size.
CREATE TABLE IF NOT EXISTS crdt_op_identities (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	digest      BLOB NOT NULL,
	PRIMARY KEY (document_id, id)
);
`

// Gate is the boundary the service consults before observing any CRDT
// content. Every check runs inside the service's own serialized transaction,
// so a rejected submission or compaction cannot reveal type or state and a
// concurrent permission change cannot race the commit. The concrete
// implementation is the composition root, wiring the registration layer and
// the permission service; this package knows neither.
type Gate interface {
	// CheckDeviceTx fails with a registration-layer sentinel error when
	// deviceID is not a registered device.
	CheckDeviceTx(tx *sql.Tx, deviceID string) error
	// CheckPermissionTx fails with a permission sentinel error when deviceID's
	// permission for documentID is revoked.
	CheckPermissionTx(tx *sql.Tx, documentID, deviceID string) error
}

// Service is the durable CRDT state layer.
type Service struct {
	db   *sql.DB
	gate Gate

	// mu serializes submissions against each other, against compaction and
	// against a subscription's atomic register-plus-initial-read. Holding it
	// across a state-changing commit and its notification makes notifications
	// leave in transaction-commit order, and makes a commit's state land
	// either in a subscriber's initial read or in its queue, never both and
	// never neither. The single database connection already serializes the
	// transactions themselves, so this costs no concurrency.
	mu sync.Mutex

	subs *subRegistry
}

// New opens the CRDT state service on db, creating its tables if needed. gate
// may be nil for standalone use when submissions need no authorization; the
// running process wires it to the registration and permission services.
func New(db *sql.DB, gate Gate) (*Service, error) {
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{
		db:   db,
		gate: gate,
		subs: newSubRegistry(),
	}, nil
}

// gateDevice enforces the registration/permission boundary inside tx and
// before any CRDT content is observed.
func (s *Service) gateDevice(tx *sql.Tx, documentID, deviceID string) error {
	if s.gate == nil {
		return nil
	}
	if err := s.gate.CheckDeviceTx(tx, deviceID); err != nil {
		return err
	}
	return s.gate.CheckPermissionTx(tx, documentID, deviceID)
}

// Submit validates and commits one CRDT batch atomically.
//
// declaredType is the type the client asserts for the document
// ("counter", "gset", "register" or "orset"). It fixes the type on the
// document's first batch; every later batch must declare the same type. Two
// batches declaring different types race in one serialized transaction, so
// exactly one wins and the other gets an *ErrConflict (409).
//
// When a gate is configured the device is gated first (its sentinel errors)
// before any CRDT content is read. Every operation is then resolved against
// the committed history: a repeated id is idempotent only with the same
// device and content, otherwise it is an *ErrConflict; a counter contribution
// below the device's current maximum, or a register version not strictly
// greater than the device's accepted versions, is likewise an *ErrConflict.
// Any failure rejects the whole batch. Results come back in request order.
//
// When the committed batch actually moved the merged state (a counter maximum
// advanced, the set union grew, the register's winning value changed or the
// orset's live element set changed) the document's subscribers are notified
// with the new merged state after the commit; idempotent repeats, rejected
// batches and no-op additions or removals change nothing and notify nobody.
func (s *Service) Submit(documentID, declaredType string, ops []Op) ([]Result, error) {
	// Serialize submissions with each other, with compaction and with a
	// subscriber's atomic register+initial-read.
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	deviceID := ops[0].DeviceID

	if err := s.gateDevice(tx, documentID, deviceID); err != nil {
		return nil, err
	}

	// Resolve the document type: fixed on the first accepted batch.
	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// First batch for this document: its declared type wins the race.
		if _, err := tx.Exec(
			`INSERT INTO crdt_documents (document_id, type) VALUES (?, ?)`,
			documentID, declaredType,
		); err != nil {
			return nil, err
		}
		docType = declaredType
	case err != nil:
		return nil, err
	default:
		if docType != declaredType {
			return nil, &ErrConflict{
				Reason: fmt.Sprintf("document type is already %q", docType),
			}
		}
	}

	results := make([]Result, len(ops))

	var stateChanged bool
	switch docType {
	case TypeCounter:
		changed, err := applyCounterOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeGSet:
		changed, err := applyGSetOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeRegister:
		changed, err := applyRegisterOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeORSet:
		changed, err := applyORSetOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if stateChanged {
		// The committed batch moved the merged state; re-derive it (the merge
		// is never cached) and publish it after commit, so a subscriber only
		// ever observes a durable state and commits arrive in commit order.
		state, err := s.GetState(documentID)
		if err != nil {
			return results, nil
		}
		s.subs.notify(documentID, state)
	}
	return results, nil
}

// GetState returns the merged CRDT state for documentID. A document with no
// committed operations yields ErrNotFound (the caller answers 404).
//
// The merged value is derived from the per-device maxima (counter), the union
// table (gset), the version/id ordering of the accepted operations (register)
// or the live tags minus tombstones (orset), not cached: it is exactly what
// any equivalent batch order would converge to.
func (s *Service) GetState(documentID string) (State, error) {
	var docType string
	err := s.db.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return State{}, ErrNotFound
	case err != nil:
		return State{}, err
	}

	switch docType {
	case TypeCounter:
		var total int64
		if err := s.db.QueryRow(
			`SELECT COALESCE(SUM(value), 0) FROM crdt_counter_values WHERE document_id = ?`,
			documentID,
		).Scan(&total); err != nil {
			return State{}, err
		}
		raw, err := json.Marshal(total)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	case TypeGSet:
		rows, err := s.db.Query(
			`SELECT element FROM crdt_set_elements WHERE document_id = ? ORDER BY element ASC`,
			documentID,
		)
		if err != nil {
			return State{}, err
		}
		elements := make([]string, 0)
		for rows.Next() {
			var element string
			if err := rows.Scan(&element); err != nil {
				_ = rows.Close()
				return State{}, err
			}
			elements = append(elements, element)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return State{}, err
		}
		_ = rows.Close()
		raw, err := json.Marshal(elements)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	case TypeRegister:
		// A document's type row commits together with its first batch, so a
		// register document always has at least one accepted operation.
		var value []byte
		if err := s.db.QueryRow(
			`SELECT value FROM crdt_register_ops WHERE document_id = ?
			 ORDER BY version DESC, id ASC LIMIT 1`,
			documentID,
		).Scan(&value); err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: json.RawMessage(value)}, nil
	case TypeORSet:
		elements, err := queryORSetElements(s.db, documentID)
		if err != nil {
			return State{}, err
		}
		raw, err := json.Marshal(elements)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	default:
		return State{}, fmt.Errorf("unknown crdt type %q stored for document", docType)
	}
}
