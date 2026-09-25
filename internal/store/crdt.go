// CRDT state sits beside the change log as an independent, automatically
// mergeable state layer. A document's CRDT type is declared with its first
// operation batch — "counter", "gset" (a grow-only set) or "register" (a
// last-writer-wins single value) — and never changes afterward; two batches
// declaring different types concurrently serialize in one transaction and
// exactly one takes effect.
//
// A counter operation carries the originating device's accumulated
// contribution. Each device's contribution only moves forward (a regressing
// value is an ErrCRDTConflict and changes nothing), and the merged state is the
// sum of every device's maximum contribution — a PN-free grow-only counter
// (GCounter), so the merge is commutative, associative and idempotent.
//
// A gset operation adds elements; the merged state is the union of every
// accepted element, presented in ascending order. Re-adding an element does
// not change the state.
//
// A register operation carries a single arbitrary JSON value and a logical
// version. A device's versions only move strictly forward (a regressing or
// equal version is an ErrCRDTConflict and changes nothing), and the merged
// state is the value of the operation with the largest version, ties broken
// by the smaller operation id — a last-writer-wins register over a logical
// clock, so the merge is commutative, associative and idempotent.
//
// Every operation carries a stable, client-supplied id, unique per document.
// Re-posting the same id with the same content and origin is idempotent; the
// same id with different content or a different device is an
// ErrCRDTConflict and leaves the state untouched. Operations and the merged
// result commit together in one serialized transaction and are durable: the
// decisions and the merged state are unchanged after a restart.
//
// The CRDT layer shares the registration/permission layer with the rest of the
// service: an unregistered device yields ErrDeviceNotFound (404) and a revoked
// device yields ErrPermissionDenied (403), before any CRDT content is observed.
// It never touches the change log, document cursors, snapshots or the
// subscription machinery.

package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// CRDT document types.
const (
	// CRDTTypeCounter is the grow-only counter: the merged value is the sum of
	// each device's maximum accumulated contribution.
	CRDTTypeCounter = "counter"
	// CRDTTypeGSet is the grow-only set: the merged value is the union of all
	// added elements.
	CRDTTypeGSet = "gset"
	// CRDTTypeRegister is the last-writer-wins register: the merged value is
	// the value of the operation with the largest logical version, ties broken
	// by the smaller operation id.
	CRDTTypeRegister = "register"
)

// ErrCRDTNotFound reports that no CRDT operation has ever been committed for
// the document, so it has neither a type nor a merged state yet. The caller
// maps it to 404.
var ErrCRDTNotFound = errors.New("crdt state not found")

// ErrCRDTConflict reports a rejected CRDT batch: the document's type is
// already fixed to another type, a counter contribution regressed, or an
// operation id already exists with different content or origin. Nothing is
// written; the caller maps it to 409.
type ErrCRDTConflict struct {
	// ID is the conflicting operation id when one is known; it is empty for a
	// document-level conflict (a mismatched declared type).
	ID string
	// Reason carries a short, machine-facing description of the conflict.
	Reason string
}

func (e *ErrCRDTConflict) Error() string {
	if e.ID != "" {
		return fmt.Sprintf("crdt conflict for operation %q: %s", e.ID, e.Reason)
	}
	return "crdt conflict: " + e.Reason
}

// CRDTOp is one element of an inbound CRDT batch.
//
// For a counter, Value is the device's accumulated contribution: it must be a
// JSON number that is a non-negative integer, and it may never decrease for
// the same device. Elements and Version are unused.
//
// For a gset, Elements lists the strings the operation adds to the set and
// must be non-empty; Value and Version are unused.
//
// For a register, Value is any JSON value (null included) stored as-is and
// Version is the operation's non-negative logical version: it must strictly
// increase for the same device. Elements is unused.
type CRDTOp struct {
	ID       string          // client-supplied stable id, unique per document
	DeviceID string          // originating device (shared by the whole batch)
	Value    json.RawMessage // counter: contribution; register: the stored JSON value
	Elements []string        // gset: elements to add
	Version  int64           // register: logical version, strictly increasing per device
}

// CRDTState is a document's merged CRDT state.
//
// Value holds the type-specific merged result: for a counter it decodes to the
// JSON integer sum of per-device maxima; for a gset it decodes to the sorted
// JSON array of every accepted element (an empty set is []); for a register it
// is the winning operation's JSON value, exactly as submitted.
type CRDTState struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// CRDTResult reports the outcome for one element of an accepted CRDT batch.
type CRDTResult struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
}

// crdtSchema is appended to the schema created in init.
const crdtSchema = `
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
`

// SubmitCRDTOps validates and commits one CRDT batch atomically.
//
// declaredType is the type the client asserts for the document
// ("counter", "gset" or "register"). It fixes the type on the document's
// first batch; every later batch must declare the same type. Two batches
// declaring different types race in one serialized transaction, so exactly
// one wins and the other gets an *ErrCRDTConflict (409).
//
// The device is gated first (ErrDeviceNotFound / ErrPermissionDenied) before
// any CRDT content is read. Every operation is then resolved against the
// committed history: a repeated id is idempotent only with the same device and
// content, otherwise it is an *ErrCRDTConflict; a counter contribution below
// the device's current maximum or a register version at or below the device's
// current maximum is likewise an *ErrCRDTConflict. Any failure rejects the
// whole batch. Results come back in request order.
//
// When the committed batch actually moved the merged state (a counter maximum
// advanced, the set union grew or the register's winning operation changed)
// the document's CRDT state subscribers are notified with the new merged
// state after the commit; idempotent repeats, rejected batches and no-op
// additions change nothing and notify nobody.
func (s *Store) SubmitCRDTOps(documentID, declaredType string, ops []CRDTOp) ([]CRDTResult, error) {
	// Serialize submissions with each other and with a subscriber's atomic
	// register+initial-read: notifications then leave in commit order, and a
	// committed state lands either in the initial read or in the queue.
	s.crdtMu.Lock()
	defer s.crdtMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	deviceID := ops[0].DeviceID

	// Registration and permission are enforced before any CRDT content is
	// observed, so a rejected submission cannot reveal type or state.
	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return nil, err
	}
	if !deviceExists {
		return nil, ErrDeviceNotFound
	}
	var storedAuth int
	err = tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&storedAuth)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
	case err != nil:
		return nil, err
	default:
		if storedAuth == 0 {
			return nil, ErrPermissionDenied
		}
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
			return nil, &ErrCRDTConflict{
				Reason: fmt.Sprintf("document type is already %q", docType),
			}
		}
	}

	results := make([]CRDTResult, len(ops))

	var stateChanged bool
	switch docType {
	case CRDTTypeCounter:
		changed, err := applyCounterOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case CRDTTypeGSet:
		changed, err := applyGSetOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case CRDTTypeRegister:
		changed, err := applyRegisterOps(tx, documentID, ops, results)
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
		state, err := s.GetCRDTState(documentID)
		if err != nil {
			return results, nil
		}
		s.notifyCRDTSubscribers(documentID, state)
	}
	return results, nil
}

// applyCounterOps resolves and applies every counter operation inside tx. A
// repeated id is idempotent only with the same device and contribution. A new
// id with a contribution below the device's stored maximum regresses the
// counter and is rejected; an equal contribution is an accepted no-op; a
// larger one advances the device's maximum. It reports whether the merged
// state actually changed — only an advancing contribution moves the sum.
func applyCounterOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) (bool, error) {
	changed := false
	for i, op := range ops {
		value, err := decodeCounterValue(op.Value)
		if err != nil {
			return false, &ErrCRDTConflict{ID: op.ID, Reason: err.Error()}
		}

		var existingDevice string
		var existingValue []byte
		err = tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// New id: enforce monotonicity against this device's maximum.
			var current sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT value FROM crdt_counter_values WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&current); scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
				return false, scanErr
			}
			if current.Valid && value < current.Int64 {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "counter contribution must not decrease",
				}
			}
			if _, err := tx.Exec(
				`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
				documentID, op.ID, op.DeviceID, []byte(op.Value),
			); err != nil {
				return false, err
			}
			if !current.Valid || value > current.Int64 {
				if _, err := tx.Exec(
					`INSERT INTO crdt_counter_values (document_id, device_id, value) VALUES (?, ?, ?)
					 ON CONFLICT (document_id, device_id) DO UPDATE SET value = excluded.value`,
					documentID, op.DeviceID, value,
				); err != nil {
					return false, err
				}
				changed = true
			}
			results[i] = CRDTResult{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !jsonEqual(existingValue, op.Value) {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or value",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
		}
	}
	return changed, nil
}

// applyGSetOps resolves and applies every grow-only-set operation inside tx.
// A repeated id is idempotent only with the same device and the same set of
// elements (element order is irrelevant to the merge). New elements are
// inserted into the union table; re-adding an element changes nothing. It
// reports whether the merged state actually changed — only an element that
// was not already in the union moves it.
func applyGSetOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) (bool, error) {
	changed := false
	for i, op := range ops {
		var existingDevice string
		var existingElements []byte
		err := tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingElements)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// New id; insert the op and its elements below.
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !stringSetEqual(existingElements, op.Elements) {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or elements",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
			continue
		}

		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, op.DeviceID, []byte(encodeSetElements(op.Elements)),
		); err != nil {
			return false, err
		}
		for _, element := range op.Elements {
			// The union table is an identity insert: a repeated element simply
			// exists once, and only a genuinely new element moves the merge.
			res, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_set_elements (document_id, element) VALUES (?, ?)`,
				documentID, element,
			)
			if err != nil {
				return false, err
			}
			if n, err := res.RowsAffected(); err == nil && n > 0 {
				changed = true
			}
		}
		results[i] = CRDTResult{ID: op.ID, Created: true}
	}
	return changed, nil
}

// registerWinner is the register operation that currently wins the merge: the
// one with the largest version, ties broken by the smaller id.
type registerWinner struct {
	id      string
	version int64
	value   []byte
}

// queryRegisterWinner returns the winning register operation inside tx, or
// ok=false when the document has no register operation yet.
func queryRegisterWinner(tx *sql.Tx, documentID string) (registerWinner, bool, error) {
	var w registerWinner
	err := tx.QueryRow(
		`SELECT id, version, value FROM crdt_register_ops WHERE document_id = ?
		 ORDER BY version DESC, id ASC LIMIT 1`,
		documentID,
	).Scan(&w.id, &w.version, &w.value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return registerWinner{}, false, nil
	case err != nil:
		return registerWinner{}, false, err
	}
	return w, true, nil
}

// applyRegisterOps resolves and applies every register operation inside tx. A
// repeated id is idempotent only with the same device, value and version. A
// new id whose version is not strictly greater than the device's accepted
// maximum regresses the logical clock and is rejected. It reports whether the
// merged state actually changed — only a new operation that overtakes the
// current winner moves it.
func applyRegisterOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) (bool, error) {
	before, _, err := queryRegisterWinner(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Version < 0 || len(op.Value) == 0 {
			// The API boundary validates both; a violation here means the
			// caller skipped validation.
			return false, &ErrCRDTConflict{
				ID:     op.ID,
				Reason: "register op must carry a JSON value and a non-negative integer version",
			}
		}

		var existingDevice string
		var existingVersion int64
		var existingValue []byte
		err := tx.QueryRow(
			`SELECT device_id, version, value FROM crdt_register_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingVersion, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// New id: the version must strictly exceed this device's accepted
			// maximum (ops committed earlier in this batch included).
			var current sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT MAX(version) FROM crdt_register_ops WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&current); scanErr != nil {
				return false, scanErr
			}
			if current.Valid && op.Version <= current.Int64 {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "register version must strictly increase for a device",
				}
			}
			if _, err := tx.Exec(
				`INSERT INTO crdt_register_ops (document_id, id, device_id, version, value) VALUES (?, ?, ?, ?, ?)`,
				documentID, op.ID, op.DeviceID, op.Version, []byte(op.Value),
			); err != nil {
				return false, err
			}
			results[i] = CRDTResult{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || existingVersion != op.Version || !jsonEqual(existingValue, op.Value) {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, value or version",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
		}
	}

	after, _, err := queryRegisterWinner(tx, documentID)
	if err != nil {
		return false, err
	}
	changed := before.id != after.id || before.version != after.version ||
		!jsonEqual(before.value, after.value)
	return changed, nil
}

// decodeCounterValue requires raw to be a non-negative JSON integer with no
// fraction or exponent. Floats, strings, booleans and null are rejected at the
// API boundary; a malformed integer here means the caller skipped validation.
func decodeCounterValue(raw json.RawMessage) (int64, error) {
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil || n < 0 {
		return 0, errors.New("counter value must be a non-negative integer")
	}
	return n, nil
}

// encodeSetElements renders the elements of one gset operation as a JSON array
// for durable storage of the original operation content.
func encodeSetElements(elements []string) json.RawMessage {
	raw, err := json.Marshal(elements)
	if err != nil {
		// Strings always marshal.
		return json.RawMessage("[]")
	}
	return raw
}

// stringSetEqual reports whether raw (a JSON array of strings stored for a
// prior gset operation) contains exactly the elements of want, ignoring
// duplicates and order: the content of a set operation is the set of elements.
func stringSetEqual(raw json.RawMessage, want []string) bool {
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		return false
	}
	set := make(map[string]struct{}, len(got))
	for _, e := range got {
		set[e] = struct{}{}
	}
	for _, e := range want {
		if _, ok := set[e]; !ok {
			return false
		}
		delete(set, e)
	}
	return len(set) == 0
}

// GetCRDTState returns the merged CRDT state for documentID. A document with
// no committed operations yields ErrCRDTNotFound (the caller answers 404).
//
// The merged value is derived from the per-device maxima (counter), the union
// table (gset) or the winning register operation, not cached: it is exactly
// what any equivalent batch order would converge to.
func (s *Store) GetCRDTState(documentID string) (CRDTState, error) {
	var docType string
	err := s.db.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return CRDTState{}, ErrCRDTNotFound
	case err != nil:
		return CRDTState{}, err
	}

	switch docType {
	case CRDTTypeCounter:
		var total int64
		if err := s.db.QueryRow(
			`SELECT COALESCE(SUM(value), 0) FROM crdt_counter_values WHERE document_id = ?`,
			documentID,
		).Scan(&total); err != nil {
			return CRDTState{}, err
		}
		raw, err := json.Marshal(total)
		if err != nil {
			return CRDTState{}, err
		}
		return CRDTState{Type: docType, Value: raw}, nil
	case CRDTTypeGSet:
		rows, err := s.db.Query(
			`SELECT element FROM crdt_set_elements WHERE document_id = ? ORDER BY element ASC`,
			documentID,
		)
		if err != nil {
			return CRDTState{}, err
		}
		elements := make([]string, 0)
		for rows.Next() {
			var element string
			if err := rows.Scan(&element); err != nil {
				_ = rows.Close()
				return CRDTState{}, err
			}
			elements = append(elements, element)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return CRDTState{}, err
		}
		_ = rows.Close()
		raw, err := json.Marshal(elements)
		if err != nil {
			return CRDTState{}, err
		}
		return CRDTState{Type: docType, Value: raw}, nil
	case CRDTTypeRegister:
		var value []byte
		err := s.db.QueryRow(
			`SELECT value FROM crdt_register_ops WHERE document_id = ?
			 ORDER BY version DESC, id ASC LIMIT 1`,
			documentID,
		).Scan(&value)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The document row exists only after an accepted batch, which
			// always inserts at least one register operation.
			return CRDTState{}, ErrCRDTNotFound
		case err != nil:
			return CRDTState{}, err
		}
		return CRDTState{Type: docType, Value: json.RawMessage(value)}, nil
	default:
		return CRDTState{}, fmt.Errorf("unknown crdt type %q stored for document", docType)
	}
}
