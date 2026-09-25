// CRDT state sits beside the change log as an independent, automatically
// mergeable state layer. A document's CRDT type is declared with its first
// operation batch — "counter" or "gset" (a grow-only set) — and never changes
// afterward; two batches declaring different types concurrently serialize in
// one transaction and exactly one takes effect.
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
	"bytes"
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
// the same device. Elements is unused.
//
// For a gset, Elements lists the strings the operation adds to the set and
// must be non-empty; Value is unused.
type CRDTOp struct {
	ID       string          // client-supplied stable id, unique per document
	DeviceID string          // originating device (shared by the whole batch)
	Value    json.RawMessage // counter: the device's accumulated contribution
	Elements []string        // gset: elements to add
}

// CRDTState is a document's merged CRDT state.
//
// Value holds the type-specific merged result: for a counter it decodes to the
// JSON integer sum of per-device maxima; for a gset it decodes to the sorted
// JSON array of every accepted element (an empty set is []).
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
`

// SubmitCRDTOps validates and commits one CRDT batch atomically.
//
// declaredType is the type the client asserts for the document
// ("counter" or "gset"). It fixes the type on the document's first batch;
// every later batch must declare the same type. Two batches declaring
// different types race in one serialized transaction, so exactly one wins and
// the other gets an *ErrCRDTConflict (409).
//
// The device is gated first (ErrDeviceNotFound / ErrPermissionDenied) before
// any CRDT content is read. Every operation is then resolved against the
// committed history: a repeated id is idempotent only with the same device and
// content, otherwise it is an *ErrCRDTConflict; a counter contribution below
// the device's current maximum is likewise an *ErrCRDTConflict. Any failure
// rejects the whole batch. Results come back in request order.
//
// The whole call runs under crdtMu, which also covers the post-commit
// fan-out: when the batch changed the merged value (including the batch that
// first gives the document a state), the new merged state is queued to every
// live CRDT subscription of the document, in commit order. Batches that leave
// the merged value untouched — idempotent repeats, equal counter
// contributions, re-added set elements — enqueue nothing, and rejected
// batches (which commit nothing) never reach the fan-out.
func (s *Store) SubmitCRDTOps(documentID, declaredType string, ops []CRDTOp) ([]CRDTResult, error) {
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
	hadState := false
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
		hadState = true
	}

	// Snapshot the merged value before the batch so the post-commit fan-out
	// fires only when the batch actually changed it.
	var before json.RawMessage
	if hadState {
		before, err = mergedCRDTValue(tx, documentID, docType)
		if err != nil {
			return nil, err
		}
	}

	results := make([]CRDTResult, len(ops))

	switch docType {
	case CRDTTypeCounter:
		if err := applyCounterOps(tx, documentID, ops, results); err != nil {
			return nil, err
		}
	case CRDTTypeGSet:
		if err := applyGSetOps(tx, documentID, ops, results); err != nil {
			return nil, err
		}
	}

	after, err := mergedCRDTValue(tx, documentID, docType)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	// The first accepted batch gives the document its state; any later batch
	// notifies only when the merged value actually moved.
	if !hadState || !bytes.Equal(before, after) {
		s.enqueueCRDTStateLocked(documentID, CRDTState{Type: docType, Value: after})
	}
	return results, nil
}

// applyCounterOps resolves and applies every counter operation inside tx. A
// repeated id is idempotent only with the same device and contribution. A new
// id with a contribution below the device's stored maximum regresses the
// counter and is rejected; an equal contribution is an accepted no-op; a
// larger one advances the device's maximum.
func applyCounterOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) error {
	for i, op := range ops {
		value, err := decodeCounterValue(op.Value)
		if err != nil {
			return &ErrCRDTConflict{ID: op.ID, Reason: err.Error()}
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
				return scanErr
			}
			if current.Valid && value < current.Int64 {
				return &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "counter contribution must not decrease",
				}
			}
			if _, err := tx.Exec(
				`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
				documentID, op.ID, op.DeviceID, []byte(op.Value),
			); err != nil {
				return err
			}
			if !current.Valid || value > current.Int64 {
				if _, err := tx.Exec(
					`INSERT INTO crdt_counter_values (document_id, device_id, value) VALUES (?, ?, ?)
					 ON CONFLICT (document_id, device_id) DO UPDATE SET value = excluded.value`,
					documentID, op.DeviceID, value,
				); err != nil {
					return err
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: true}
		case err != nil:
			return err
		default:
			if existingDevice != op.DeviceID || !jsonEqual(existingValue, op.Value) {
				return &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or value",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
		}
	}
	return nil
}

// applyGSetOps resolves and applies every grow-only-set operation inside tx.
// A repeated id is idempotent only with the same device and the same set of
// elements (element order is irrelevant to the merge). New elements are
// inserted into the union table; re-adding an element changes nothing.
func applyGSetOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) error {
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
			return err
		default:
			if existingDevice != op.DeviceID || !stringSetEqual(existingElements, op.Elements) {
				return &ErrCRDTConflict{
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
			return err
		}
		for _, element := range op.Elements {
			// The union table is an identity insert: a repeated element simply
			// exists once.
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_set_elements (document_id, element) VALUES (?, ?)`,
				documentID, element,
			); err != nil {
				return err
			}
		}
		results[i] = CRDTResult{ID: op.ID, Created: true}
	}
	return nil
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
// The merged value is derived from the per-device maxima (counter) or the
// union table (gset), not cached: it is exactly what any equivalent batch
// order would converge to.
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

	value, err := mergedCRDTValue(s.db, documentID, docType)
	if err != nil {
		return CRDTState{}, err
	}
	return CRDTState{Type: docType, Value: value}, nil
}

// sqlQuerier is the query surface shared by *sql.DB and *sql.Tx, so the
// merged value can be computed both for plain reads and inside the committing
// transaction (for the change-detection snapshot).
type sqlQuerier interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

// mergedCRDTValue computes the document's merged value: for a counter the
// JSON integer sum of per-device maxima; for a gset the sorted JSON array of
// every accepted element (an empty set is []). The encoding is canonical, so
// two values compare equal with bytes.Equal exactly when the merged states
// are equal.
func mergedCRDTValue(q sqlQuerier, documentID, docType string) (json.RawMessage, error) {
	switch docType {
	case CRDTTypeCounter:
		var total int64
		if err := q.QueryRow(
			`SELECT COALESCE(SUM(value), 0) FROM crdt_counter_values WHERE document_id = ?`,
			documentID,
		).Scan(&total); err != nil {
			return nil, err
		}
		return json.Marshal(total)
	case CRDTTypeGSet:
		rows, err := q.Query(
			`SELECT element FROM crdt_set_elements WHERE document_id = ? ORDER BY element ASC`,
			documentID,
		)
		if err != nil {
			return nil, err
		}
		elements := make([]string, 0)
		for rows.Next() {
			var element string
			if err := rows.Scan(&element); err != nil {
				_ = rows.Close()
				return nil, err
			}
			elements = append(elements, element)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
		return json.Marshal(elements)
	default:
		return nil, fmt.Errorf("unknown crdt type %q stored for document", docType)
	}
}
