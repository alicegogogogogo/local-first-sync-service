// CRDT state sits beside the change log as an independent, automatically
// mergeable state layer. A document's CRDT type is declared with its first
// operation batch — "counter", "gset" (a grow-only set) or "register" (a
// last-writer-wins register) — and never changes afterward; two batches
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
// A register operation carries an arbitrary JSON value (null, number, string,
// array or object, stored verbatim) and a non-negative integer version. A
// device's versions only move strictly forward (a regressing or equal version
// is an ErrCRDTConflict and changes nothing), and the merged state is the
// value of the operation with the greatest version, ties broken by the
// lexicographically smaller operation id — a last-writer-wins register whose
// merge is commutative, associative and idempotent, so every reader converges
// to the same value regardless of arrival order.
//
// An orset (observed-remove set) operation either adds one element or removes
// one. Every add introduces a fresh tag (its own operation id) for the
// element; a remove tombstones exactly the tags it observes — the adds
// accepted before it — and leaves later adds untouched, so a remove concurrent
// with an add never deletes it. Removing an element that was never added (or
// is already fully removed) is an accepted no-op. The merged state is the set
// of elements with at least one live tag, presented in ascending order; the
// merge is commutative, associative and idempotent, so every reader converges
// to the same set regardless of arrival order.
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
//
// Compaction bounds the state layer's growth: operations and tombstones that
// no longer participate in the merge are deleted in one serialized
// transaction, leaving the merged state byte-for-byte identical and the
// idempotency/conflict decisions driven by the retained records unchanged. A
// snapshot read reports the merged state together with the retained operation
// and tombstone counts; both surfaces are durable across restarts.

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
	// the value of the operation with the greatest version, ties broken by the
	// lexicographically smaller operation id.
	CRDTTypeRegister = "register"
	// CRDTTypeORSet is the observed-remove set: the merged value is the set of
	// elements with at least one add not tombstoned by an observed remove.
	CRDTTypeORSet = "orset"
)

// OR-Set operation actions.
const (
	// CRDTORSetAdd adds the operation's element with a fresh tag.
	CRDTORSetAdd = "add"
	// CRDTORSetRemove tombstones every add tag for the element that the
	// operation observes.
	CRDTORSetRemove = "remove"
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
// For a register, Value is an arbitrary JSON value stored verbatim and
// Version is a non-negative integer that must be strictly greater than every
// version the same device has already had accepted. Elements is unused.
//
// For an orset, Action is "add" or "remove" and Element is the non-empty
// string the operation adds or removes. Value, Elements and Version are
// unused.
type CRDTOp struct {
	ID       string          // client-supplied stable id, unique per document
	DeviceID string          // originating device (shared by the whole batch)
	Value    json.RawMessage // counter: accumulated contribution; register: the value
	Elements []string        // gset: elements to add
	Version  int64           // register: logical version, per-device strictly increasing
	Action   string          // orset: CRDTORSetAdd or CRDTORSetRemove
	Element  string          // orset: the element the action applies to
}

// CRDTState is a document's merged CRDT state.
//
// Value holds the type-specific merged result: for a counter it decodes to the
// JSON integer sum of per-device maxima; for a gset it decodes to the sorted
// JSON array of every accepted element (an empty set is []); for a register it
// is the winning operation's JSON value, exactly as submitted; for an orset it
// decodes to the sorted JSON array of the elements with at least one live add
// (an empty set is []).
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
`

// SubmitCRDTOps validates and commits one CRDT batch atomically.
//
// declaredType is the type the client asserts for the document
// ("counter", "gset", "register" or "orset"). It fixes the type on the document's
// first batch; every later batch must declare the same type. Two batches
// declaring different types race in one serialized transaction, so exactly
// one wins and the other gets an *ErrCRDTConflict (409).
//
// The device is gated first (ErrDeviceNotFound / ErrPermissionDenied) before
// any CRDT content is read. Every operation is then resolved against the
// committed history: a repeated id is idempotent only with the same device and
// content, otherwise it is an *ErrCRDTConflict; a counter contribution below
// the device's current maximum, or a register version not strictly greater
// than the device's accepted versions, is likewise an *ErrCRDTConflict. Any
// failure rejects the whole batch. Results come back in request order.
//
// When the committed batch actually moved the merged state (a counter maximum
// advanced, the set union grew, the register's winning value changed or the
// orset's live element set changed) the document's CRDT state subscribers are
// notified with the new merged state after the commit; idempotent repeats,
// rejected batches and no-op additions or removals change nothing and notify
// nobody.
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
	if err := gateCRDTDevice(tx, documentID, deviceID); err != nil {
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
	case CRDTTypeORSet:
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

// applyRegisterOps resolves and applies every register operation inside tx. A
// repeated id is idempotent only with the same device, version and value. A
// new id whose version is not strictly greater than every version already
// accepted from the same device regresses (or stalls) the device's clock and
// is rejected. It reports whether the merged state actually changed — only a
// change of the winning operation's value moves it.
func applyRegisterOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) (bool, error) {
	// The merged value before the batch, so a batch that leaves the winner's
	// value untouched (an idempotent replay, or a new op that loses the
	// last-writer-wins comparison) reports no change and notifies nobody.
	prev, prevOK, err := registerWinner(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Version < 0 {
			// The API boundary rejects negative versions; a negative version
			// here means the caller skipped validation.
			return false, &ErrCRDTConflict{ID: op.ID, Reason: "register version must be a non-negative integer"}
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
			// New id: the device's versions only move strictly forward. The
			// per-device maximum is derived from the accepted ops, so the
			// decision is durable across restarts without extra state.
			var maxVersion sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT MAX(version) FROM crdt_register_ops WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&maxVersion); scanErr != nil {
				return false, scanErr
			}
			if maxVersion.Valid && op.Version <= maxVersion.Int64 {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "register version must be greater than the device's previously accepted version",
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
					Reason: "operation id already exists with a different device, version or value",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
		}
	}

	next, nextOK, err := registerWinner(tx, documentID)
	if err != nil {
		return false, err
	}
	// The merge is the winner's value; a new winner carrying a JSON-equal
	// value does not move the state.
	changed := prevOK != nextOK || (prevOK && nextOK && !jsonEqual(prev, next))
	return changed, nil
}

// registerWinner returns the value the register currently merges to: the
// value of the accepted operation with the greatest version, ties broken by
// the lexicographically smaller operation id. It reports false when the
// document has no accepted register operation yet.
func registerWinner(tx *sql.Tx, documentID string) (json.RawMessage, bool, error) {
	var value []byte
	err := tx.QueryRow(
		`SELECT value FROM crdt_register_ops WHERE document_id = ?
		 ORDER BY version DESC, id ASC LIMIT 1`,
		documentID,
	).Scan(&value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	default:
		return json.RawMessage(value), true, nil
	}
}

// applyORSetOps resolves and applies every observed-remove-set operation
// inside tx. A repeated id is idempotent only with the same device, action and
// element; an idempotent replay applies nothing, so a replayed remove never
// tombstones adds that landed after its first acceptance. An add tags the
// element with its own operation id; a remove tombstones exactly the tags
// already accepted for the element — adds that arrive later carry fresh tags
// and are unaffected, and removing an element with no live (or no) tags is an
// accepted no-op. It reports whether the merged state actually changed — only
// a change in the set of elements with at least one live tag moves it.
func applyORSetOps(tx *sql.Tx, documentID string, ops []CRDTOp, results []CRDTResult) (bool, error) {
	// The merged element set before the batch, so a batch that leaves it
	// untouched (an idempotent replay, an add of an already-present element,
	// a remove with nothing observed) reports no change and notifies nobody.
	prev, err := queryORSetElements(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Action != CRDTORSetAdd && op.Action != CRDTORSetRemove {
			// The API boundary rejects unknown actions; one here means the
			// caller skipped validation.
			return false, &ErrCRDTConflict{ID: op.ID, Reason: `orset action must be "add" or "remove"`}
		}

		var existingDevice string
		var existingContent []byte
		err := tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingContent)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// New id; insert the op and apply its effect below.
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !jsonEqual(existingContent, encodeORSetContent(op.Action, op.Element)) {
				return false, &ErrCRDTConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, action or element",
				}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
			continue
		}

		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, op.DeviceID, []byte(encodeORSetContent(op.Action, op.Element)),
		); err != nil {
			return false, err
		}
		if op.Action == CRDTORSetAdd {
			// The tag is the operation id itself: unique per document, so the
			// insert always lands and concurrent adds of the same element
			// accumulate independent tags.
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_orset_tags (document_id, element, op_id) VALUES (?, ?, ?)`,
				documentID, op.Element, op.ID,
			); err != nil {
				return false, err
			}
		} else {
			// Tombstone exactly the tags this remove observes: the adds
			// accepted before it. Tags created later are not in the table yet
			// and survive; already-tombstoned tags are an identity insert.
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_orset_tombstones (document_id, element, op_id, removed_by)
				 SELECT ?, ?, op_id, ? FROM crdt_orset_tags
				 WHERE document_id = ? AND element = ?`,
				documentID, op.Element, op.ID, documentID, op.Element,
			); err != nil {
				return false, err
			}
		}
		results[i] = CRDTResult{ID: op.ID, Created: true}
	}

	next, err := queryORSetElements(tx, documentID)
	if err != nil {
		return false, err
	}
	return !stringSliceEqual(prev, next), nil
}

// orsetLiveElementsSQL selects the elements with at least one add tag no
// tombstone covers, in ascending order — the orset merge, derived from the
// accepted operations rather than cached.
const orsetLiveElementsSQL = `
SELECT DISTINCT element FROM crdt_orset_tags t
WHERE t.document_id = ? AND NOT EXISTS (
	SELECT 1 FROM crdt_orset_tombstones x
	WHERE x.document_id = t.document_id
	  AND x.element = t.element
	  AND x.op_id = t.op_id
)
ORDER BY element ASC`

// orsetQuerier is satisfied by both *sql.Tx (inside a submission) and *sql.DB
// (a state read), so the merge query exists exactly once.
type orsetQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// queryORSetElements returns the document's live orset elements in ascending
// order. The empty set is an empty slice, never nil, so it marshals as [].
func queryORSetElements(q orsetQuerier, documentID string) ([]string, error) {
	rows, err := q.Query(orsetLiveElementsSQL, documentID)
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
	return elements, nil
}

// encodeORSetContent renders an orset operation's content (its action and
// element) as a JSON object for durable storage, so a later submission of the
// same id can be compared against the original content.
func encodeORSetContent(action, element string) json.RawMessage {
	raw, err := json.Marshal(struct {
		Action  string `json:"action"`
		Element string `json:"element"`
	}{Action: action, Element: element})
	if err != nil {
		// Strings always marshal.
		return json.RawMessage(`{"action":"","element":""}`)
	}
	return raw
}

// stringSliceEqual reports whether a and b hold the same strings in the same
// order. Both orset element lists are ascending, so order-sensitive equality
// is set equality.
func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
// table (gset), the version/id ordering of the accepted operations (register)
// or the live tags minus tombstones (orset), not cached: it is exactly what
// any equivalent batch order would converge to.
func (s *Store) GetCRDTState(documentID string) (CRDTState, error) {
	return getCRDTState(s.db, documentID)
}

// crdtQuerier is satisfied by both *sql.DB (a bare state read) and *sql.Tx
// (a read inside a submission, compaction or snapshot transaction), so the
// merge and the type lookup exist exactly once.
type crdtQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// gateCRDTDevice enforces the registration/permission layer shared by every
// CRDT write path: an unregistered device yields ErrDeviceNotFound and a
// revoked one ErrPermissionDenied, before any CRDT content is observed.
func gateCRDTDevice(tx *sql.Tx, documentID, deviceID string) error {
	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return err
	}
	if !deviceExists {
		return ErrDeviceNotFound
	}
	var storedAuth int
	err := tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&storedAuth)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
		return nil
	case err != nil:
		return err
	default:
		if storedAuth == 0 {
			return ErrPermissionDenied
		}
		return nil
	}
}

// crdtDocType returns the document's fixed CRDT type, or ErrCRDTNotFound when
// no operation has ever been committed for it.
func crdtDocType(q crdtQuerier, documentID string) (string, error) {
	var docType string
	err := q.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", ErrCRDTNotFound
	case err != nil:
		return "", err
	default:
		return docType, nil
	}
}

// getCRDTState is the querier-based core of GetCRDTState.
func getCRDTState(q crdtQuerier, documentID string) (CRDTState, error) {
	docType, err := crdtDocType(q, documentID)
	if err != nil {
		return CRDTState{}, err
	}

	switch docType {
	case CRDTTypeCounter:
		var total int64
		if err := q.QueryRow(
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
		rows, err := q.Query(
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
		// A document's type row commits together with its first batch, so a
		// register document always has at least one accepted operation.
		var value []byte
		if err := q.QueryRow(
			`SELECT value FROM crdt_register_ops WHERE document_id = ?
			 ORDER BY version DESC, id ASC LIMIT 1`,
			documentID,
		).Scan(&value); err != nil {
			return CRDTState{}, err
		}
		return CRDTState{Type: docType, Value: json.RawMessage(value)}, nil
	case CRDTTypeORSet:
		elements, err := queryORSetElements(q, documentID)
		if err != nil {
			return CRDTState{}, err
		}
		raw, err := json.Marshal(elements)
		if err != nil {
			return CRDTState{}, err
		}
		return CRDTState{Type: docType, Value: raw}, nil
	default:
		return CRDTState{}, fmt.Errorf("unknown crdt type %q stored for document", docType)
	}
}

// CRDTSnapshot is a document's merged CRDT state together with the storage
// footprint of the state layer. Operations is the number of operation records
// still participating in the merge (after any compaction); Tombstones is the
// number of retained orset tombstones and is always zero for the other types.
// Both are non-negative.
type CRDTSnapshot struct {
	Type       string
	Value      json.RawMessage
	Operations int64
	Tombstones int64
}

// GetCRDTSnapshot returns the document's merged state plus its current
// operation/tombstone counts, read in one transaction so the two never come
// from different points in time. A document with no committed operations
// yields ErrCRDTNotFound (the caller answers 404). The read is side-effect
// free: it touches neither the change log nor any cursor or subscription.
func (s *Store) GetCRDTSnapshot(documentID string) (CRDTSnapshot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return CRDTSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()

	state, err := getCRDTState(tx, documentID)
	if err != nil {
		return CRDTSnapshot{}, err
	}
	operations, tombstones, err := crdtCounts(tx, documentID, state.Type)
	if err != nil {
		return CRDTSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return CRDTSnapshot{}, err
	}
	return CRDTSnapshot{
		Type:       state.Type,
		Value:      state.Value,
		Operations: operations,
		Tombstones: tombstones,
	}, nil
}

// CompactCRDT trims the CRDT state layer of documentID: operation records and
// tombstones that no longer participate in the merge are deleted, so a
// long-lived document stops accumulating storage it will never consult again.
//
//   - counter: an operation whose contribution is below its device's current
//     maximum participates in nothing — the merge reads only the per-device
//     maxima — so it is dropped; the operations carrying each device's maximum
//     stay, and monotonicity is still enforced against the retained maxima.
//   - gset: an operation whose elements are all also added by other retained
//     operations cannot move the union, so it is dropped; the union table is
//     untouched.
//   - register: only each device's highest-version operation still decides
//     anything (the merge winner and the strictly-forward version check), so
//     dominated operations are dropped.
//   - orset: a tombstoned tag is dead forever (its operation id is taken and
//     an idempotent replay never re-inserts it), so the tag and every
//     tombstone covering it are dropped; live tags and all operation records
//     stay, so observed-remove semantics are unchanged.
//
// The merged state before and after compaction is byte-for-byte identical,
// and the idempotency/conflict decisions driven by the retained records are
// unchanged. The device is gated exactly like a submission (ErrDeviceNotFound
// / ErrPermissionDenied, before any CRDT content is observed); a document with
// no committed operations yields ErrCRDTNotFound. Compaction runs in the same
// serialized transaction discipline as submissions, commits durably, is
// idempotent (a repeated compact trims nothing further and reports the same
// counts), never touches the change log or its cursors, and — since the
// merged value never moves — notifies no subscriber.
func (s *Store) CompactCRDT(documentID, deviceID string) (CRDTSnapshot, error) {
	// Serialize with submissions and other compactions, so a concurrent batch
	// either commits entirely before the trim (and is subject to it) or
	// entirely after (and is judged against the trimmed state).
	s.crdtMu.Lock()
	defer s.crdtMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return CRDTSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateCRDTDevice(tx, documentID, deviceID); err != nil {
		return CRDTSnapshot{}, err
	}
	docType, err := crdtDocType(tx, documentID)
	if err != nil {
		return CRDTSnapshot{}, err
	}

	switch docType {
	case CRDTTypeCounter:
		err = compactCounterOps(tx, documentID)
	case CRDTTypeGSet:
		err = compactGSetOps(tx, documentID)
	case CRDTTypeRegister:
		err = compactRegisterOps(tx, documentID)
	case CRDTTypeORSet:
		err = compactORSetState(tx, documentID)
	}
	if err != nil {
		return CRDTSnapshot{}, err
	}

	// The response is derived inside the same transaction, so it is exactly
	// what a snapshot read immediately after the commit reports.
	state, err := getCRDTState(tx, documentID)
	if err != nil {
		return CRDTSnapshot{}, err
	}
	operations, tombstones, err := crdtCounts(tx, documentID, docType)
	if err != nil {
		return CRDTSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return CRDTSnapshot{}, err
	}
	return CRDTSnapshot{
		Type:       state.Type,
		Value:      state.Value,
		Operations: operations,
		Tombstones: tombstones,
	}, nil
}

// crdtCounts reports the state layer's storage footprint for the document:
// the number of retained operation records and, for an orset, the number of
// retained tombstones (zero for every other type).
func crdtCounts(q crdtQuerier, documentID, docType string) (operations, tombstones int64, err error) {
	opTable := "crdt_ops"
	if docType == CRDTTypeRegister {
		opTable = "crdt_register_ops"
	}
	if err := q.QueryRow(
		`SELECT COUNT(*) FROM `+opTable+` WHERE document_id = ?`, documentID,
	).Scan(&operations); err != nil {
		return 0, 0, err
	}
	if docType == CRDTTypeORSet {
		if err := q.QueryRow(
			`SELECT COUNT(*) FROM crdt_orset_tombstones WHERE document_id = ?`, documentID,
		).Scan(&tombstones); err != nil {
			return 0, 0, err
		}
	}
	return operations, tombstones, nil
}

// compactCounterOps drops every counter operation whose contribution is
// strictly below its device's current maximum. The merge sums the per-device
// maxima, so a dominated operation participates in nothing; the operations
// carrying each device's maximum stay and keep their idempotency.
func compactCounterOps(tx *sql.Tx, documentID string) error {
	_, err := tx.Exec(
		`DELETE FROM crdt_ops
		 WHERE document_id = ?
		   AND CAST(value AS INTEGER) < (
		       SELECT value FROM crdt_counter_values
		       WHERE document_id = crdt_ops.document_id
		         AND device_id = crdt_ops.device_id)`,
		documentID,
	)
	return err
}

// compactGSetOps drops every grow-only-set operation whose elements are all
// also added by other retained operations: removing it cannot move the union.
// Coverage is evaluated greedily in operation-id order, so the result is
// deterministic and every element keeps at least one contributing operation.
func compactGSetOps(tx *sql.Tx, documentID string) error {
	rows, err := tx.Query(
		`SELECT id, value FROM crdt_ops WHERE document_id = ? ORDER BY id ASC`,
		documentID,
	)
	if err != nil {
		return err
	}
	type gsetOp struct {
		id       string
		elements []string
	}
	var ops []gsetOp
	for rows.Next() {
		var op gsetOp
		var raw []byte
		if err := rows.Scan(&op.id, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		if err := json.Unmarshal(raw, &op.elements); err != nil {
			_ = rows.Close()
			return err
		}
		ops = append(ops, op)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	// coverage[e] counts the retained operations adding e; an operation is
	// redundant when every element it adds is also added by at least one
	// other retained operation.
	coverage := make(map[string]int)
	for _, op := range ops {
		for _, element := range uniqueStrings(op.elements) {
			coverage[element]++
		}
	}
	for _, op := range ops {
		elements := uniqueStrings(op.elements)
		redundant := len(elements) > 0
		for _, element := range elements {
			if coverage[element] < 2 {
				redundant = false
				break
			}
		}
		if !redundant {
			continue
		}
		for _, element := range elements {
			coverage[element]--
		}
		if _, err := tx.Exec(
			`DELETE FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.id,
		); err != nil {
			return err
		}
	}
	return nil
}

// uniqueStrings returns the distinct strings of in, in first-seen order.
func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// compactRegisterOps drops every register operation that is not its device's
// highest-version one. The merge winner is a per-device maximum, and the
// strictly-forward version check is enforced against the per-device maxima,
// so a dominated operation participates in neither.
func compactRegisterOps(tx *sql.Tx, documentID string) error {
	_, err := tx.Exec(
		`DELETE FROM crdt_register_ops
		 WHERE document_id = ?
		   AND version < (
		       SELECT MAX(version) FROM crdt_register_ops r
		       WHERE r.document_id = crdt_register_ops.document_id
		         AND r.device_id = crdt_register_ops.device_id)`,
		documentID,
	)
	return err
}

// compactORSetState drops every tombstoned tag together with the tombstones
// covering it. A tombstoned tag is dead forever — its operation id is taken,
// so nothing can resurrect it — and removing a dead tag and its tombstones
// leaves the live element set untouched. Operation records stay, so a replayed
// add or remove remains idempotent and a later remove still tombstones exactly
// the tags it observes.
func compactORSetState(tx *sql.Tx, documentID string) error {
	if _, err := tx.Exec(
		`DELETE FROM crdt_orset_tags
		 WHERE document_id = ?
		   AND EXISTS (
		       SELECT 1 FROM crdt_orset_tombstones x
		       WHERE x.document_id = crdt_orset_tags.document_id
		         AND x.element = crdt_orset_tags.element
		         AND x.op_id = crdt_orset_tags.op_id)`,
		documentID,
	); err != nil {
		return err
	}
	_, err := tx.Exec(
		`DELETE FROM crdt_orset_tombstones WHERE document_id = ?`,
		documentID,
	)
	return err
}
