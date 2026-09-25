package crdtstate

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// applyCounterOps resolves and applies every counter operation inside tx. A
// repeated id is idempotent only with the same device and contribution. A new
// id with a contribution below the device's stored maximum regresses the
// counter and is rejected; an equal contribution is an accepted no-op; a
// larger one advances the device's maximum. It reports whether the merged
// state actually changed — only an advancing contribution moves the sum.
func applyCounterOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	changed := false
	for i, op := range ops {
		value, err := decodeCounterValue(op.Value)
		if err != nil {
			return false, &ErrConflict{ID: op.ID, Reason: err.Error()}
		}

		var existingDevice string
		var existingValue []byte
		err = tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The merge row may have been trimmed by compaction; the retained
			// identity still decides between idempotent replay and conflict.
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeCounter,
				"operation id already exists with a different device or value", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id: enforce monotonicity against this device's maximum.
			var current sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT value FROM crdt_counter_values WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&current); scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
				return false, scanErr
			}
			if current.Valid && value < current.Int64 {
				return false, &ErrConflict{
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
			results[i] = Result{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !jsonEqual(existingValue, op.Value) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or value",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
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
func applyGSetOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
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
			// The operation row covering the union may have been trimmed; the
			// retained identity still decides replay versus conflict.
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeGSet,
				"operation id already exists with a different device or elements", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id; insert the op and its elements below.
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !stringSetEqual(existingElements, op.Elements) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or elements",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
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
		results[i] = Result{ID: op.ID, Created: true}
	}
	return changed, nil
}

// applyRegisterOps resolves and applies every register operation inside tx.
// A repeated id is idempotent only with the same device, version and value. A
// new id whose version is not strictly greater than every version already
// accepted from the same device regresses (or stalls) the device's clock and
// is rejected. It reports whether the merged state actually changed — only a
// change of the winning operation's value moves it.
func applyRegisterOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
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
			return false, &ErrConflict{ID: op.ID, Reason: "register version must be a non-negative integer"}
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
			// An older per-device version may have been trimmed; the retained
			// identity still decides replay versus conflict before the version
			// monotonicity gate, which only applies to an id never accepted.
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeRegister,
				"operation id already exists with a different device, version or value", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id: the device's versions only move strictly forward. The
			// per-device maximum is derived from the retained operations, so
			// the decision survives compaction (which keeps each device's
			// latest) and restarts without extra state.
			var maxVersion sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT MAX(version) FROM crdt_register_ops WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&maxVersion); scanErr != nil {
				return false, scanErr
			}
			if maxVersion.Valid && op.Version <= maxVersion.Int64 {
				return false, &ErrConflict{
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
			results[i] = Result{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || existingVersion != op.Version || !jsonEqual(existingValue, op.Value) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, version or value",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
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
// inside tx. A repeated id is idempotent only with the same device, action
// and element; an idempotent replay applies nothing, so a replayed remove
// never tombstones adds that landed after its first acceptance. An add tags
// the element with its own operation id; a remove tombstones exactly the tags
// already accepted for the element — adds that arrive later carry fresh tags
// and are unaffected, and removing an element with no live (or no) tags is an
// accepted no-op. It reports whether the merged state actually changed — only
// a change in the set of elements with at least one live tag moves it.
func applyORSetOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	// The merged element set before the batch, so a batch that leaves it
	// untouched (an idempotent replay, an add of an already-present element,
	// a remove with nothing observed) reports no change and notifies nobody.
	prev, err := queryORSetElements(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Action != ORSetAdd && op.Action != ORSetRemove {
			// The API boundary rejects unknown actions; one here means the
			// caller skipped validation.
			return false, &ErrConflict{ID: op.ID, Reason: `orset action must be "add" or "remove"`}
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
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, action or element",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
			continue
		}

		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, op.DeviceID, []byte(encodeORSetContent(op.Action, op.Element)),
		); err != nil {
			return false, err
		}
		if op.Action == ORSetAdd {
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
		results[i] = Result{ID: op.ID, Created: true}
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
