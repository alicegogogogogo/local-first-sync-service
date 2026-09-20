// Package store persists document change batches in a SQLite database.
//
// Within a document every change has a client-supplied id. New ids receive a
// monotonically increasing cursor; re-posting an existing id is idempotent
// only when the deviceId and decoded JSON payload match, otherwise it is a
// conflict. Valid batches commit atomically, so cursors never repeat and no
// record is partially written, even under concurrent writers.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	_ "modernc.org/sqlite"
)

// Change is one element of an inbound batch or one row of a listing.
type Change struct {
	ID       string          // client-supplied change id, unique per document
	DeviceID string          // originating device
	Payload  json.RawMessage // decoded JSON value, stored verbatim
}

// Result reports the outcome for one element of an accepted batch.
type Result struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
	Cursor  int64  `json:"cursor"`
}

// MergeOutcome is the result of a single MergeChange attempt.
type MergeOutcome string

const (
	// MergeApplied means the change was appended at the current cursor because
	// baseCursor equalled the document high-water mark.
	MergeApplied MergeOutcome = "applied"
	// MergeMerged means the change was appended even though baseCursor lagged,
	// because every intervening payload was a JSON object with no top-level
	// key colliding with the new payload.
	MergeMerged MergeOutcome = "merged"
	// MergeIdempotent means the id already existed with matching deviceId and
	// payload; nothing was written and the original result is returned.
	MergeIdempotent MergeOutcome = "idempotent"
)

// MergeResult reports the outcome of a MergeChange call.
type MergeResult struct {
	ID      string       `json:"id"`
	Outcome MergeOutcome `json:"outcome"`
	// Result carries the original result for an idempotent replay. It is nil
	// for applied/merged outcomes.
	Result *Result `json:"result,omitempty"`
	// Cursor is the cursor of the newly appended change for applied/merged
	// outcomes, or the original cursor for an idempotent replay.
	Cursor int64 `json:"cursor"`
}

// ErrMergeConflict reports that a merge was rejected: either an existing
// change id carried a different deviceId or payload, or a lagging baseCursor
// could not be merged because an intervening change's payload was not a JSON
// object or shared a top-level key with the new payload. Nothing is written.
type ErrMergeConflict struct {
	ID     string
	Reason string
}

func (e *ErrMergeConflict) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("merge conflict for change %q", e.ID)
	}
	return fmt.Sprintf("merge conflict for change %q: %s", e.ID, e.Reason)
}

// ErrInvalidCursor reports that baseCursor does not name a usable merge base:
// it is larger than the document's current cursor.
type ErrInvalidCursor struct {
	BaseCursor    int64
	CurrentCursor int64
}

func (e *ErrInvalidCursor) Error() string {
	return fmt.Sprintf("baseCursor %d is greater than current cursor %d", e.BaseCursor, e.CurrentCursor)
}

// ListedChange is one row in a ListChanges response.
type ListedChange struct {
	ID       string          `json:"id"`
	DeviceID string          `json:"deviceId"`
	Payload  json.RawMessage `json:"payload"`
	Cursor   int64           `json:"cursor"`
}

// ErrConflict reports that a batch references an existing change id with a
// different deviceId or payload. The whole batch is rejected.
type ErrConflict struct {
	ID string
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("conflicting change %q for document", e.ID)
}

// Store is the durable change log.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the SQLite database at path. The empty path
// opens an in-memory database useful for tests.
func Open(path string) (*Store, error) {
	if path == "" {
		// With a single pooled connection this is a private, per-instance
		// in-memory database (useful for tests).
		path = "file::memory:"
	}
	// _txlock=immediate makes writers acquire the write lock at BEGIN, so
	// concurrent batches serialize without SQLITE_BUSY deadlocks.
	dsn := path + "?_txlock=immediate&_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL&_foreign_keys=on"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection avoids cross-connection locking surprises; writes are
	// serialized by the immediate transaction anyway, and SQLite concurrency
	// for readers does not help correctness here.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS changes (
	document_id TEXT NOT NULL,
	cursor      INTEGER NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	payload     BLOB NOT NULL,
	PRIMARY KEY (document_id, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS changes_doc_cursor_idx
	ON changes(document_id, cursor);
`)
	return err
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// PostChanges validates and commits one batch atomically.
//
// Every change must have a non-empty id; duplicate ids within the batch are
// rejected by the caller before reaching this point. On conflict the returned
// error is *ErrConflict and nothing is written. Cursors are assigned in batch
// order to ids that do not yet exist. Results are returned in the same order
// as the input.
func (s *Store) PostChanges(documentID string, changes []Change) ([]Result, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	results := make([]Result, len(changes))

	// First pass: resolve every id against existing rows. A single mismatch
	// aborts the whole batch before any cursor is allocated.
	pending := make([]int, 0, len(changes))
	for i, c := range changes {
		var existingDevice string
		var existingPayload []byte
		var existingCursor int64
		err := tx.QueryRow(
			`SELECT cursor, device_id, payload FROM changes WHERE document_id = ? AND id = ?`,
			documentID, c.ID,
		).Scan(&existingCursor, &existingDevice, &existingPayload)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			pending = append(pending, i)
		case err != nil:
			return nil, err
		default:
			if existingDevice != c.DeviceID || !jsonEqual(existingPayload, c.Payload) {
				return nil, &ErrConflict{ID: c.ID}
			}
			results[i] = Result{
				ID:      c.ID,
				Created: false,
				Cursor:  existingCursor,
			}
		}
	}

	// Allocate cursors for genuinely new ids.
	var nextCursor int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) + 1 FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&nextCursor); err != nil {
		return nil, err
	}
	for _, i := range pending {
		c := changes[i]
		if _, err := tx.Exec(
			`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
			documentID, nextCursor, c.ID, c.DeviceID, []byte(c.Payload),
		); err != nil {
			return nil, err
		}
		results[i] = Result{
			ID:      c.ID,
			Created: true,
			Cursor:  nextCursor,
		}
		nextCursor++
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// MergeChange validates and appends one change under cursor-based merge
// semantics, all within a single serialized write transaction.
//
// The caller guarantees deviceID and id are non-empty and change.Payload is a
// JSON object; baseCursor is non-negative. Within the transaction:
//
//   - If the document is unknown, only baseCursor 0 is valid (a non-zero
//     baseCursor yields *ErrInvalidCursor). Otherwise baseCursor must not be
//     greater than the document's current high-water cursor.
//   - An existing id follows the original idempotency rule: matching
//     deviceId and semantically equal payload returns MergeIdempotent with the
//     original result; a mismatch yields *ErrMergeConflict.
//   - A new id with baseCursor == current cursor is appended (MergeApplied).
//   - A new id with baseCursor < current cursor is appended only when every
//     intervening change (cursor > baseCursor) carries a JSON object payload
//     whose top-level keys are disjoint from the new payload's keys
//     (MergeMerged); otherwise *ErrMergeConflict is returned.
//
// On any error nothing is written. Concurrent callers serialize on the
// immediate write transaction, so the cursor and key checks can never be
// bypassed by a racing commit.
func (s *Store) MergeChange(documentID, deviceID string, baseCursor int64, change Change) (*MergeResult, error) {
	if baseCursor < 0 {
		return nil, &ErrInvalidCursor{BaseCursor: baseCursor}
	}
	newFields, ok := JSONObject(change.Payload)
	if !ok {
		return nil, fmt.Errorf("merge payload must be a JSON object")
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Resolve the document high-water mark and whether the document exists.
	var current int64
	var knownCount int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0), COUNT(*) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&current, &knownCount); err != nil {
		return nil, err
	}
	known := knownCount > 0
	if (!known && baseCursor > 0) || baseCursor > current {
		return nil, &ErrInvalidCursor{BaseCursor: baseCursor, CurrentCursor: current}
	}

	// An existing id keeps the original idempotency/conflict semantics.
	var existingDevice string
	var existingPayload []byte
	var existingCursor int64
	err = tx.QueryRow(
		`SELECT cursor, device_id, payload FROM changes WHERE document_id = ? AND id = ?`,
		documentID, change.ID,
	).Scan(&existingCursor, &existingDevice, &existingPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// New id: fall through to the append checks.
	case err != nil:
		return nil, err
	default:
		if existingDevice != deviceID || !jsonEqual(existingPayload, change.Payload) {
			return nil, &ErrMergeConflict{ID: change.ID, Reason: "existing change has different deviceId or payload"}
		}
		original := &Result{ID: change.ID, Created: false, Cursor: existingCursor}
		return &MergeResult{ID: change.ID, Outcome: MergeIdempotent, Result: original, Cursor: existingCursor}, nil
	}

	outcome := MergeApplied
	if baseCursor < current {
		// Inspect every change committed after the client's base. All must be
		// JSON objects, and none may share a top-level key with the new
		// payload.
		rows, err := tx.Query(
			`SELECT id, payload FROM changes
			 WHERE document_id = ? AND cursor > ?
			 ORDER BY cursor ASC`,
			documentID, baseCursor,
		)
		if err != nil {
			return nil, err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var interveningID string
			var interveningPayload []byte
			if err := rows.Scan(&interveningID, &interveningPayload); err != nil {
				return nil, err
			}
			fields, isObject := JSONObject(interveningPayload)
			if !isObject {
				return nil, &ErrMergeConflict{
					ID:     change.ID,
					Reason: "intervening change " + interveningID + " payload is not a JSON object",
				}
			}
			for k := range newFields {
				if _, collision := fields[k]; collision {
					return nil, &ErrMergeConflict{
						ID:     change.ID,
						Reason: "top-level field " + strconv.Quote(k) + " already modified by intervening change " + interveningID,
					}
				}
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		outcome = MergeMerged
	}

	nextCursor := current + 1
	if _, err := tx.Exec(
		`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
		documentID, nextCursor, change.ID, deviceID, []byte(change.Payload),
	); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &MergeResult{ID: change.ID, Outcome: outcome, Cursor: nextCursor}, nil
}

// ListChanges returns at most limit changes for documentID whose cursor is
// greater than after, in cursor order, together with the cursor to pass as
// the next after value. An unknown document yields an empty list and cursor 0;
// a known document with no rows past after yields nextCursor == after.
func (s *Store) ListChanges(documentID string, after, limit int64) ([]ListedChange, int64, error) {
	rows, err := s.db.Query(
		`SELECT id, device_id, payload, cursor FROM changes
		 WHERE document_id = ? AND cursor > ?
		 ORDER BY cursor ASC
		 LIMIT ?`,
		documentID, after, limit,
	)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]ListedChange, 0)
	var maxCursor int64 = after
	for rows.Next() {
		var c ListedChange
		if err := rows.Scan(&c.ID, &c.DeviceID, &c.Payload, &c.Cursor); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
		maxCursor = c.Cursor
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// Distinguish an unknown document from a known one with nothing new:
	// unknown documents must report nextCursor 0.
	if len(out) == 0 {
		var known bool
		if err := s.db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)`,
			documentID,
		).Scan(&known); err != nil {
			return nil, 0, err
		}
		if !known {
			return out, 0, nil
		}
	}
	return out, maxCursor, nil
}

// JSONObject decodes payload and reports the top-level keys when it is a JSON
// object, or ok=false for any other JSON value (array, scalar, null).
func JSONObject(payload json.RawMessage) (keys map[string]struct{}, ok bool) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload, &m); err != nil || m == nil {
		// m == nil rejects a JSON null (which decodes without error but leaves
		// the map nil); arrays and scalars fail the type check.
		return nil, false
	}
	keys = make(map[string]struct{}, len(m))
	for k := range m {
		keys[k] = struct{}{}
	}
	return keys, true
}

// jsonEqual reports whether two payloads are equal after JSON decoding, so
// 1 and 1.0, or {"a":1,"b":2} and {"b":2,"a":1}, compare equal.
func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return deepEqual(va, vb)
}

func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			w, exists := bv[k]
			if !exists || !deepEqual(v, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
