// Package store persists document change batches in a SQLite database.
//
// Within a document every change has a client-supplied id. New ids receive a
// monotonically increasing cursor; re-posting an existing id is idempotent
// only when the deviceId and decoded JSON payload match, otherwise it is a
// conflict. Valid batches commit atomically, so cursors never repeat and no
// record is partially written, even under concurrent writers.
//
// A document may also carry snapshots: caller-supplied JSON states pinned to
// existing change cursors. Snapshots are write-once per cursor — a matching
// re-post is idempotent, a differing one a conflict — and live apart from the
// change log.
//
// A snapshot can be restored: RestoreSnapshot appends an ordinary change whose
// payload is the pinned state, tagged with the snapshot cursor it came from.
// The tag makes a repeated restore idempotent only when every input, including
// the source snapshot, matches.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// ErrStaleCursor reports that a merge targets a base cursor for an unknown
// document, or a base cursor greater than the document's current cursor. The
// caller maps it to 400; nothing is written.
var ErrStaleCursor = errors.New("baseCursor is unknown or ahead of the current cursor")

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

// ListedChange is one row in a ListChanges response.
type ListedChange struct {
	ID       string          `json:"id"`
	DeviceID string          `json:"deviceId"`
	Payload  json.RawMessage `json:"payload"`
	Cursor   int64           `json:"cursor"`
}

// MergeResult reports the outcome of an accepted MergeChange. Outcome is one
// of "idempotent", "applied" or "merged".
type MergeResult struct {
	ID      string  `json:"id"`
	Outcome string  `json:"outcome"`
	Cursor  int64   `json:"cursor"`
	Result  *Result `json:"result,omitempty"`
}

// RestoreResult reports the outcome of an accepted RestoreSnapshot. A repeated
// restore reports Created=false with the first call's cursor; RestoredFrom is
// always the snapshot cursor used as the payload source.
type RestoreResult struct {
	ID           string `json:"id"`
	Created      bool   `json:"created"`
	Cursor       int64  `json:"cursor"`
	RestoredFrom int64  `json:"restoredFrom"`
}

// ErrConflict reports that a batch references an existing change id with a
// different deviceId or payload. The whole batch is rejected.
type ErrConflict struct {
	ID string
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("conflicting change %q for document", e.ID)
}

// ErrSnapshotBase reports that a snapshot targets an unknown document or a
// cursor that is not an existing change cursor of the document. The caller
// maps it to 400; nothing is written.
var ErrSnapshotBase = errors.New("snapshot cursor is not an existing cursor of the document")

// ErrSnapshotNotFound reports that no snapshot exists for the document and
// cursor. The caller maps it to 404.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// ErrSnapshotConflict reports that a snapshot already exists for the document
// and cursor with a different state. The stored snapshot is left unchanged.
type ErrSnapshotConflict struct {
	Cursor int64
}

func (e *ErrSnapshotConflict) Error() string {
	return fmt.Sprintf("conflicting snapshot for cursor %d", e.Cursor)
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
CREATE TABLE IF NOT EXISTS snapshots (
	document_id TEXT NOT NULL,
	cursor      INTEGER NOT NULL,
	state       BLOB NOT NULL,
	PRIMARY KEY (document_id, cursor)
);
`)
	if err != nil {
		return err
	}
	// Databases created before history restore lack the provenance column;
	// add it in place. NULL marks ordinary changes posted via /changes or
	// /merge; a non-NULL value records the snapshot cursor a restore used.
	has, err := columnExists(s.db, "changes", "restored_from")
	if err != nil {
		return err
	}
	if !has {
		if _, err := s.db.Exec(`ALTER TABLE changes ADD COLUMN restored_from INTEGER`); err != nil {
			return err
		}
	}
	return nil
}

// columnExists reports whether table has a column named col.
func columnExists(db *sql.DB, table, col string) (bool, error) {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notNull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notNull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
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

// MergeChange validates and appends one change against a caller-observed base
// cursor, all within a single serialized transaction.
//
//   - An existing id is idempotent only when deviceId and the decoded payload
//     match (Outcome "idempotent", with the original Result); a mismatch is an
//     *ErrConflict and nothing is written.
//   - A new id with baseCursor equal to the current cursor is appended as
//     "applied".
//   - A new id with a base cursor behind the current one is appended as
//     "merged" only when every change after baseCursor carries a JSON object
//     payload whose top-level keys are disjoint from the new payload's; any
//     non-object later payload or shared key is an *ErrConflict.
//
// A non-zero baseCursor for an unknown document, or a baseCursor greater than
// the current cursor, returns ErrStaleCursor. An unknown document with
// baseCursor 0 accepts the first change as "applied".
func (s *Store) MergeChange(documentID string, baseCursor int64, c Change) (MergeResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return MergeResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Current cursor is the document's high-water mark, 0 when unknown.
	var current int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&current); err != nil {
		return MergeResult{}, err
	}
	if baseCursor > current || (baseCursor != 0 && current == 0) {
		return MergeResult{}, ErrStaleCursor
	}

	// An existing id keeps the original idempotency/conflict rules.
	var existingDevice string
	var existingPayload []byte
	var existingCursor int64
	err = tx.QueryRow(
		`SELECT cursor, device_id, payload FROM changes WHERE document_id = ? AND id = ?`,
		documentID, c.ID,
	).Scan(&existingCursor, &existingDevice, &existingPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// New id; fall through to append logic below.
	case err != nil:
		return MergeResult{}, err
	default:
		if existingDevice != c.DeviceID || !jsonEqual(existingPayload, c.Payload) {
			return MergeResult{}, &ErrConflict{ID: c.ID}
		}
		return MergeResult{
			ID:      c.ID,
			Outcome: "idempotent",
			Cursor:  existingCursor,
			Result: &Result{
				ID:      c.ID,
				Created: false,
				Cursor:  existingCursor,
			},
		}, nil
	}

	outcome := "applied"
	if baseCursor < current {
		// Merge is allowed only when every change after baseCursor is a JSON
		// object whose top-level keys do not collide with the new payload's.
		newKeys, err := objectKeys(c.Payload)
		if err != nil {
			if errors.Is(err, errNotObject) {
				return MergeResult{}, &ErrConflict{ID: c.ID}
			}
			return MergeResult{}, err
		}
		rows, err := tx.Query(
			`SELECT payload FROM changes
			 WHERE document_id = ? AND cursor > ?
			 ORDER BY cursor ASC`,
			documentID, baseCursor,
		)
		if err != nil {
			return MergeResult{}, err
		}
		for rows.Next() {
			var laterPayload []byte
			if err := rows.Scan(&laterPayload); err != nil {
				_ = rows.Close()
				return MergeResult{}, err
			}
			laterKeys, err := objectKeys(laterPayload)
			if err != nil {
				_ = rows.Close()
				if errors.Is(err, errNotObject) {
					return MergeResult{}, &ErrConflict{ID: c.ID}
				}
				return MergeResult{}, err
			}
			for k := range newKeys {
				if _, clash := laterKeys[k]; clash {
					_ = rows.Close()
					return MergeResult{}, &ErrConflict{ID: c.ID}
				}
			}
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return MergeResult{}, err
		}
		_ = rows.Close()
		outcome = "merged"
	}

	next := current + 1
	if _, err := tx.Exec(
		`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
		documentID, next, c.ID, c.DeviceID, []byte(c.Payload),
	); err != nil {
		return MergeResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return MergeResult{}, err
	}
	return MergeResult{ID: c.ID, Outcome: outcome, Cursor: next}, nil
}

// PutSnapshot stores state as the snapshot of documentID at cursor, creating
// it on first use and reporting whether this call created it.
//
// Only cursors of existing changes are valid snapshot points: an unknown
// document, a cursor below 1 or a cursor ahead of the document's current
// cursor yields ErrSnapshotBase and nothing is written. Re-posting a state
// that decodes equal to the stored one is idempotent (created=false); a
// different state is an *ErrSnapshotConflict and the stored snapshot is left
// unchanged. Snapshots are independent of the change log: they neither move
// the document's cursor nor affect merges.
func (s *Store) PutSnapshot(documentID string, cursor int64, state json.RawMessage) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var current int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&current); err != nil {
		return false, err
	}
	if current == 0 || cursor < 1 || cursor > current {
		return false, ErrSnapshotBase
	}

	var existing []byte
	err = tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO snapshots (document_id, cursor, state) VALUES (?, ?, ?)`,
			documentID, cursor, []byte(state),
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	default:
		if !jsonEqual(existing, state) {
			return false, &ErrSnapshotConflict{Cursor: cursor}
		}
		return false, nil
	}
}

// GetSnapshot returns the state stored for documentID at cursor. Any miss —
// unknown document, unknown cursor or absent snapshot — yields
// ErrSnapshotNotFound.
func (s *Store) GetSnapshot(documentID string, cursor int64) (json.RawMessage, error) {
	var state []byte
	err := s.db.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrSnapshotNotFound
	case err != nil:
		return nil, err
	default:
		return json.RawMessage(state), nil
	}
}

// RestoreSnapshot appends an ordinary change whose payload is the state of the
// document's snapshot at snapshotCursor, all within a single serialized
// transaction.
//
// The snapshot is resolved before any write: an unknown document or a cursor
// without a snapshot yields ErrSnapshotNotFound and nothing is written. On a
// hit the change is appended with the next cursor and its provenance is pinned
// to snapshotCursor; existing rows are untouched.
//
// Reposting the same change id is idempotent only when the stored row was
// produced by the same restore: deviceId, provenance (restoredFrom) and the
// source snapshot state must all match, in which case Created=false and the
// first cursor are returned. An id already held by an ordinary change, or by a
// restore from another snapshot (or whose source state has diverged), is an
// *ErrConflict and nothing is written.
func (s *Store) RestoreSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Resolve the snapshot inside the transaction so the state appended as the
	// payload is exactly the state identified by snapshotCursor.
	var state []byte
	err = tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, snapshotCursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RestoreResult{}, ErrSnapshotNotFound
	case err != nil:
		return RestoreResult{}, err
	}

	// Resolve an existing id, provenance included.
	var existingDevice string
	var existingPayload []byte
	var existingCursor int64
	var existingFrom sql.NullInt64
	err = tx.QueryRow(
		`SELECT cursor, device_id, payload, restored_from FROM changes WHERE document_id = ? AND id = ?`,
		documentID, changeID,
	).Scan(&existingCursor, &existingDevice, &existingPayload, &existingFrom)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// New id; fall through to append below.
	case err != nil:
		return RestoreResult{}, err
	default:
		// Idempotent only when the row is a restore from the same snapshot
		// with the same deviceId and source state. Ordinary rows (restored_from
		// IS NULL) never match a restore.
		if !existingFrom.Valid || existingFrom.Int64 != snapshotCursor ||
			existingDevice != deviceID || !jsonEqual(existingPayload, state) {
			return RestoreResult{}, &ErrConflict{ID: changeID}
		}
		return RestoreResult{
			ID:           changeID,
			Created:      false,
			Cursor:       existingCursor,
			RestoredFrom: snapshotCursor,
		}, nil
	}

	// Append the restore as an ordinary change carrying the snapshot state.
	var next int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) + 1 FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&next); err != nil {
		return RestoreResult{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO changes (document_id, cursor, id, device_id, payload, restored_from)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		documentID, next, changeID, deviceID, state, snapshotCursor,
	); err != nil {
		return RestoreResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return RestoreResult{}, err
	}
	return RestoreResult{
		ID:           changeID,
		Created:      true,
		Cursor:       next,
		RestoredFrom: snapshotCursor,
	}, nil
}

// errNotObject marks a payload that is not a JSON object.
var errNotObject = errors.New("payload is not a JSON object")

// objectKeys returns the top-level keys of a JSON object payload. A null,
// array or scalar payload yields errNotObject.
func objectKeys(raw json.RawMessage) (map[string]struct{}, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		// Valid JSON that is not an object (array, scalar, null) decodes with
		// a type error into a map; treat it and malformed JSON as non-object.
		return nil, errNotObject
	}
	if m == nil {
		return nil, errNotObject
	}
	keys := make(map[string]struct{}, len(m))
	for k := range m {
		keys[k] = struct{}{}
	}
	return keys, nil
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
