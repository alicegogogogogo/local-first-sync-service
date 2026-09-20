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
