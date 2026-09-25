// Package events is the change event service: the durable per-document change
// log together with everything written on top of it.
//
// Within a document every change has a client-supplied id. New ids receive a
// monotonically increasing cursor; re-posting an existing id is idempotent
// only when the deviceId and decoded JSON payload match, otherwise it is a
// conflict. Valid batches commit atomically in one serialized transaction, so
// cursors never repeat and no record is partially written, even under
// concurrent writers. Ordinary posts, offline replays, merges and snapshot
// restores all allocate from the same per-document contiguous cursor space.
//
// A document may also carry snapshots: caller-supplied JSON states pinned to
// existing change cursors. Snapshots are write-once per cursor — a matching
// re-post is idempotent, a differing one a conflict — and live apart from the
// change log. A restore appends an ordinary change whose payload is a
// snapshot's state, recording its provenance so a repeated restore after a
// restart keeps the same idempotency/conflict decisions.
//
// Reads page by cursor in ascending order. The session view and the document
// view are the same read; the HTTP layer adds the session/permission checks.
// Long polling and push subscriptions are in-memory observers of commits:
// they register before the confirming read, hold no durable state, and are
// woken only after a change-bearing transaction has committed.
//
// The service owns only its own tables. The registration and permission
// layers are reached through its Gate interface, so a replay or merge never
// reaches into another service's storage.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrStoreClosing reports that the service is shutting down and parked long
// polls have been woken so they can return without writing anything. The
// caller maps it to 503.
var ErrStoreClosing = errors.New("store is closing")

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

// ErrConflict reports that a batch references an existing change id with a
// different deviceId or payload. The whole batch is rejected.
type ErrConflict struct {
	ID string
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("conflicting change %q for document", e.ID)
}

// ErrStaleCursor reports that a merge targets a base cursor for an unknown
// document, or a base cursor greater than the document's current cursor. The
// caller maps it to 400; nothing is written.
var ErrStaleCursor = errors.New("baseCursor is unknown or ahead of the current cursor")

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

// ErrRestoreConflict reports that a restore references a change id that is
// already taken by an ordinary change, or by a prior restore whose deviceId,
// snapshot cursor or source state differs. Nothing is written; the caller maps
// it to 409.
type ErrRestoreConflict struct {
	ID string
}

func (e *ErrRestoreConflict) Error() string {
	return fmt.Sprintf("change id %q conflicts with the change log", e.ID)
}

// RestoreResult reports the outcome of an accepted RestoreSnapshot.
type RestoreResult struct {
	ID           string `json:"id"`
	Created      bool   `json:"created"`
	Cursor       int64  `json:"cursor"`
	RestoredFrom int64  `json:"restoredFrom"`
}

// schema is created when the service is constructed. Table and index shapes
// are unchanged from the pre-refactor layout, so data already on disk stays
// readable.
const schema = `
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
CREATE TABLE IF NOT EXISTS restores (
	document_id     TEXT NOT NULL,
	change_id       TEXT NOT NULL,
	device_id       TEXT NOT NULL,
	snapshot_cursor INTEGER NOT NULL,
	change_cursor   INTEGER NOT NULL,
	state           BLOB NOT NULL,
	PRIMARY KEY (document_id, change_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS restores_doc_cursor_idx
	ON restores(document_id, change_cursor);
`
