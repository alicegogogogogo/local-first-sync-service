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
// A restore appends an ordinary change whose payload is a snapshot's state.
// Restore provenance (the snapshot cursor the state came from) is persisted in
// its own table, so after restart a repeated restore is still distinguishable
// from an ordinary change and idempotency/conflict decisions are unchanged.
//
// Devices and sessions form the registration layer on top of which clients
// sync. A device registers with a client-supplied id; a session belongs to
// exactly one registered device and is addressed by its own client-supplied
// id. Registration and session creation are idempotent re-posts: the first
// call creates, an identical repeat does not. A session id already owned by
// another device is a conflict, never silently re-homed. Deletes are
// owner-scoped and hard: a repeat delete, another device, and a cross-device
// path all miss with 404, after which the id is free to be created again.
//
// Document permissions sit on top of registration: every (document, device)
// pair starts authorized, and a grant or revoke is recorded per pair. Writes
// are serialized in one transaction and committed to disk, so concurrent
// grant/revoke calls each land as a complete state and survive a restart.
// Revoking never deletes changes, snapshots or sessions; it only gates the
// session-scoped change listing.
//
// Attachments are resumable chunked uploads owned by the registered device
// that created them. Creation pins the declared total size, chunk size and
// SHA-256 digest; chunks land in any order and are stored durably as they
// arrive, so an interrupted upload resumes after a restart with its
// idempotency and conflict decisions intact. Finishing concatenates the
// chunks, verifies the digest and seals the upload; a sealed upload is
// immutable and rejects every further chunk write. Finished content is
// addressed by digest, so a second attachment with the same digest and size
// reuses the stored bytes instead of copying them.
//
// Long polling lets a caught-up client wait for subsequent changes. Waiters
// register per document before the confirming read, so a commit landing
// between that read and the park is never missed; every change-producing
// commit (batch, merge, restore, replay) signals the document's waiters after
// it commits. Wait state is in-memory only — a canceled disconnect or a
// closing store leaves no rows behind — while the changes themselves stay
// durable, so waiting and idempotency decisions are unchanged by restart.
//
// CRDT state is an auto-merging layer that lives entirely apart from the
// change log: its operations never take a document cursor and its reads never
// appear in changes/poll/subscribe traffic. A document's CRDT type is fixed on
// its very first accepted operation as either "counter" or "gset" and can
// never change; two concurrent type declarations serialize in one immediate
// transaction so exactly one wins and the loser observes the established type
// (ErrCRDTTypeConflict). Counter ops carry the device's cumulative
// contribution; the merged value is the sum over devices of each device's
// maximum, and a device's own value must move only upward (ErrCRDTRejected).
// G-set ops add one element each and merge as a set union; elements present in
// ascending order. Every op carries a stable id: an identical re-post (same
// type, device and value) is idempotent, while an existing id with a different
// device or value is a conflict (ErrCRDTOpConflict). Submits additionally go
// through the same registration/permission gate as replay: an unregistered
// device is ErrDeviceNotFound (404), a revoked one ErrPermissionDenied (403),
// neither ever exposing state.
//
// Replay is the offline retry of an ordinary batch: it shares the same
// per-document contiguous cursor space and the same serialized transaction as
// PostChanges, and additionally enforces the registration/permission layer
// (ErrDeviceNotFound, ErrPermissionDenied) before any change id is resolved,
// so its error responses never reveal change content.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ErrStoreClosing reports that the store is shutting down and parked long
// polls have been woken so they can return without writing anything. The
// caller maps it to 503.
var ErrStoreClosing = errors.New("store is closing")

// ErrPermissionDenied reports that a device's access to the document has been
// revoked. The caller maps it to 403; nothing is written and no change content
// is exposed.
var ErrPermissionDenied = errors.New("device permission for this document has been revoked")

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

// ErrDeviceNotFound reports that an operation targets a device id that was
// never registered. The caller maps it to 404; nothing is written.
var ErrDeviceNotFound = errors.New("device not found")

// ErrSessionConflict reports that a session id already belongs to a different
// device. The session keeps its original owner; nothing is written. The
// caller maps it to 409.
type ErrSessionConflict struct {
	ID string
}

func (e *ErrSessionConflict) Error() string {
	return fmt.Sprintf("session %q belongs to another device", e.ID)
}

// ErrSessionNotFound reports that no live session matches the (device,
// session) pair: the session was never created, was already deleted, or
// belongs to another device. The caller maps it to 404.
var ErrSessionNotFound = errors.New("session not found")

// Store is the durable change log.
type Store struct {
	db *sql.DB

	// pollMu guards pollWaits, subs, nextWaitID, nextSubID and closed. Parked
	// long polls wait on a per-document set of channels; a committed change to
	// a document closes (signals) every channel parked on it. WebSocket
	// subscriptions live in a second registry keyed by (document, device): a
	// commit signals every subscription under the document, while a permission
	// write signals only the matching device's subscriptions.
	pollMu     sync.Mutex
	pollWaits  map[string]map[uint64]chan struct{}
	subs       map[subKey]map[uint64]*subscription
	nextWaitID uint64
	nextSubID  uint64
	subWG      sync.WaitGroup
	closed     bool
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
	s := &Store{
		db:        db,
		pollWaits: make(map[string]map[uint64]chan struct{}),
		subs:      make(map[subKey]map[uint64]*subscription),
	}
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
CREATE TABLE IF NOT EXISTS devices (
	id TEXT NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS sessions (
	id        TEXT NOT NULL PRIMARY KEY,
	device_id TEXT NOT NULL REFERENCES devices(id)
);
CREATE INDEX IF NOT EXISTS sessions_device_idx
	ON sessions(device_id);
CREATE TABLE IF NOT EXISTS document_permissions (
	document_id TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	authorized  INTEGER NOT NULL,
	PRIMARY KEY (document_id, device_id)
);
CREATE TABLE IF NOT EXISTS attachments (
	id          TEXT NOT NULL PRIMARY KEY,
	device_id   TEXT NOT NULL REFERENCES devices(id),
	total_bytes INTEGER NOT NULL,
	chunk_size  INTEGER NOT NULL,
	sha256      TEXT NOT NULL,
	complete    INTEGER NOT NULL DEFAULT 0,
	reused      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS attachment_chunks (
	attachment_id TEXT NOT NULL REFERENCES attachments(id),
	idx           INTEGER NOT NULL,
	data          BLOB NOT NULL,
	PRIMARY KEY (attachment_id, idx)
);
CREATE TABLE IF NOT EXISTS attachment_contents (
	sha256 TEXT NOT NULL PRIMARY KEY,
	size   INTEGER NOT NULL,
	data   BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS crdt_documents (
	document_id TEXT NOT NULL PRIMARY KEY,
	type        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS crdt_ops (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	value       TEXT NOT NULL,
	PRIMARY KEY (document_id, id)
);
CREATE INDEX IF NOT EXISTS crdt_ops_doc_device_idx
	ON crdt_ops(document_id, device_id);
`)
	return err
}

// InterruptWaits wakes every parked long poll without closing the database,
// so an orderly server shutdown drains waiting connections immediately
// (they answer 503) instead of holding Shutdown hostage until their wait
// deadline. Committed state is untouched.
func (s *Store) InterruptWaits() {
	s.pollMu.Lock()
	s.closed = true
	waits := s.pollWaits
	s.pollWaits = map[string]map[uint64]chan struct{}{}
	// Snapshot the live subscription channels without reaping the registry:
	// unregister remains valid while handlers drain, so the WaitGroup in
	// Close balances. New AddSubscription calls fail fast on closed.
	var subChans []chan struct{}
	for _, set := range s.subs {
		for _, sub := range set {
			subChans = append(subChans, sub.wakes)
		}
	}
	s.pollMu.Unlock()
	for _, set := range waits {
		for _, ch := range set {
			close(ch)
		}
	}
	// Live subscriptions get one last wakeup as well; the server re-checks
	// the closed flag and finishes the connection with a going-away close.
	for _, ch := range subChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Close releases the database handle. Parked long polls are woken first so
// they stop waiting and return without writing; the wake happens before the
// handle closes, so a waiter never observes a closed database. WebSocket
// subscriptions are likewise woken and drained (every handler unregisters)
// before the handle closes.
func (s *Store) Close() error {
	s.InterruptWaits()
	s.subWG.Wait()
	return s.db.Close()
}

// registerWait parks a channel for documentID and returns it together with a
// removal function. The channel is closed on the next committed change to the
// document, or when the store closes.
func (s *Store) registerWait(documentID string) (ch chan struct{}, remove func()) {
	ch = make(chan struct{}, 1)

	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	if s.closed {
		// Close beat the registration: signal immediately so the caller does
		// not park on a channel nobody will close.
		close(ch)
		return ch, func() {}
	}
	s.nextWaitID++
	id := s.nextWaitID
	set := s.pollWaits[documentID]
	if set == nil {
		set = make(map[uint64]chan struct{})
		s.pollWaits[documentID] = set
	}
	set[id] = ch
	return ch, func() {
		s.pollMu.Lock()
		if set, ok := s.pollWaits[documentID]; ok {
			delete(set, id)
			if len(set) == 0 {
				delete(s.pollWaits, documentID)
			}
		}
		s.pollMu.Unlock()
	}
}

// notifyWaiters signals every long poll parked on documentID and every
// WebSocket subscription open on it. It is called only after a
// change-bearing transaction has committed, so parked readers observe the
// new rows when they re-query and pushed frames describe committed changes.
func (s *Store) notifyWaiters(documentID string) {
	s.pollMu.Lock()
	set := s.pollWaits[documentID]
	delete(s.pollWaits, documentID)
	s.pollMu.Unlock()
	for _, ch := range set {
		close(ch)
	}
	s.signalSubscribers(documentID)
}

// WaitForChanges blocks until documentID has a change with cursor greater than
// after, a change is committed while waiting, wait elapses, ctx is canceled
// (the client disconnected) or the store closes. It then returns the current
// page of up to limit changes exactly as ListChanges would, with timedOut=true
// only when the wait deadline expired with no new rows.
//
// Registration precedes the first query: a commit landing between the read and
// the park still notifies a registered channel, so no change is missed.
//
// An unknown document is never parked on: it returns immediately with an empty
// list and nextCursor 0, timedOut=false. Data already past after is likewise
// returned immediately. wait <= 0 is an immediately expired deadline.
func (s *Store) WaitForChanges(ctx context.Context, documentID string, after, limit int64, wait time.Duration) (changes []ListedChange, nextCursor int64, timedOut bool, err error) {
	ch, remove := s.registerWait(documentID)
	defer remove()

	changes, nextCursor, err = s.ListChanges(documentID, after, limit)
	if err != nil || len(changes) > 0 {
		return changes, nextCursor, false, err
	}

	// Empty page: distinguish an unknown document (immediate empty/0, never
	// parked on) from a known document caught up to after.
	known, err := s.DocumentExists(documentID)
	if err != nil || !known {
		return changes, 0, false, err
	}

	// Known document, nothing new: a zero wait is an immediately expired
	// deadline; ListChanges already echoes nextCursor == after.
	if wait <= 0 {
		return changes, nextCursor, true, nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ch:
		// Woken by a committed change or by Close.
		s.pollMu.Lock()
		closing := s.closed
		s.pollMu.Unlock()
		if closing {
			return nil, 0, false, ErrStoreClosing
		}
		changes, nextCursor, err = s.ListChanges(documentID, after, limit)
		return changes, nextCursor, len(changes) == 0, err
	case <-timer.C:
		// Timeout with no new rows: echo the caller's cursor without
		// advancing it. The document was known above, so ListChanges returns
		// nextCursor == after for an empty page.
		changes, nextCursor, err = s.ListChanges(documentID, after, limit)
		return changes, nextCursor, len(changes) == 0, err
	case <-ctx.Done():
		return nil, 0, false, ctx.Err()
	}
}

// DocumentExists reports whether documentID has any change row and is
// therefore a known document rather than an empty namespace.
func (s *Store) DocumentExists(documentID string) (bool, error) {
	var known bool
	if err := s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)`,
		documentID,
	).Scan(&known); err != nil {
		return false, err
	}
	return known, nil
}

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
	if len(pending) > 0 {
		s.notifyWaiters(documentID)
	}
	return results, nil
}

// ReplayChanges commits a retried offline batch with exactly the same change
// semantics as PostChanges: ids resolve against the existing log, matching ids
// are idempotent only when deviceID and the decoded payload match, a mismatch
// is *ErrConflict and aborts the whole batch, and new ids take contiguous
// cursors in input order in one serialized transaction.
//
// Unlike PostChanges it additionally enforces the registration/permission
// layer before touching the log: an unregistered device yields
// ErrDeviceNotFound (404) and a revoked device yields ErrPermissionDenied
// (403). None of those outcomes writes anything or exposes change content.
func (s *Store) ReplayChanges(documentID string, changes []Change) ([]Result, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, in the same transaction: the device row and the permission
	// row are read before any change id is resolved, so a rejected replay
	// cannot observe (and its error cannot reveal) change content.
	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, changes[0].DeviceID,
	).Scan(&deviceExists); err != nil {
		return nil, err
	}
	if !deviceExists {
		return nil, ErrDeviceNotFound
	}
	var stored int
	err = tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, changes[0].DeviceID,
	).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
	case err != nil:
		return nil, err
	default:
		if stored == 0 {
			return nil, ErrPermissionDenied
		}
	}

	results := make([]Result, len(changes))

	// Resolve every id before allocating anything.
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
	if len(pending) > 0 {
		s.notifyWaiters(documentID)
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
	s.notifyWaiters(documentID)
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

// RestoreSnapshot appends one ordinary change whose payload is the state of
// documentID's snapshot at snapshotCursor, all within a single serialized
// transaction.
//
//   - A snapshot miss (unknown document or a cursor without a snapshot)
//     returns ErrSnapshotNotFound; nothing is written.
//   - If changeID already belongs to a prior restore, the call is idempotent
//     only when deviceId, snapshotCursor and the decoded source state all
//     match; the first result (created=false, original cursor) is returned.
//   - If changeID is taken by an ordinary change, or a restore whose provenance
//     differs, the call returns *ErrRestoreConflict; nothing is written.
//
// Otherwise a new change is appended with the next document cursor and the
// restore provenance is recorded. Old rows are never modified.
func (s *Store) RestoreSnapshot(documentID string, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// The snapshot must exist; its state is the payload to append.
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

	// Resolve the change id: an ordinary row and a prior restore are handled
	// differently, so consult both tables.
	var existingCursor int64
	var existingDevice string
	var existingPayload []byte
	err = tx.QueryRow(
		`SELECT cursor, device_id, payload FROM changes WHERE document_id = ? AND id = ?`,
		documentID, changeID,
	).Scan(&existingCursor, &existingDevice, &existingPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// New id; fall through to append below.
	case err != nil:
		return RestoreResult{}, err
	default:
		var restDevice string
		var restSnapshotCursor int64
		var restState []byte
		err = tx.QueryRow(
			`SELECT device_id, snapshot_cursor, state FROM restores WHERE document_id = ? AND change_id = ?`,
			documentID, changeID,
		).Scan(&restDevice, &restSnapshotCursor, &restState)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The id belongs to an ordinary change (or a batch/merge change),
			// not a restore: never treat it as an idempotent restore.
			return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
		case err != nil:
			return RestoreResult{}, err
		default:
			if restDevice != deviceID || restSnapshotCursor != snapshotCursor || !jsonEqual(restState, state) {
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			return RestoreResult{
				ID:           changeID,
				Created:      false,
				Cursor:       existingCursor,
				RestoredFrom: snapshotCursor,
			}, nil
		}
	}

	var nextCursor int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) + 1 FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&nextCursor); err != nil {
		return RestoreResult{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
		documentID, nextCursor, changeID, deviceID, state,
	); err != nil {
		return RestoreResult{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO restores (document_id, change_id, device_id, snapshot_cursor, change_cursor, state)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		documentID, changeID, deviceID, snapshotCursor, nextCursor, state,
	); err != nil {
		return RestoreResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return RestoreResult{}, err
	}
	s.notifyWaiters(documentID)
	return RestoreResult{
		ID:           changeID,
		Created:      true,
		Cursor:       nextCursor,
		RestoredFrom: snapshotCursor,
	}, nil
}

// RegisterDevice registers a device by client-supplied id. The first call for
// an id creates it and reports created=true; repeating the registration is
// idempotent (created=false). Device identity never changes.
func (s *Store) RegisterDevice(deviceID string) (created bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var exists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&exists); err != nil {
		return false, err
	}
	if !exists {
		if _, err := tx.Exec(`INSERT INTO devices (id) VALUES (?)`, deviceID); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	// An idempotent repeat commits nothing; releasing the read transaction is
	// enough.
	return false, tx.Commit()
}

// CreateSession creates a session owned by deviceID with a client-supplied id.
//
//   - An unknown device yields ErrDeviceNotFound; nothing is written.
//   - A session id already owned by another device yields *ErrSessionConflict;
//     nothing is written and the owner is unchanged.
//   - Re-posting the same (device, session) pair is idempotent: created=false.
//
// A deleted session is gone for good, so its id can be registered again as a
// brand-new session (created=true). The lookup, owner check and insert run in
// one serialized transaction, so concurrent creates of the same session id
// cannot both create it.
func (s *Store) CreateSession(deviceID, sessionID string) (created bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return false, err
	}
	if !deviceExists {
		return false, ErrDeviceNotFound
	}

	var owner string
	err = tx.QueryRow(
		`SELECT device_id FROM sessions WHERE id = ?`, sessionID,
	).Scan(&owner)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO sessions (id, device_id) VALUES (?, ?)`,
			sessionID, deviceID,
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	}

	if owner != deviceID {
		return false, &ErrSessionConflict{ID: sessionID}
	}
	return false, tx.Commit()
}

// DeleteSession removes the live session matching the (deviceID, sessionID)
// pair. Any miss — the session does not exist, was already deleted, or belongs
// to another device — returns ErrSessionNotFound and changes nothing. The
// check and the delete run in one transaction, so a concurrent create cannot
// interleave and a repeat delete still misses; the removal is committed to
// disk before the call returns.
func (s *Store) DeleteSession(deviceID, sessionID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var owner string
	err = tx.QueryRow(
		`SELECT device_id FROM sessions WHERE id = ?`, sessionID,
	).Scan(&owner)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrSessionNotFound
	case err != nil:
		return err
	}
	if owner != deviceID {
		return ErrSessionNotFound
	}

	if _, err := tx.Exec(
		`DELETE FROM sessions WHERE id = ? AND device_id = ?`,
		sessionID, deviceID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// SessionExists reports whether a live session with sessionID exists. Ownership
// is intentionally not part of this check: session-scoped reads only require
// that the session currently exists.
func (s *Store) SessionExists(sessionID string) (bool, error) {
	var exists bool
	if err := s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?)`, sessionID,
	).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// SessionDevice returns the id of the device that owns the live session
// sessionID. A session that was never created or was deleted yields
// ErrSessionNotFound.
func (s *Store) SessionDevice(sessionID string) (string, error) {
	var deviceID string
	err := s.db.QueryRow(
		`SELECT device_id FROM sessions WHERE id = ?`, sessionID,
	).Scan(&deviceID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", ErrSessionNotFound
	case err != nil:
		return "", err
	default:
		return deviceID, nil
	}
}

// SetDocumentPermission grants or revokes deviceID's access to documentID and
// reports whether the stored permission actually changed.
//
// Every device starts authorized for every document, so the first revoke is
// the first write for the pair (changed=true); repeating a state that already
// holds writes nothing and reports changed=false. An unregistered device
// yields ErrDeviceNotFound and nothing is written. The lookup and the write
// run in one serialized transaction, so concurrent grant/revoke calls each
// commit completely and the final state matches the last committed action.
func (s *Store) SetDocumentPermission(documentID, deviceID string, authorized bool) (changed bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return false, err
	}
	if !deviceExists {
		return false, ErrDeviceNotFound
	}

	current := true // devices start authorized; a row only records a deviation
	var stored int
	err = tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No row: the default (authorized) holds.
	case err != nil:
		return false, err
	default:
		current = stored != 0
	}
	if current == authorized {
		return false, tx.Commit()
	}

	v := 0
	if authorized {
		v = 1
	}
	if _, err := tx.Exec(
		`INSERT INTO document_permissions (document_id, device_id, authorized) VALUES (?, ?, ?)
		 ON CONFLICT (document_id, device_id) DO UPDATE SET authorized = excluded.authorized`,
		documentID, deviceID, v,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	// A grant cannot unblock a subscription (the first revoke already ended
	// it, stickily); only a revoke needs to end live subscriptions.
	if !authorized {
		s.signalRevokedSubscribers(documentID, deviceID)
	}
	return true, nil
}

// DocumentAuthorized reports whether deviceID currently holds permission for
// documentID. Devices start authorized, so an absent row means authorized.
func (s *Store) DocumentAuthorized(documentID, deviceID string) (bool, error) {
	var stored int
	err := s.db.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return true, nil
	case err != nil:
		return false, err
	default:
		return stored != 0, nil
	}
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
