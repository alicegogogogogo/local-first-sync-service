// Package store is the durable kernel shared by the internal services: it
// owns the SQLite database handle and the registration layer (devices and
// sessions) plus resumable attachments.
//
// The business services layered on top of the kernel — the change event log,
// the permission ledger and the CRDT state — live in their own packages and
// cooperate only through their internal interfaces. They all share this one
// handle: it opens a single pooled connection with immediate write
// transactions, so every service's transactional judgment is serialized on
// the same connection and no batch is ever half written.
//
// Devices and sessions form the registration layer. A device registers with a
// client-supplied id; a session belongs to exactly one registered device and
// is addressed by its own client-supplied id. Registration and session
// creation are idempotent re-posts: the first call creates, an identical
// repeat does not. A session id already owned by another device is a
// conflict, never silently re-homed. Deletes are owner-scoped and hard: a
// repeat delete, another device, and a cross-device path all miss with 404,
// after which the id is free to be created again.
//
// Attachments are resumable chunked uploads owned by the registered device
// that created them. Creation pins the declared total size, chunk size and
// SHA-256 digest; chunks land in any order and are stored durably as they
// arrive, so an interrupted upload resumes after a restart with its
// idempotency and conflict decisions intact. Finishing concatenates the
// chunks, verifies the digest and seals the upload; a sealed upload is
// immutable and rejects every further chunk write. Finished content is
// addressed by digest, so a second attachment with the same digest and size
// reuses the stored bytes instead of copying them. The creator may grant
// other registered devices read-only access per attachment; every
// (attachment, device) pair starts unauthorized, grants and revokes commit in
// the same serialized way, and access never widens the write paths. Deletes
// are owner-scoped and hard: the record, its chunks and its grants vanish in
// one serialized transaction, a repeat delete misses with 404, the id is free
// to be created again, and the shared content bytes are reclaimed once the
// last completed attachment referencing them is gone.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "modernc.org/sqlite"
)

// ErrDeviceNotFound reports that an operation targets a device id that was
// never registered. Services map it to 404; nothing is written.
var ErrDeviceNotFound = errors.New("device not found")

// ErrPermissionDenied reports that a device's access to a document has been
// revoked. Services map it to 403; nothing is written and no document content
// is exposed. It is a shared verdict produced by the permission service and
// consulted by every service that enforces the gate inside a transaction.
var ErrPermissionDenied = errors.New("device permission for this document has been revoked")

// ErrSessionConflict reports that a session id already belongs to a different
// device. The session keeps its original owner; nothing is written. Services
// map it to 409.
type ErrSessionConflict struct {
	ID string
}

func (e *ErrSessionConflict) Error() string {
	return fmt.Sprintf("session %q belongs to another device", e.ID)
}

// ErrSessionNotFound reports that no live session matches the (device,
// session) pair: the session was never created, was already deleted, or
// belongs to another device. Services map it to 404.
var ErrSessionNotFound = errors.New("session not found")

// DBTX is the subset of *sql.DB and *sql.Tx the services use for their
// serialized work, so judgments made "in the same transaction" (for example a
// permission gate followed by the gated write) run on one concrete connection
// whether the caller holds a transaction or the handle itself.
type DBTX interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Store is the durable kernel: one SQLite database handle together with the
// device, session and attachment data it owns.
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
	// for readers does not help correctness here. It also keeps every
	// service's transaction serialized on the same connection.
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// DB returns the underlying handle. The business services use it to open the
// serialized transactions their judgments run in; callers outside the
// internal packages never need it.
func (s *Store) DB() *sql.DB { return s.db }

// Close releases the database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS devices (
	id TEXT NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS sessions (
	id        TEXT NOT NULL PRIMARY KEY,
	device_id TEXT NOT NULL REFERENCES devices(id)
);
CREATE INDEX IF NOT EXISTS sessions_device_idx
	ON sessions(device_id);
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
CREATE TABLE IF NOT EXISTS attachment_access (
	attachment_id TEXT NOT NULL REFERENCES attachments(id),
	device_id     TEXT NOT NULL,
	authorized    INTEGER NOT NULL,
	PRIMARY KEY (attachment_id, device_id)
);
`)
	return err
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

	created, err = RegisterDeviceTx(tx, deviceID)
	if err != nil {
		return false, err
	}
	return created, tx.Commit()
}

// RegisterDeviceTx is RegisterDevice against an existing transaction, so the
// services can resolve the registration layer as the first step of a larger
// serialized transaction (their permission/content gates then share it).
func RegisterDeviceTx(q DBTX, deviceID string) (created bool, err error) {
	var exists bool
	if err := q.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&exists); err != nil {
		return false, err
	}
	if !exists {
		if _, err := q.Exec(`INSERT INTO devices (id) VALUES (?)`, deviceID); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// DeviceExistsTx reports whether deviceID is registered, evaluated inside q so
// a service's gate runs on the same transaction as the work it guards.
func DeviceExistsTx(q DBTX, deviceID string) (bool, error) {
	var exists bool
	if err := q.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
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

// SessionExists reports whether a live session with sessionID exists.
// Ownership is intentionally not part of this check: session-scoped reads only
// require that the session currently exists.
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

// JSONEqual reports whether two payloads are equal after JSON decoding, so
// 1 and 1.0, or {"a":1,"b":2} and {"b":2,"a":1}, compare equal. It is shared
// verbatim by the change event and CRDT services' idempotency judgments.
func JSONEqual(a, b json.RawMessage) bool {
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
