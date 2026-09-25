// Package store is the composition root of the sync service's durable state.
// It owns the SQLite handle and the registration layer (devices and
// sessions), plus the attachment upload tables, and wires the three
// independent internal services into one facade:
//
//   - changelog: the change-event service — cursor allocation, atomic batch
//     commits, paged reads and event ordering (changes, snapshots, restores);
//   - crdtstate: the CRDT state service — document type fixation, idempotency
//     and conflict decisions, merge results and compaction;
//   - permission: the permission service — per-(document, device)
//     authorization state and the unified revoke decision.
//
// The services never read or write each other's storage. The two cross-service
// needs are met through interfaces wired here: the gate (registration plus the
// permission decision, enforced inside each service's own serialized
// transaction) and the revoke notification (a committed revoke ends live
// subscriptions in both other services). Everything the public HTTP surface
// and the pre-split tests call keeps its exact signature and semantics.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/alicegogogogogo/local-first-sync-service/internal/changelog"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdtstate"
	"github.com/alicegogogogogo/local-first-sync-service/internal/permission"
)

// ---------------------------------------------------------------------------
// Re-exported service types and errors. These aliases keep the pre-split
// package API intact: callers (the HTTP layer, the process supervisor and the
// white-box tests) see the same names with the same shapes.
// ---------------------------------------------------------------------------

// Change-event service types.
type (
	Change        = changelog.Change
	Result        = changelog.Result
	ListedChange  = changelog.ListedChange
	MergeResult   = changelog.MergeResult
	RestoreResult = changelog.RestoreResult
)

// Change-event service errors.
var (
	ErrStoreClosing     = changelog.ErrStoreClosing
	ErrStaleCursor      = changelog.ErrStaleCursor
	ErrSnapshotBase     = changelog.ErrSnapshotBase
	ErrSnapshotNotFound = changelog.ErrSnapshotNotFound
)

type (
	ErrConflict         = changelog.ErrConflict
	ErrSnapshotConflict = changelog.ErrSnapshotConflict
	ErrRestoreConflict  = changelog.ErrRestoreConflict
)

// CRDT state service types and constants.
type (
	CRDTOp           = crdtstate.Op
	CRDTState        = crdtstate.State
	CRDTResult       = crdtstate.Result
	CRDTSnapshot     = crdtstate.Snapshot
	CRDTSubscription = crdtstate.Subscription
)

const (
	CRDTTypeCounter  = crdtstate.TypeCounter
	CRDTTypeGSet     = crdtstate.TypeGSet
	CRDTTypeRegister = crdtstate.TypeRegister
	CRDTTypeORSet    = crdtstate.TypeORSet
	CRDTORSetAdd     = crdtstate.ORSetAdd
	CRDTORSetRemove  = crdtstate.ORSetRemove
)

var ErrCRDTNotFound = crdtstate.ErrNotFound

type ErrCRDTConflict = crdtstate.ErrConflict

// Permission service errors.
var ErrPermissionDenied = permission.ErrPermissionDenied

// ---------------------------------------------------------------------------
// Registration-layer errors (owned by this package).
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// The facade.
// ---------------------------------------------------------------------------

// Store is the composition root: the shared database handle, the registration
// layer and the wiring between the three services.
type Store struct {
	db *sql.DB

	changes     *changelog.Service
	crdt        *crdtstate.Service
	permissions *permission.Service
}

// schema holds the tables this package owns: the registration layer and the
// attachment uploads. The services create their own tables when constructed.
const schema = `
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
`

// Open opens (creating if needed) the SQLite database at path, constructs the
// three services on the shared handle and wires their collaboration
// boundaries. The empty path opens an in-memory database useful for tests.
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
	// for readers does not help correctness here. The services share this
	// handle, so their transactions serialize on the same connection.
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}

	// The gate both content services enforce inside their own transactions:
	// registration from this package's devices table, the permission decision
	// from the permission service — both read through the service's tx, so a
	// commit, a compaction and a permission change are judged in one
	// serialized transaction.
	gate := &serviceGate{store: s}

	permissions, err := permission.New(db, deviceRegistry{store: s})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	changes, err := changelog.New(db, gate)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	crdt, err := crdtstate.New(db, gate)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	// A committed revoke ends live subscriptions in both content services,
	// stickily. The permission service itself knows nothing about them.
	permissions.OnRevoke(changes.OnRevoke)
	permissions.OnRevoke(crdt.OnRevoke)

	s.permissions = permissions
	s.changes = changes
	s.crdt = crdt
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(schema)
	return err
}

// deviceRegistry adapts the registration layer to the permission service's
// DeviceRegistry boundary: an unregistered device fails a permission write
// with ErrDeviceNotFound.
type deviceRegistry struct{ store *Store }

func (r deviceRegistry) RequireTx(tx *sql.Tx, deviceID string) error {
	var exists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrDeviceNotFound
	}
	return nil
}

// serviceGate adapts the registration layer and the permission service to the
// Gate boundary of both content services. The checks run inside the calling
// service's transaction.
type serviceGate struct{ store *Store }

func (g *serviceGate) CheckDeviceTx(tx *sql.Tx, deviceID string) error {
	return deviceRegistry{store: g.store}.RequireTx(tx, deviceID)
}

func (g *serviceGate) CheckPermissionTx(tx *sql.Tx, documentID, deviceID string) error {
	return g.store.permissions.CheckTx(tx, documentID, deviceID)
}

// InterruptWaits wakes every parked long poll and every live subscription in
// both content services, so an orderly server shutdown drains waiting
// connections immediately (they answer 503 or finish with a going-away
// close). Committed state is untouched.
func (s *Store) InterruptWaits() {
	s.changes.InterruptWaits()
	s.crdt.InterruptWaits()
}

// Close releases the database handle. Parked long polls and live
// subscriptions are interrupted and drained first, so no waiter observes a
// closed database.
func (s *Store) Close() error {
	s.changes.Close()
	s.crdt.Close()
	return s.db.Close()
}

// Closing reports whether the store has begun shutting down. A woken
// subscription uses it to distinguish a termination signal from a commit.
func (s *Store) Closing() bool {
	return s.changes.Closing() || s.crdt.Closing()
}

// ---------------------------------------------------------------------------
// Change-event service delegation.
// ---------------------------------------------------------------------------

// PostChanges validates and commits one batch atomically. See
// changelog.Service.PostChanges.
func (s *Store) PostChanges(documentID string, changes []Change) ([]Result, error) {
	return s.changes.PostChanges(documentID, changes)
}

// ReplayChanges commits a retried offline batch with the registration and
// permission layer enforced first. See changelog.Service.ReplayChanges.
func (s *Store) ReplayChanges(documentID string, changes []Change) ([]Result, error) {
	return s.changes.ReplayChanges(documentID, changes)
}

// MergeChange validates and appends one change against a caller-observed base
// cursor. See changelog.Service.MergeChange.
func (s *Store) MergeChange(documentID string, baseCursor int64, c Change) (MergeResult, error) {
	return s.changes.MergeChange(documentID, baseCursor, c)
}

// ListChanges returns a cursor-ordered page of changes. See
// changelog.Service.ListChanges.
func (s *Store) ListChanges(documentID string, after, limit int64) ([]ListedChange, int64, error) {
	return s.changes.ListChanges(documentID, after, limit)
}

// DocumentExists reports whether documentID has any change row. See
// changelog.Service.DocumentExists.
func (s *Store) DocumentExists(documentID string) (bool, error) {
	return s.changes.DocumentExists(documentID)
}

// WaitForChanges is the long-polling wait over the change log. See
// changelog.Service.WaitForChanges.
func (s *Store) WaitForChanges(ctx context.Context, documentID string, after, limit int64, wait time.Duration) (changes []ListedChange, nextCursor int64, timedOut bool, err error) {
	return s.changes.WaitForChanges(ctx, documentID, after, limit, wait)
}

// PutSnapshot stores state as the snapshot of documentID at cursor. See
// changelog.Service.PutSnapshot.
func (s *Store) PutSnapshot(documentID string, cursor int64, state json.RawMessage) (bool, error) {
	return s.changes.PutSnapshot(documentID, cursor, state)
}

// GetSnapshot returns the state stored for documentID at cursor. See
// changelog.Service.GetSnapshot.
func (s *Store) GetSnapshot(documentID string, cursor int64) (json.RawMessage, error) {
	return s.changes.GetSnapshot(documentID, cursor)
}

// RestoreSnapshot appends one ordinary change whose payload is a snapshot's
// state. See changelog.Service.RestoreSnapshot.
func (s *Store) RestoreSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	return s.changes.RestoreSnapshot(documentID, deviceID, changeID, snapshotCursor)
}

// AddSubscription registers an in-memory change subscription. See
// changelog.Service.AddSubscription.
func (s *Store) AddSubscription(documentID, deviceID string) (wakes <-chan struct{}, revoked <-chan struct{}, unregister func()) {
	return s.changes.AddSubscription(documentID, deviceID)
}

// ---------------------------------------------------------------------------
// CRDT state service delegation.
// ---------------------------------------------------------------------------

// SubmitCRDTOps validates and commits one CRDT batch atomically. See
// crdtstate.Service.Submit.
func (s *Store) SubmitCRDTOps(documentID, declaredType string, ops []CRDTOp) ([]CRDTResult, error) {
	return s.crdt.Submit(documentID, declaredType, ops)
}

// GetCRDTState returns the merged CRDT state for documentID. See
// crdtstate.Service.GetState.
func (s *Store) GetCRDTState(documentID string) (CRDTState, error) {
	return s.crdt.GetState(documentID)
}

// GetCRDTSnapshot returns the merged state plus its storage footprint. See
// crdtstate.Service.GetSnapshot.
func (s *Store) GetCRDTSnapshot(documentID string) (CRDTSnapshot, error) {
	return s.crdt.GetSnapshot(documentID)
}

// CompactCRDT trims stored operations and tombstones that no longer affect
// the merge. See crdtstate.Service.Compact.
func (s *Store) CompactCRDT(documentID, deviceID string) (CRDTSnapshot, error) {
	return s.crdt.Compact(documentID, deviceID)
}

// AddCRDTSubscription registers an in-memory CRDT state subscription. See
// crdtstate.Service.AddSubscription.
func (s *Store) AddCRDTSubscription(documentID, deviceID string) (*CRDTSubscription, func()) {
	return s.crdt.AddSubscription(documentID, deviceID)
}

// OpenCRDTSubscription atomically registers a CRDT subscription and reads the
// current merged state. See crdtstate.Service.OpenSubscription.
func (s *Store) OpenCRDTSubscription(documentID, deviceID string) (state *CRDTState, sub *CRDTSubscription, unregister func(), err error) {
	return s.crdt.OpenSubscription(documentID, deviceID)
}

// ---------------------------------------------------------------------------
// Permission service delegation.
// ---------------------------------------------------------------------------

// SetDocumentPermission grants or revokes deviceID's access to documentID and
// reports whether the stored permission actually changed. See
// permission.Service.Set.
func (s *Store) SetDocumentPermission(documentID, deviceID string, authorized bool) (changed bool, err error) {
	return s.permissions.Set(documentID, deviceID, authorized)
}

// DocumentAuthorized reports whether deviceID currently holds permission for
// documentID. See permission.Service.Authorized.
func (s *Store) DocumentAuthorized(documentID, deviceID string) (bool, error) {
	return s.permissions.Authorized(documentID, deviceID)
}

// ---------------------------------------------------------------------------
// Registration layer (owned by this package).
// ---------------------------------------------------------------------------

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

// CreateSession creates a session owned by deviceID with a client-supplied
// id.
//
//   - An unknown device yields ErrDeviceNotFound; nothing is written.
//   - A session id already owned by another device yields
//     *ErrSessionConflict; nothing is written and the owner is unchanged.
//   - Re-posting the same (device, session) pair is idempotent:
//     created=false.
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
// pair. Any miss — the session does not exist, was already deleted, or
// belongs to another device — returns ErrSessionNotFound and changes nothing.
// The check and the delete run in one transaction, so a concurrent create
// cannot interleave and a repeat delete still misses; the removal is
// committed to disk before the call returns.
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
// Ownership is intentionally not part of this check: session-scoped reads
// only require that the session currently exists.
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

// jsonEqual reports whether two payloads are equal after JSON decoding. It is
// kept here for the white-box tests of this package; the services carry their
// own copies.
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
