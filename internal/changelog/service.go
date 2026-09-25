package changelog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// schema holds the service's own tables. No other service reads or writes
// them.
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

// Gate is the boundary the service consults when a write must be authorized
// before any change content is observed. Every check runs inside the
// service's own serialized transaction, so a rejected call cannot observe
// content and a concurrent permission change cannot race the commit. The
// concrete implementation is the composition root: registration lives in the
// registration layer and the permission decision lives in the permission
// service; this package knows neither.
type Gate interface {
	// CheckDeviceTx fails with a registration-layer sentinel error when
	// deviceID is not a registered device.
	CheckDeviceTx(tx *sql.Tx, deviceID string) error
	// CheckPermissionTx fails with a permission sentinel error when deviceID's
	// permission for documentID is revoked.
	CheckPermissionTx(tx *sql.Tx, documentID, deviceID string) error
}

// Service is the durable change-event log.
type Service struct {
	db   *sql.DB
	gate Gate

	// mu guards waits, subs, nextWaitID, nextSubID and closed. Parked long
	// polls wait on a per-document set of channels; a committed change to a
	// document closes (signals) every channel parked on it. WebSocket
	// subscriptions live in a second registry keyed by (document, device): a
	// commit signals every subscription under the document, while a
	// permission decision delivered to OnRevoke signals only the matching
	// device's subscriptions.
	mu       sync.Mutex
	waits    map[string]map[uint64]chan struct{}
	subs     map[subKey]map[uint64]*subscription
	nextWait uint64
	nextSub  uint64
	subWG    sync.WaitGroup
	closed   bool
}

// New opens the change-event service on db, creating its tables if needed.
// gate may be nil when every caller posts without authorization (e.g. a
// standalone test of the log); in the running process it is the composition
// root wiring the registration and permission services.
func New(db *sql.DB, gate Gate) (*Service, error) {
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{
		db:    db,
		gate:  gate,
		waits: make(map[string]map[uint64]chan struct{}),
		subs:  make(map[subKey]map[uint64]*subscription),
	}, nil
}

// DB exposes the handle the service was opened on. The composition root owns
// the shared database and hands the same handle to every service so their
// transactions serialize on one connection; services themselves never reach
// for another handle.
func (s *Service) DB() *sql.DB { return s.db }

// PostChanges validates and commits one batch atomically.
//
// Every change must have a non-empty id; duplicate ids within the batch are
// rejected by the caller before reaching this point. On conflict the returned
// error is *ErrConflict and nothing is written. Cursors are assigned in batch
// order to ids that do not yet exist. Results are returned in the same order
// as the input.
func (s *Service) PostChanges(documentID string, changes []Change) ([]Result, error) {
	return s.commit(documentID, changes, false)
}

// ReplayChanges commits a retried offline batch with exactly the same change
// semantics as PostChanges, but first enforces the Gate inside the same
// transaction: an unregistered device and a revoked device each fail with the
// gate's sentinel error before any change id is resolved, so a rejected
// replay neither writes nor exposes change content.
func (s *Service) ReplayChanges(documentID string, changes []Change) ([]Result, error) {
	if s.gate == nil {
		return nil, errors.New("changelog: replay requires an authorization gate")
	}
	return s.commit(documentID, changes, true)
}

// commit is the single implementation of the batch transaction. When gated
// (an offline replay) the device and permission checks run first, inside the
// transaction.
func (s *Service) commit(documentID string, changes []Change, gated bool) ([]Result, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if gated {
		// Gate first, in the same transaction: the device row and the
		// permission row are read before any change id is resolved, so a
		// rejected replay cannot observe (and its error cannot reveal) change
		// content.
		if err := s.gate.CheckDeviceTx(tx, changes[0].DeviceID); err != nil {
			return nil, err
		}
		if err := s.gate.CheckPermissionTx(tx, documentID, changes[0].DeviceID); err != nil {
			return nil, err
		}
	}

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
		s.notifyCommits(documentID)
	}
	return results, nil
}

// DocumentExists reports whether documentID has any change row and is
// therefore a known document rather than an empty namespace.
func (s *Service) DocumentExists(documentID string) (bool, error) {
	var known bool
	if err := s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)`,
		documentID,
	).Scan(&known); err != nil {
		return false, err
	}
	return known, nil
}

// ListChanges returns at most limit changes for documentID whose cursor is
// greater than after, in cursor order, together with the cursor to pass as
// the next after value. An unknown document yields an empty list and cursor 0;
// a known document with no rows past after yields nextCursor == after.
func (s *Service) ListChanges(documentID string, after, limit int64) ([]ListedChange, int64, error) {
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
		known, err := s.DocumentExists(documentID)
		if err != nil {
			return nil, 0, err
		}
		if !known {
			return out, 0, nil
		}
	}
	return out, maxCursor, nil
}

// errNotObject marks a payload that is not a JSON object.
var errNotObject = errors.New("payload is not a JSON object")

// objectKeys returns the top-level keys of a JSON object payload. A null,
// array or scalar payload yields errNotObject.
func objectKeys(raw json.RawMessage) (map[string]struct{}, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
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
func (s *Service) MergeChange(documentID string, baseCursor int64, c Change) (MergeResult, error) {
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
	s.notifyCommits(documentID)
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
func (s *Service) PutSnapshot(documentID string, cursor int64, state json.RawMessage) (bool, error) {
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
func (s *Service) GetSnapshot(documentID string, cursor int64) (json.RawMessage, error) {
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
//   - If changeID is taken by an ordinary change, or a restore whose
//     provenance differs, the call returns *ErrRestoreConflict; nothing is
//     written.
//
// Otherwise a new change is appended with the next document cursor and the
// restore provenance is recorded. Old rows are never modified.
func (s *Service) RestoreSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
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
	s.notifyCommits(documentID)
	return RestoreResult{
		ID:           changeID,
		Created:      true,
		Cursor:       nextCursor,
		RestoredFrom: snapshotCursor,
	}, nil
}

// WaitForChanges blocks until documentID has a change with cursor greater than
// after, a change is committed while waiting, wait elapses, ctx is canceled
// (the client disconnected) or the service is interrupted. It then returns
// the current page of up to limit changes exactly as ListChanges would, with
// timedOut=true only when the wait deadline expired with no new rows.
//
// Registration precedes the first query: a commit landing between the read
// and the park still notifies a registered channel, so no change is missed.
//
// An unknown document is never parked on: it returns immediately with an
// empty list and nextCursor 0, timedOut=false. Data already past after is
// likewise returned immediately. wait <= 0 is an immediately expired
// deadline.
func (s *Service) WaitForChanges(ctx context.Context, documentID string, after, limit int64, wait time.Duration) (changes []ListedChange, nextCursor int64, timedOut bool, err error) {
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
		// Woken by a committed change or by Interrupt.
		if s.Closing() {
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

// InterruptWaits wakes every parked long poll without closing the database,
// so an orderly server shutdown drains waiting connections immediately (they
// answer 503) instead of holding shutdown hostage until their wait deadline.
// Committed state is untouched. Live subscriptions receive one last wakeup as
// well so their handlers finish with a going-away close.
func (s *Service) InterruptWaits() {
	s.mu.Lock()
	s.closed = true
	waits := s.waits
	s.waits = map[string]map[uint64]chan struct{}{}
	// Snapshot the live subscription channels without reaping the registry:
	// unregister remains valid while handlers drain, so the WaitGroup in
	// Close balances.
	var subChans []chan struct{}
	for _, set := range s.subs {
		for _, sub := range set {
			subChans = append(subChans, sub.wakes)
		}
	}
	s.mu.Unlock()
	for _, set := range waits {
		for _, ch := range set {
			close(ch)
		}
	}
	for _, ch := range subChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Close interrupts parked waits and waits for every subscription handler to
// unregister. The shared database handle itself is closed by its owner (the
// composition root), never by one service.
func (s *Service) Close() {
	s.InterruptWaits()
	s.subWG.Wait()
}

// Closing reports whether the service has begun shutting down. A woken
// subscription uses it to distinguish a termination signal from a commit.
func (s *Service) Closing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// registerWait parks a channel for documentID and returns it together with a
// removal function. The channel is closed on the next committed change to the
// document, or when the service is interrupted.
func (s *Service) registerWait(documentID string) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		close(ch)
		return ch, func() {}
	}
	s.nextWait++
	id := s.nextWait
	set := s.waits[documentID]
	if set == nil {
		set = make(map[uint64]chan struct{})
		s.waits[documentID] = set
	}
	set[id] = ch
	return ch, func() {
		s.mu.Lock()
		if set, ok := s.waits[documentID]; ok {
			delete(set, id)
			if len(set) == 0 {
				delete(s.waits, documentID)
			}
		}
		s.mu.Unlock()
	}
}

// notifyCommits signals every long poll parked on documentID and every
// subscription open on it. It is called only after a change-bearing
// transaction has committed, so parked observers see the new rows when they
// re-query and pushed frames describe committed changes.
func (s *Service) notifyCommits(documentID string) {
	s.mu.Lock()
	set := s.waits[documentID]
	delete(s.waits, documentID)
	s.mu.Unlock()
	for _, ch := range set {
		close(ch)
	}
	s.signalSubscribers(documentID)
}
