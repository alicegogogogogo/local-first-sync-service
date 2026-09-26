package events

import (
	"database/sql"
	"encoding/json"
	"errors"
	"sync"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// Gate is the only seam the change event service uses to reach the
// registration and permission layers. It is evaluated inside the caller's
// serialized transaction, so a rejected request never observes change
// content and the gate and the gated write commit as one judgment.
type Gate interface {
	// DeviceAuthorizedTx returns nil when deviceID is registered and currently
	// authorized for documentID, store.ErrDeviceNotFound when the device is not
	// registered and ErrPermissionDenied when its access was revoked.
	DeviceAuthorizedTx(tx *sql.Tx, documentID, deviceID string) error
}

// Service is the change event service: the durable change log, snapshots and
// restores plus the in-memory long-poll and subscription observers of them.
type Service struct {
	db   *sql.DB
	gate Gate

	// mu guards waits, subs, nextWaitID, nextSubID and closed. Parked long
	// polls wait on a per-document set of channels; a committed change closes
	// (signals) every channel parked on it. WebSocket subscriptions live in a
	// second registry keyed by (document, device): a commit signals every
	// subscription under the document, while the permission service signals
	// only the matching device's subscriptions.
	mu       sync.Mutex
	waits    map[string]map[uint64]chan struct{}
	subs     map[subKey]map[uint64]*subscription
	nextWait uint64
	nextSub  uint64
	subWG    sync.WaitGroup
	closed   bool
}

// New constructs the change event service over the shared kernel handle and
// creates its tables. gate answers the registration/permission checks inside
// a write transaction; it may be nil for callers that only use the paths that
// do not gate (ordinary post, merge, snapshots, reads).
func New(kernel *store.Store, gate Gate) (*Service, error) {
	db := kernel.DB()
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

// PostChanges validates and commits one batch atomically.
//
// Every change must have a non-empty id; duplicate ids within the batch are
// rejected by the caller before reaching this point. On conflict the returned
// error is *ErrConflict and nothing is written. Cursors are assigned in batch
// order to ids that do not yet exist. Results are returned in the same order
// as the input.
func (s *Service) PostChanges(documentID string, changes []Change) ([]Result, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	results, pending, err := resolveChanges(tx, documentID, changes)
	if err != nil {
		return nil, err
	}
	if err := appendPending(tx, documentID, changes, pending, results); err != nil {
		return nil, err
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
// semantics as PostChanges, but enforces the gate (registration then
// permission) first in the same transaction: an unregistered device yields
// store.ErrDeviceNotFound (404) and a revoked device yields
// ErrPermissionDenied (403). None of those outcomes writes anything or
// exposes change content.
func (s *Service) ReplayChanges(documentID string, changes []Change) ([]Result, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first: the device row and the permission row are read before any
	// change id is resolved, so a rejected replay cannot reveal content.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, changes[0].DeviceID); err != nil {
			return nil, err
		}
	}

	results, pending, err := resolveChanges(tx, documentID, changes)
	if err != nil {
		return nil, err
	}
	if err := appendPending(tx, documentID, changes, pending, results); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(pending) > 0 {
		s.notifyWaiters(documentID)
	}
	return results, nil
}

// resolveChanges resolves every change id against existing rows inside tx,
// falling back to the retained summaries of compacted-away ids. A single
// device/payload mismatch aborts the whole batch before any cursor is
// allocated. It returns the per-id results and the indices of genuinely new
// changes that still need a cursor.
func resolveChanges(tx *sql.Tx, documentID string, changes []Change) ([]Result, []int, error) {
	results := make([]Result, len(changes))
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
			// No online row: the id may belong to a change compaction
			// trimmed, whose retained summary still decides idempotency.
			found, cursor, err := resolveTrimmedIdentity(tx, documentID, c)
			if err != nil {
				return nil, nil, err
			}
			if !found {
				pending = append(pending, i)
				continue
			}
			results[i] = Result{
				ID:      c.ID,
				Created: false,
				Cursor:  cursor,
			}
		case err != nil:
			return nil, nil, err
		default:
			if existingDevice != c.DeviceID || !store.JSONEqual(existingPayload, c.Payload) {
				return nil, nil, &ErrConflict{ID: c.ID}
			}
			results[i] = Result{
				ID:      c.ID,
				Created: false,
				Cursor:  existingCursor,
			}
		}
	}
	return results, pending, nil
}

// appendPending allocates contiguous cursors to the new change indices and
// inserts them in input order. It runs inside the caller's transaction. The
// allocation continues past the compaction boundary, so trimming the online
// log never makes the cursor space restart.
func appendPending(tx *sql.Tx, documentID string, changes []Change, pending []int, results []Result) error {
	nextCursor, err := currentCursorTx(tx, documentID)
	if err != nil {
		return err
	}
	nextCursor++
	for _, i := range pending {
		c := changes[i]
		if _, err := tx.Exec(
			`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
			documentID, nextCursor, c.ID, c.DeviceID, []byte(c.Payload),
		); err != nil {
			return err
		}
		results[i] = Result{
			ID:      c.ID,
			Created: true,
			Cursor:  nextCursor,
		}
		nextCursor++
	}
	return nil
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
// the current cursor, returns ErrStaleCursor. A baseCursor below the
// compaction boundary returns ErrCompactedBase: the changes the merge would be
// checked against have left the online log, so the conflict check cannot be
// completed. An unknown document with baseCursor 0 accepts the first change as
// "applied".
func (s *Service) MergeChange(documentID string, baseCursor int64, c Change) (MergeResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return MergeResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Current cursor is the document's high-water mark (0 when unknown); it
	// never drops below the compaction boundary.
	current, err := currentCursorTx(tx, documentID)
	if err != nil {
		return MergeResult{}, err
	}
	if baseCursor > current || (baseCursor != 0 && current == 0) {
		return MergeResult{}, ErrStaleCursor
	}
	boundary, err := boundaryOfTx(tx, documentID)
	if err != nil {
		return MergeResult{}, err
	}
	if baseCursor < boundary {
		return MergeResult{}, ErrCompactedBase
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
		// No online row: a compacted-away id still answers from its retained
		// summary; anything else is a new id and falls through to the append
		// logic below.
		found, cursor, err := resolveTrimmedIdentity(tx, documentID, c)
		if err != nil {
			return MergeResult{}, err
		}
		if found {
			return MergeResult{
				ID:      c.ID,
				Outcome: "idempotent",
				Cursor:  cursor,
				Result: &Result{
					ID:      c.ID,
					Created: false,
					Cursor:  cursor,
				},
			}, nil
		}
	case err != nil:
		return MergeResult{}, err
	default:
		if existingDevice != c.DeviceID || !store.JSONEqual(existingPayload, c.Payload) {
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
