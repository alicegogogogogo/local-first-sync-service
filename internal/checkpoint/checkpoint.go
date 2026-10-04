// Package checkpoint is the session-scoped durable sync checkpoint service.
//
// A checkpoint records, per (session, document) pair, the last change cursor a
// session has confirmed it applied locally in full. It lets a client find its
// consumption position again after a dropped connection or a server restart
// instead of saving the position only on the client.
//
// Checkpoints are durable rows in their own table, owned solely by this
// service. They are not changes: confirming a checkpoint allocates no change
// cursor, writes no change, wakes no long poll and notifies no subscriber;
// reading changes never advances one. The stored value is monotonic — a
// smaller confirmation can never overwrite a larger one, even when two
// confirmations race, because the existence, permission and cursor judgments
// and the write all run in one immediate transaction on the single shared
// connection.
//
// The service reaches the registration layer (sessions and their owning
// devices) and the cursor facts (compaction boundary and document high-water
// mark) only through the interfaces it is constructed with, so it never reads
// another service's tables directly. Cascade cleanup — session, device and
// document deletion and permission revocation — is driven by the composition
// root through this service's Tx helpers so the row removals commit as one
// judgment with the deletion that owns them.
package checkpoint

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// schema is created when the service is constructed.
const schema = `
CREATE TABLE IF NOT EXISTS session_checkpoints (
	session_id  TEXT NOT NULL,
	document_id TEXT NOT NULL,
	cursor      INTEGER NOT NULL,
	PRIMARY KEY (session_id, document_id)
);
`

// ErrSessionNotFound reports that the session a checkpoint targets is not a
// live session. The caller maps it to 404; nothing is written.
var ErrSessionNotFound = store.ErrSessionNotFound

// ErrRegress reports that a confirmation names a cursor below the one already
// recorded for the (session, document) pair. The stored checkpoint is left
// unchanged; the caller maps it to 409 and reports the standing cursor.
type ErrRegress struct {
	Current int64
}

func (e *ErrRegress) Error() string {
	return fmt.Sprintf("checkpoint cursor %d is below the recorded cursor", e.Current)
}

// ErrAboveMax reports that a confirmation names a cursor greater than the
// document's current high-water mark. Nothing is written; the caller maps it
// to 409 and reports that maximum.
type ErrAboveMax struct {
	Max int64
}

func (e *ErrAboveMax) Error() string {
	return fmt.Sprintf("checkpoint cursor is above the document maximum %d", e.Max)
}

// ErrBelowBoundary reports that the first confirmation for a pair names a
// cursor below the document's compaction boundary: the changes up to the
// boundary have left the online log, so the client must restore the snapshot
// first. Confirming exactly the boundary is allowed. Nothing is written; the
// caller maps it to 409 and reports the boundary.
type ErrBelowBoundary struct {
	Boundary int64
}

func (e *ErrBelowBoundary) Error() string {
	return fmt.Sprintf("first checkpoint cursor is below the compaction boundary %d", e.Boundary)
}

// CursorFacts answers the document cursor facts the checkpoint judgment needs,
// evaluated inside the checkpoint's own transaction.
type CursorFacts interface {
	DocumentCursorInfoTx(q store.DBTX, documentID string) (boundary, maxCursor int64, err error)
}

// Service owns the durable session checkpoint rows.
type Service struct {
	db    *sql.DB
	authz *authz.Service
	curs  CursorFacts
}

// New constructs the checkpoint service over the shared kernel handle and
// creates its table. authz answers the registration/permission gate and curs
// answers the document boundary and high-water mark; both are evaluated inside
// the checkpoint's own serialized transaction.
func New(kernel *store.Store, authzSvc *authz.Service, curs CursorFacts) (*Service, error) {
	db := kernel.DB()
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{db: db, authz: authzSvc, curs: curs}, nil
}

// PutResult is the outcome of a confirmation: the cursor now recorded and
// whether this confirmation moved it forward. Re-confirming the recorded
// cursor leaves the row untouched and reports Advanced=false.
type PutResult struct {
	Cursor   int64 `json:"cursor"`
	Advanced bool  `json:"advanced"`
}

// GetResult is the checkpoint read: the recorded cursor (zero when never
// confirmed), whether any confirmation has ever been recorded, and the
// document's current compaction boundary and high-water mark.
type GetResult struct {
	Cursor    int64 `json:"cursor"`
	Recorded  bool  `json:"recorded"`
	Boundary  int64 `json:"boundary"`
	MaxCursor int64 `json:"maxCursor"`
}

// Put confirms cursor as the (sessionID, documentID) pair's checkpoint.
//
// The fixed verdict order runs in one serialized transaction, before the row
// is written:
//
//   - the session must currently exist and its owning device is resolved from
//     it (a missing or deleted session yields store.ErrSessionNotFound);
//   - that device must currently be authorized for the document (a revoked
//     device yields store.ErrPermissionDenied);
//   - the cursor may not exceed the document's high-water mark (*ErrAboveMax);
//   - for the pair's first confirmation it may not be below the compaction
//     boundary, equality allowed (*ErrBelowBoundary);
//   - a later confirmation may not regress below the recorded cursor
//     (*ErrRegress).
//
// An unknown document has boundary and high-water mark zero, so only a zero
// cursor can be confirmed; recording it creates no document row. Confirming a
// cursor above the recorded one inserts/updates the row (Advanced=true);
// re-confirming the recorded cursor writes nothing and reports Advanced=false.
// No confirmation allocates a change cursor or notifies a waiter or
// subscriber. The outcome is durable before the call returns.
func (s *Service) Put(sessionID, documentID string, cursor int64) (PutResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return PutResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	deviceID, err := sessionDeviceTx(tx, sessionID)
	if err != nil {
		return PutResult{}, err
	}
	if err := s.authz.AuthorizedTx(tx, documentID, deviceID); err != nil {
		return PutResult{}, err
	}

	boundary, maxCursor, err := s.curs.DocumentCursorInfoTx(tx, documentID)
	if err != nil {
		return PutResult{}, err
	}
	if cursor > maxCursor {
		return PutResult{}, &ErrAboveMax{Max: maxCursor}
	}

	var recorded sql.NullInt64
	if err := tx.QueryRow(
		`SELECT cursor FROM session_checkpoints WHERE session_id = ? AND document_id = ?`,
		sessionID, documentID,
	).Scan(&recorded); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return PutResult{}, err
		}
	}

	if recorded.Valid {
		switch {
		case cursor < recorded.Int64:
			return PutResult{}, &ErrRegress{Current: recorded.Int64}
		case cursor == recorded.Int64:
			// A repeat confirmation is idempotent: the stored row is left
			// exactly as it was and nothing is written.
			return PutResult{Cursor: cursor, Advanced: false}, tx.Commit()
		}
	} else if cursor < boundary {
		// The first confirmation must be reproducible: below the boundary the
		// changes have left the online log, so the client restores the snapshot
		// before it can confirm. Equality with the boundary is permitted.
		return PutResult{}, &ErrBelowBoundary{Boundary: boundary}
	}

	if _, err := tx.Exec(
		`INSERT INTO session_checkpoints (session_id, document_id, cursor) VALUES (?, ?, ?)
		 ON CONFLICT (session_id, document_id) DO UPDATE SET cursor = excluded.cursor`,
		sessionID, documentID, cursor,
	); err != nil {
		return PutResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PutResult{}, err
	}
	return PutResult{Cursor: cursor, Advanced: true}, nil
}

// Get reads the (sessionID, documentID) pair's checkpoint together with the
// document's current compaction boundary and high-water mark. Session
// existence (store.ErrSessionNotFound) and document permission
// (store.ErrPermissionDenied) are checked first inside one serialized
// transaction. A pair that has never been confirmed answers Recorded=false
// with Cursor zero; an unknown document answers boundary and high-water mark
// zero. The read writes nothing, allocates no cursor and notifies nobody.
func (s *Service) Get(sessionID, documentID string) (GetResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return GetResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	deviceID, err := sessionDeviceTx(tx, sessionID)
	if err != nil {
		return GetResult{}, err
	}
	if err := s.authz.AuthorizedTx(tx, documentID, deviceID); err != nil {
		return GetResult{}, err
	}

	var result GetResult
	var recorded sql.NullInt64
	if err := tx.QueryRow(
		`SELECT cursor FROM session_checkpoints WHERE session_id = ? AND document_id = ?`,
		sessionID, documentID,
	).Scan(&recorded); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return GetResult{}, err
		}
	}
	if recorded.Valid {
		result.Recorded = true
		result.Cursor = recorded.Int64
	}

	boundary, maxCursor, err := s.curs.DocumentCursorInfoTx(tx, documentID)
	if err != nil {
		return GetResult{}, err
	}
	result.Boundary = boundary
	result.MaxCursor = maxCursor

	return result, tx.Commit()
}

// sessionDeviceTx resolves the live session's owning device inside the
// caller's serialized transaction; a missing or deleted session yields
// store.ErrSessionNotFound.
func sessionDeviceTx(q store.DBTX, sessionID string) (string, error) {
	var deviceID string
	err := q.QueryRow(
		`SELECT device_id FROM sessions WHERE id = ?`, sessionID,
	).Scan(&deviceID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSessionNotFound
	}
	if err != nil {
		return "", err
	}
	return deviceID, nil
}

// DeleteSessionTx removes every checkpoint row of sessionID inside the
// caller's transaction: a deleted session leaves no consumption position
// behind, so the id later created again starts unconfirmed.
func (s *Service) DeleteSessionTx(q store.DBTX, sessionID string) error {
	_, err := q.Exec(`DELETE FROM session_checkpoints WHERE session_id = ?`, sessionID)
	return err
}

// DeleteDeviceTx removes the checkpoint rows of every session owned by
// deviceID inside the caller's transaction. It must run while the device's
// sessions still exist (the rows are matched through them); the registration
// cascade removes those sessions afterward in the same serialized judgment.
func (s *Service) DeleteDeviceTx(q store.DBTX, deviceID string) error {
	_, err := q.Exec(
		`DELETE FROM session_checkpoints
		 WHERE session_id IN (SELECT id FROM sessions WHERE device_id = ?)`,
		deviceID,
	)
	return err
}

// DeleteDocumentTx removes every checkpoint row naming documentID inside the
// caller's transaction, so a deleted document — whose cursor space restarts at
// 1 — leaves no stale positions behind.
func (s *Service) DeleteDocumentTx(q store.DBTX, documentID string) error {
	_, err := q.Exec(`DELETE FROM session_checkpoints WHERE document_id = ?`, documentID)
	return err
}

// RevokeDocumentForDeviceTx removes the checkpoint rows of every session owned
// by deviceID on documentID inside the caller's transaction, so after a
// permission revoke the device re-confirms from the unconfirmed state even if
// it is later re-authorized. It runs as part of the revoke's serialized
// judgment, before the revoke commits.
func (s *Service) RevokeDocumentForDeviceTx(q store.DBTX, documentID, deviceID string) error {
	_, err := q.Exec(
		`DELETE FROM session_checkpoints
		 WHERE document_id = ?
		   AND session_id IN (SELECT id FROM sessions WHERE device_id = ?)`,
		documentID, deviceID,
	)
	return err
}

// Compile-time checks that events.Service supplies the document cursor facts
// and that the concrete database types satisfy the shared DBTX interface.
var _ CursorFacts = (*events.Service)(nil)
var _ store.DBTX = (*sql.Tx)(nil)

// RevokeHook adapts the checkpoint service to authz.RevokeTxHook so a genuine
// permission revoke clears the revoked device's session checkpoints on the
// document inside the revoke's own transaction.
type RevokeHook struct{ svc *Service }

// NewRevokeHook returns the in-transaction revoke hook for s, registered with
// the permission service at composition time.
func NewRevokeHook(s *Service) RevokeHook { return RevokeHook{svc: s} }

// PermissionRevokingTx implements authz.RevokeTxHook.
func (h RevokeHook) PermissionRevokingTx(q authz.Tx, documentID, deviceID string) error {
	return h.svc.RevokeDocumentForDeviceTx(q, documentID, deviceID)
}

var _ authz.RevokeTxHook = RevokeHook{}
