// Package authz is the permission service: the durable, per-document and
// per-device authorization ledger together with the system's single revoke
// verdict.
//
// Every (document, device) pair starts authorized; a grant or revoke is
// recorded per pair. The lookup and the write run in one serialized
// transaction on the shared connection and commit to disk, so concurrent
// grant/revoke calls each land as a complete state and survive a restart.
//
// The other services do not read the permission table. They take their
// registration-and-authorization verdict through AuthorizedTx, evaluated
// inside their own write transaction, so a rejected request is judged before
// any document content is observed and the gate and the gated work commit as
// one judgment. Revoking never deletes changes, snapshots, sessions or CRDT
// state; it only produces the denial and, after the revoke commits, notifies
// the registered revoke sinks (the live subscription registries) so they can
// end the affected connections with the one-time, sticky 4403 close.
package authz

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// schema is created when the service is constructed; its shape is unchanged
// from the pre-refactor layout, so authorization rows already on disk stay
// readable.
const schema = `
CREATE TABLE IF NOT EXISTS document_permissions (
	document_id TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	authorized  INTEGER NOT NULL,
	PRIMARY KEY (document_id, device_id)
);
`

// RevokeSink observes a committed revoke. The change event service and the
// CRDT service register themselves to end the affected live subscriptions; a
// sink must not block and must be safe to call after the revoke transaction
// has committed. The permission service depends on this interface alone, so
// the dependency points from the observers toward the verdict and never back.
type RevokeSink interface {
	PermissionRevoked(documentID, deviceID string)
}

// Service is the permission service.
type Service struct {
	db *sql.DB

	mu    sync.Mutex
	sinks []RevokeSink
}

// New constructs the permission service over the shared kernel handle and
// creates its table.
func New(kernel *store.Store) (*Service, error) {
	db := kernel.DB()
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{db: db}, nil
}

// AddRevokeSink registers an observer notified after every committed revoke.
// It is called by the composition root while wiring the services, before the
// surface serves requests.
func (s *Service) AddRevokeSink(sink RevokeSink) {
	s.mu.Lock()
	s.sinks = append(s.sinks, sink)
	s.mu.Unlock()
}

// AuthorizedTx is the shared gate: it returns nil when deviceID is registered
// and currently authorized for documentID, store.ErrDeviceNotFound when the
// device is not registered and store.ErrPermissionDenied when its access has
// been revoked. It runs inside the caller's transaction q, so the verdict is
// taken on the same serialized connection as the work it guards, before any
// document content is read.
func (s *Service) AuthorizedTx(q store.DBTX, documentID, deviceID string) error {
	exists, err := store.DeviceExistsTx(q, deviceID)
	if err != nil {
		return err
	}
	if !exists {
		return store.ErrDeviceNotFound
	}
	var stored int
	err = q.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
		return nil
	case err != nil:
		return err
	default:
		if stored == 0 {
			return store.ErrPermissionDenied
		}
		return nil
	}
}

// DeviceAuthorizedTx adapts the shared gate to the change event and CRDT
// services' Gate interface (which names *sql.Tx). The verdict is identical.
func (s *Service) DeviceAuthorizedTx(tx *sql.Tx, documentID, deviceID string) error {
	return s.AuthorizedTx(tx, documentID, deviceID)
}

// SetDocumentPermission grants or revokes deviceID's access to documentID and
// reports whether the stored permission actually changed.
//
// Every device starts authorized for every document, so the first revoke is
// the first write for the pair (changed=true); repeating a state that already
// holds writes nothing and reports changed=false. An unregistered device
// yields store.ErrDeviceNotFound and nothing is written. The lookup and the
// write run in one serialized transaction, so concurrent grant/revoke calls
// each commit completely and the final state matches the last committed
// action. After a revoke commits, every registered sink is notified; a grant
// notifies nothing (a prior revoke already ended the connection stickily).
func (s *Service) SetDocumentPermission(documentID, deviceID string, authorized bool) (changed bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	exists, err := store.DeviceExistsTx(tx, deviceID)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, store.ErrDeviceNotFound
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
		s.mu.Lock()
		sinks := append([]RevokeSink(nil), s.sinks...)
		s.mu.Unlock()
		for _, sink := range sinks {
			sink.PermissionRevoked(documentID, deviceID)
		}
	}
	return true, nil
}

// DeleteDevicePermissionsTx removes every document-permission ledger row that
// names deviceID, inside the caller's transaction: grants and revokes the
// deregistered device held on documents vanish together with the device. It is
// the permission service's share of device deregistration, run in the same
// serialized transaction as the registration layer's cascade, so the row
// removals and the device deletion commit as one judgment. After the device
// row is gone the default verdict would be "unregistered" anyway; clearing the
// rows guarantees a device that later re-registers the same id starts with no
// inherited authorization state.
func (s *Service) DeleteDevicePermissionsTx(q store.DBTX, deviceID string) error {
	_, err := q.Exec(
		`DELETE FROM document_permissions WHERE device_id = ?`, deviceID,
	)
	return err
}

// DocumentAuthorized reports whether deviceID currently holds permission for
// documentID. Devices start authorized, so an absent row means authorized.
func (s *Service) DocumentAuthorized(documentID, deviceID string) (bool, error) {
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

// PermissionEntry is one row of a document's permission ledger: a currently
// registered device id together with its authorization for the document.
type PermissionEntry struct {
	DeviceID   string
	Authorized bool
}

// ListDocumentPermissions is the read-only ledger view of one document: one
// entry per currently registered device, in ascending lexicographic device-id
// order, paged by limit/offset against that same order.
//
// A device with no ledger row starts authorized, so its entry reports
// Authorized=true; a device whose latest committed action was a revoke reports
// false. Deregistered devices carry no device row and their ledger rows were
// removed by the deregistration cascade, so they never appear. An unknown
// document is indistinguishable from one on which every device keeps the
// default: every registered device lists authorized. The read runs in one
// serialized transaction, creates no row, moves no cursor and notifies no sink.
func (s *Service) ListDocumentPermissions(documentID string, limit, offset int64) ([]PermissionEntry, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(
		`SELECT d.id, COALESCE(p.authorized, 1) AS authorized
		 FROM devices d
		 LEFT JOIN document_permissions p
		   ON p.document_id = ? AND p.device_id = d.id
		 ORDER BY d.id ASC
		 LIMIT ? OFFSET ?`,
		documentID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	entries := make([]PermissionEntry, 0)
	for rows.Next() {
		var entry PermissionEntry
		var stored int
		if err := rows.Scan(&entry.DeviceID, &stored); err != nil {
			return nil, err
		}
		entry.Authorized = stored != 0
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, tx.Commit()
}
