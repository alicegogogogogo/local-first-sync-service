// Package permission is the document-permission service: it persists the
// per-(document, device) authorization state and is the single place that
// decides whether a device currently holds permission for a document.
//
// Every (document, device) pair starts authorized; a stored row records a
// deviation from that default. Writes are serialized in one transaction and
// committed to disk, so concurrent grant/revoke calls each land as a complete
// state and survive a restart. Revoking never deletes anything outside this
// service's own table; it only flips the stored decision.
//
// The service owns exactly one table (document_permissions) and never reads
// or writes another service's storage. The two things it cannot decide on its
// own arrive through interfaces:
//
//   - DeviceRegistry: the registration-layer existence check a write enforces
//     inside its own serialized transaction.
//   - OnRevoke hooks: the notification channel a committed revoke is
//     published on, so subscribers living in other services can be ended
//     without this service knowing they exist.
//
// Other services enforce the revocation decision inside their own serialized
// transactions through CheckTx, so a commit, a compaction and a permission
// change are always judged against one consistent state.
package permission

import (
	"database/sql"
	"errors"
	"sync"
)

// ErrPermissionDenied reports that a device's access to the document has been
// revoked. The caller maps it to 403; nothing is written and no content is
// exposed.
var ErrPermissionDenied = errors.New("device permission for this document has been revoked")

// DeviceRegistry is the registration-layer boundary the service consults
// before recording a permission change. RequireTx runs inside the service's
// own transaction and must return a non-nil error — propagated verbatim to
// the caller — when deviceID is not a registered device.
type DeviceRegistry interface {
	RequireTx(tx *sql.Tx, deviceID string) error
}

// schema is the service's own table. No other service reads or writes it.
const schema = `
CREATE TABLE IF NOT EXISTS document_permissions (
	document_id TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	authorized  INTEGER NOT NULL,
	PRIMARY KEY (document_id, device_id)
);
`

// Service persists and decides document permissions.
type Service struct {
	db       *sql.DB
	registry DeviceRegistry

	// mu guards revokeHooks. Hooks fire synchronously after a revoke commits,
	// outside any database transaction.
	mu          sync.Mutex
	revokeHooks []func(documentID, deviceID string)
}

// New opens the permission service on db, creating its table if needed. A nil
// registry skips the device-existence check on writes, which is useful when
// the service is embedded without a registration layer.
func New(db *sql.DB, registry DeviceRegistry) (*Service, error) {
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{db: db, registry: registry}, nil
}

// OnRevoke registers a hook fired synchronously, after the commit, each time
// a call to Set actually changes a (document, device) pair from authorized to
// revoked. A grant, and a revoke that changes nothing, fire no hook. Hooks
// are the service's only notification channel: it neither knows nor touches
// any subscriber registry.
func (s *Service) OnRevoke(hook func(documentID, deviceID string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revokeHooks = append(s.revokeHooks, hook)
}

// Set grants or revokes deviceID's access to documentID and reports whether
// the stored permission actually changed.
//
// Every device starts authorized for every document, so the first revoke is
// the first write for the pair (changed=true); repeating a state that already
// holds writes nothing and reports changed=false. When a registry is
// configured, an unregistered device fails the call with the registry's error
// and nothing is written. The lookup and the write run in one serialized
// transaction, so concurrent grant/revoke calls each commit completely and
// the final state matches the last committed action.
func (s *Service) Set(documentID, deviceID string, authorized bool) (changed bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	if s.registry != nil {
		if err := s.registry.RequireTx(tx, deviceID); err != nil {
			return false, err
		}
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
	// A grant cannot unblock a revoked subscriber (the first revoke already
	// ended it, stickily); only a revoke is published.
	if !authorized {
		s.fireRevoke(documentID, deviceID)
	}
	return true, nil
}

// fireRevoke publishes a committed revoke to every registered hook, in
// registration order.
func (s *Service) fireRevoke(documentID, deviceID string) {
	s.mu.Lock()
	hooks := make([]func(string, string), len(s.revokeHooks))
	copy(hooks, s.revokeHooks)
	s.mu.Unlock()
	for _, hook := range hooks {
		hook(documentID, deviceID)
	}
}

// Authorized reports whether deviceID currently holds permission for
// documentID. Devices start authorized, so an absent row means authorized.
func (s *Service) Authorized(documentID, deviceID string) (bool, error) {
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

// CheckTx is the unified revoke decision other services enforce inside their
// own serialized transactions: it returns ErrPermissionDenied when the pair
// is currently revoked and nil otherwise. It reads only this service's table,
// so a caller's commit is judged against the same state its transaction
// commits into.
func (s *Service) CheckTx(tx *sql.Tx, documentID, deviceID string) error {
	var stored int
	err := tx.QueryRow(
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
			return ErrPermissionDenied
		}
		return nil
	}
}
