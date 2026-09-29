// The device document listing is the change event service's read-only answer
// to "which documents does this device still have outstanding changes in":
// every document that currently carries an online change the device
// originated, each at most once. Like every other read it sees only the
// online change log — the retained summaries in change_identities exist for
// idempotency alone and never participate in reads — so a document whose
// changes were all compacted away, or that was deleted outright, no longer
// appears. The read allocates no cursor and notifies no waiter or subscriber.

package events

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// ListDeviceDocuments returns the page of document ids that currently carry
// an online change originated by deviceID — the documents the device's
// sessions once wrote changes to and whose change data still exists — each
// appearing at most once, sorted lexicographically by document id ascending
// and paged by limit/offset as a slice over that fixed order. With no
// intervening commit or deletion, successive pages neither repeat nor skip
// an id, and an offset past the end yields an empty (non-nil) slice and no
// error.
//
// An unregistered device yields store.ErrDeviceNotFound and no listing. The
// read runs in one serialized transaction and writes nothing: a fully
// compacted document has no online rows and is absent, exactly as a deleted
// document is after its rows were removed.
func (s *Service) ListDeviceDocuments(deviceID string, limit, offset int64) ([]string, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	exists, err := store.DeviceExistsTx(tx, deviceID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrDeviceNotFound
	}

	rows, err := tx.Query(
		`SELECT DISTINCT document_id FROM changes
		 WHERE device_id = ?
		 ORDER BY document_id ASC
		 LIMIT ? OFFSET ?`,
		deviceID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	return ids, tx.Commit()
}
