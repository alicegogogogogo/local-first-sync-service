// The device pending-change statistics are the change event service's
// read-only answer to "how much does this device still have online per
// document": one row per document that currently carries an online change the
// device originated, carrying the document id, the count of online changes and
// the largest cursor among them. It takes the exact same data scope as the
// device document listing — only the online changes table; the retained
// summaries in change_identities exist for idempotency alone and never
// participate in reads — so a document whose changes were all compacted away,
// or that was deleted outright, no longer appears. The read allocates no
// cursor and notifies no waiter or subscriber.

package events

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// PendingStat is one row of a ListDevicePending result: one document that
// still carries online changes originated by the device, the number of those
// changes and the largest cursor among them.
type PendingStat struct {
	DocumentID  string `json:"documentId"`
	ChangeCount int64  `json:"changeCount"`
	MaxCursor   int64  `json:"maxCursor"`
}

// ListDevicePending returns the page of per-document pending statistics over
// the online changes originated by deviceID. Each document carrying at least
// one online row appears at most once, sorted lexicographically by document
// id ascending and paged by limit/offset as a slice over that fixed order;
// each entry's count and maximum cursor count only rows still present in the
// online change log. With no intervening commit or deletion, successive pages
// neither repeat nor skip an id, and an offset past the end yields an empty
// (non-nil) slice and no error.
//
// An unregistered device yields store.ErrDeviceNotFound and no statistics. The
// read runs in one serialized transaction and writes nothing.
func (s *Service) ListDevicePending(deviceID string, limit, offset int64) ([]PendingStat, error) {
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
		`SELECT document_id, COUNT(*), MAX(cursor) FROM changes
		 WHERE device_id = ?
		 GROUP BY document_id
		 ORDER BY document_id ASC
		 LIMIT ? OFFSET ?`,
		deviceID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	stats := make([]PendingStat, 0)
	for rows.Next() {
		var row PendingStat
		if err := rows.Scan(&row.DocumentID, &row.ChangeCount, &row.MaxCursor); err != nil {
			_ = rows.Close()
			return nil, err
		}
		stats = append(stats, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	return stats, tx.Commit()
}
