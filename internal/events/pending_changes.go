// The device pending-change statistics read is the change event service's
// read-only answer to "for each of this device's documents, how many online
// changes it originated are still outstanding and where does the online log
// end": one row per document that currently carries an online change the
// device originated, each carrying the document id, the count of online rows
// it originated and the largest cursor among them. Like every other read it
// sees only the online change log — the retained summaries in
// change_identities exist for idempotency alone and never participate in
// reads — so a document whose changes were all compacted away, or that was
// deleted outright, no longer appears. The read allocates no cursor and
// notifies no waiter or subscriber.

package events

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// PendingChangeStat is one document's pending-change statistics: the document
// id, the number of online changes that device originated in it (Count, never
// zero for a row that appears) and the largest cursor among those online
// changes (MaxCursor).
type PendingChangeStat struct {
	DocumentID string
	Count      int64
	MaxCursor  int64
}

// ListDevicePendingChanges returns one stat per document that currently
// carries an online change originated by deviceID — the documents the
// device's sessions once wrote changes to and whose change data still exists
// — each appearing at most once, sorted lexicographically by document id
// ascending and paged by limit/offset as a slice over that fixed order. With
// no intervening commit or deletion, successive pages neither repeat nor
// skip an id, and an offset past the end yields an empty (non-nil) slice and
// no error.
//
// The count and the maximum cursor count only the device's changes still in
// the online log, never the retained summaries compaction leaves behind. An
// unregistered device yields store.ErrDeviceNotFound and no statistics. The
// read runs in one serialized transaction and writes nothing: a fully
// compacted document has no online rows and is absent, exactly as a deleted
// document is after its rows were removed.
func (s *Service) ListDevicePendingChanges(deviceID string, limit, offset int64) ([]PendingChangeStat, error) {
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
	stats := make([]PendingChangeStat, 0)
	for rows.Next() {
		var stat PendingChangeStat
		if err := rows.Scan(&stat.DocumentID, &stat.Count, &stat.MaxCursor); err != nil {
			_ = rows.Close()
			return nil, err
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()

	return stats, tx.Commit()
}
