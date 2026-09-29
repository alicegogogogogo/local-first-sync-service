// Read-only device-scoped document listing: the change-event service's share
// of GET /v1/devices/{deviceId}/documents. It answers which documents still
// carry an online change stamped with a given device; the registration layer
// separately owns the device-existence and session judgments, so this file
// touches only the change log.

package events

import "github.com/alicegogogogogo/local-first-sync-service/internal/store"

// ListDeviceDocumentsTx returns the distinct document ids that still carry an
// online change whose originating device is deviceID, each at most once,
// sorted by document id in ascending lexicographic order and paged by
// limit/offset as a slice over that fixed order — so with no intervening
// commit or delete, successive pages neither repeat nor skip an id, and an
// offset past the end yields an empty (non-nil) slice and no error.
//
// Only the online change log is consulted: a document whose changes were all
// moved out by compaction leaves the listing even though its retained
// idempotency summaries remain (its changes have been cleared), and a
// whole-document deletion removes every row the set could be derived from.
// The caller decides whether the device exists and whether it currently owns
// a session — those rows belong to the registration layer — and runs the
// whole judgment in q, one serialized transaction. The read writes nothing,
// allocates no cursor and notifies no waiter or subscriber; its page is
// stable across repeated reads and process restarts while the committed log
// is unchanged.
func (s *Service) ListDeviceDocumentsTx(q store.DBTX, deviceID string, limit, offset int64) ([]string, error) {
	rows, err := q.Query(
		`SELECT DISTINCT document_id FROM changes
		 WHERE device_id = ?
		 ORDER BY document_id ASC
		 LIMIT ? OFFSET ?`,
		deviceID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}
