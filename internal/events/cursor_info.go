package events

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// DocumentCursorInfoTx reports the two cursor facts a session checkpoint
// judgment needs, read inside the caller's serialized transaction:
//
//   - boundary: the stored compaction boundary (the greatest cursor whose
//     change rows compaction moved out of the online log), zero when the
//     document was never compacted;
//   - maxCursor: the document's high-water mark — the greatest online change
//     cursor, or the boundary when every change up to it has left the online
//     log. An unknown document answers (0, 0).
//
// The boundary is the stored compaction boundary rather than the greatest
// snapshot cursor: until compaction actually trims the log, a client can still
// page through every change and need not restore a snapshot.
func (s *Service) DocumentCursorInfoTx(q store.DBTX, documentID string) (boundary, maxCursor int64, err error) {
	boundary, err = boundaryOfTx(q, documentID)
	if err != nil {
		return 0, 0, err
	}
	var maxOnline int64
	if err := q.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&maxOnline); err != nil {
		return 0, 0, err
	}
	maxCursor = maxOnline
	if boundary > maxCursor {
		maxCursor = boundary
	}
	return boundary, maxCursor, nil
}
