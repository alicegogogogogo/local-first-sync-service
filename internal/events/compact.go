// Change-log compaction bounds the storage a long-lived document accumulates
// in the online change log. Reads only ever need the changes after a client's
// cursor, and snapshots already pin the states a client can restart from, so
// the changes at or below the greatest snapshot cursor no longer serve any
// read and can move out of the online log.
//
// A trimmed change is not forgotten: its id keeps a small fixed-size identity
// — the originating device, a canonical digest of the payload and the restore
// provenance, and the cursor it was first assigned — so the submission paths
// keep answering idempotency and conflict exactly as before. The identity
// never participates in reads: listings, long polls and subscriptions simply
// never see the trimmed rows again, and the digest table holds one small
// record per trimmed id instead of the full payload, so storage does not grow
// back to its pre-compaction size.
//
// Compaction itself writes no change record, allocates no cursor and notifies
// no waiter or subscriber: the online log only shrinks. The cursor space is
// never reset — the high-water mark is persisted next to the boundary, so the
// next change after a compaction (or after a restart) continues one past the
// greatest cursor ever allocated.

package events

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
)

// changeDigest renders the comparable content of one change — its decoded
// payload and its restore provenance (the snapshot cursor it was restored
// from, 0 for an ordinary change) — in a single canonical form and hashes it.
// Two submissions of one id carry the same digest exactly when the existing
// equality rules accept the second as idempotent: the payload is compared
// JSON-semantically (1 and 1.0 and reordered object keys compare equal, the
// same rule store.JSONEqual applies) and the restore source numerically.
func changeDigest(payload json.RawMessage, restoredFrom int64) []byte {
	// Re-marshaling the decoded value canonicalizes whitespace, number
	// formatting and object key order. Callers only store validated JSON, so
	// the decode cannot fail; a hypothetical failure canonicalizes to null.
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		value = nil
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		canonical = []byte("null")
	}

	h := sha256.New()
	_, _ = h.Write([]byte("change"))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(canonical)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatInt(restoredFrom, 10)))
	return h.Sum(nil)
}

// currentCursorTx returns the document's cursor high-water mark: the greatest
// cursor ever allocated, which survives compaction trimming the newest online
// row. It is the ordinary table maximum raised by the persisted high-water
// mark, and 0 for an unknown document.
func currentCursorTx(tx *sql.Tx, documentID string) (int64, error) {
	var maxOnline int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&maxOnline); err != nil {
		return 0, err
	}
	var highWater int64
	err := tx.QueryRow(
		`SELECT high_water FROM change_log_state WHERE document_id = ?`,
		documentID,
	).Scan(&highWater)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return maxOnline, nil
	case err != nil:
		return 0, err
	case highWater > maxOnline:
		return highWater, nil
	default:
		return maxOnline, nil
	}
}

// compactionBoundaryTx returns the persisted compaction boundary of the
// document: the greatest snapshot cursor as of the last compaction, 0 when
// the log was never compacted. Changes at or below it are no longer online.
func compactionBoundaryTx(tx *sql.Tx, documentID string) (int64, error) {
	var boundary int64
	err := tx.QueryRow(
		`SELECT boundary FROM change_log_state WHERE document_id = ?`,
		documentID,
	).Scan(&boundary)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return boundary, nil
	}
}

// compactionBoundary is the read-path variant of compactionBoundaryTx.
func (s *Service) compactionBoundary(documentID string) (int64, error) {
	var boundary int64
	err := s.db.QueryRow(
		`SELECT boundary FROM change_log_state WHERE document_id = ?`,
		documentID,
	).Scan(&boundary)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, err
	default:
		return boundary, nil
	}
}

// resolveTrimmedIdentity answers the idempotency question for a change id
// whose online row is absent. It returns the retained first-assigned cursor
// and true when a trimmed identity matches the submission's device and
// comparable content (payload plus restore provenance), an *ErrConflict when
// a retained identity exists but differs, and false with no error when no
// identity was retained (a genuinely new id).
func resolveTrimmedIdentity(tx *sql.Tx, documentID, id, deviceID string, payload json.RawMessage, restoredFrom int64) (int64, bool, error) {
	var existingDevice string
	var existingDigest []byte
	var existingCursor int64
	err := tx.QueryRow(
		`SELECT device_id, digest, cursor FROM change_identities WHERE document_id = ? AND id = ?`,
		documentID, id,
	).Scan(&existingDevice, &existingDigest, &existingCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	}

	if existingDevice != deviceID || !bytes.Equal(existingDigest, changeDigest(payload, restoredFrom)) {
		return 0, false, &ErrConflict{ID: id}
	}
	return existingCursor, true, nil
}

// Compact moves every change of documentID whose cursor is at or below the
// compaction boundary out of the online log, in one serialized transaction,
// and reports the boundary and the number of rows removed.
//
// The boundary is the greatest cursor among the document's saved snapshots; a
// document with no snapshot compacts to boundary 0 and removes nothing, which
// is still a success. The gate is enforced exactly as in ReplayChanges and
// before any change content is observed: an unregistered device yields
// store.ErrDeviceNotFound and a revoked device yields
// store.ErrPermissionDenied, and neither touches any state.
//
// Each trimmed id keeps its identity (device, canonical payload-plus-restore
// digest, first cursor) so re-submission stays idempotent or conflicting
// exactly as before; the trimmed rows and their restore provenance records
// are deleted, so storage does not grow back. The persisted high-water mark
// keeps the cursor space continuous across the trim and across restarts.
//
// Compaction is idempotent — a repeat finds nothing at or below the boundary
// and reports the same boundary with zero removed — and it never notifies a
// waiter or subscriber: it writes no change and allocates no cursor.
func (s *Service) Compact(documentID, deviceID string) (CompactResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return CompactResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration and permission are enforced before any change content is
	// observed, so a rejected compaction cannot reveal the log.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return CompactResult{}, err
		}
	}

	// The boundary is the greatest snapshot cursor. Snapshots are never
	// deleted, so the boundary never retreats and a repeat compaction reports
	// the same one.
	var boundary int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM snapshots WHERE document_id = ?`,
		documentID,
	).Scan(&boundary); err != nil {
		return CompactResult{}, err
	}
	if boundary == 0 {
		// No snapshot: nothing is eligible to leave the online log. This is a
		// successful no-op and writes nothing — not even a state row.
		return CompactResult{Boundary: 0, Removed: 0}, nil
	}

	// The high-water mark must be captured before the trim: when the boundary
	// reaches the newest row, the online table alone would forget it.
	highWater, err := currentCursorTx(tx, documentID)
	if err != nil {
		return CompactResult{}, err
	}

	rows, err := tx.Query(
		`SELECT c.id, c.device_id, c.payload, c.cursor, COALESCE(r.snapshot_cursor, 0)
		 FROM changes c
		 LEFT JOIN restores r
		   ON r.document_id = c.document_id AND r.change_id = c.id
		 WHERE c.document_id = ? AND c.cursor <= ?
		 ORDER BY c.cursor ASC`,
		documentID, boundary,
	)
	if err != nil {
		return CompactResult{}, err
	}
	type trimmed struct {
		id           string
		deviceID     string
		payload      []byte
		cursor       int64
		restoredFrom int64
	}
	var doomed []trimmed
	for rows.Next() {
		var t trimmed
		if err := rows.Scan(&t.id, &t.deviceID, &t.payload, &t.cursor, &t.restoredFrom); err != nil {
			_ = rows.Close()
			return CompactResult{}, err
		}
		doomed = append(doomed, t)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return CompactResult{}, err
	}
	_ = rows.Close()

	for _, t := range doomed {
		// A row is trimmed at most once, so its id is normally absent;
		// INSERT OR IGNORE keeps a repeat compaction over the same rows a
		// harmless no-op.
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO change_identities (document_id, id, device_id, digest, cursor)
			 VALUES (?, ?, ?, ?, ?)`,
			documentID, t.id, t.deviceID,
			changeDigest(t.payload, t.restoredFrom), t.cursor,
		); err != nil {
			return CompactResult{}, err
		}
	}

	res, err := tx.Exec(
		`DELETE FROM changes WHERE document_id = ? AND cursor <= ?`,
		documentID, boundary,
	)
	if err != nil {
		return CompactResult{}, err
	}
	removed, err := res.RowsAffected()
	if err != nil {
		return CompactResult{}, err
	}
	// The restore provenance of a trimmed change leaves with it; its
	// idempotency is served by the retained identity from then on.
	if _, err := tx.Exec(
		`DELETE FROM restores WHERE document_id = ? AND change_cursor <= ?`,
		documentID, boundary,
	); err != nil {
		return CompactResult{}, err
	}

	if _, err := tx.Exec(
		`INSERT INTO change_log_state (document_id, boundary, high_water) VALUES (?, ?, ?)
		 ON CONFLICT(document_id) DO UPDATE SET boundary = excluded.boundary, high_water = excluded.high_water`,
		documentID, boundary, highWater,
	); err != nil {
		return CompactResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return CompactResult{}, err
	}
	return CompactResult{Boundary: boundary, Removed: removed}, nil
}
