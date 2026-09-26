// Change-log compaction moves the old end of a document's change log out of
// the online tables without touching the cursor space the log shares with
// every write path.
//
// The compaction boundary is the greatest cursor that has a saved snapshot
// (zero when the document has none): everything at or below it is reproducible
// from a snapshot plus the surviving tail, so those rows leave the online log.
// A trimmed change is not forgotten, though — its id keeps a small canonical
// summary (the originating device, a digest of the payload and, for a restore,
// the snapshot it came from, plus the cursor it was first assigned) so the
// submission layer keeps answering idempotency and conflict exactly as before.
// The summary never participates in reads: pagination, long polling and
// subscriptions simply no longer see the trimmed rows, and the summary's
// fixed size means storage does not grow back to the pre-compaction shape.
//
// Compaction itself is not a change: it allocates no cursor, writes no change
// record and wakes no waiter or subscriber. The cursor space never resets —
// the next allocated cursor is one past the greater of the online maximum and
// the boundary — and the boundary, the summaries and the read results are
// durable across restarts.

package events

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// CompactChanges moves every change of documentID whose cursor is at or below
// the compaction boundary out of the online log, in one serialized
// transaction, and returns the boundary together with the number of changes
// removed by this call.
//
// The boundary is the greatest cursor with a saved snapshot, or zero when the
// document has no snapshot — compacting a snapshot-less or unknown document
// succeeds and removes nothing. The gate is enforced before any content is
// observed, exactly as in ReplayChanges: an unregistered device yields
// store.ErrDeviceNotFound and a revoked device yields store.ErrPermissionDenied,
// and neither touches any state.
//
// Each trimmed id keeps its canonical summary (device, payload digest, restore
// provenance and first cursor) so re-posting it stays idempotent and a
// differing re-post stays a conflict. Compaction is idempotent: a repeat finds
// the same boundary, trims nothing and reports zero removed. It commits no
// change row, allocates no cursor and notifies no waiter or subscriber.
func (s *Service) CompactChanges(documentID, deviceID string) (boundary, removed int64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration and permission are enforced before any change content is
	// observed, so a rejected compaction cannot reveal anything.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return 0, 0, err
		}
	}

	// The boundary is the greatest cursor with a saved snapshot, 0 when the
	// document has none (an unknown document included).
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM snapshots WHERE document_id = ?`,
		documentID,
	).Scan(&boundary); err != nil {
		return 0, 0, err
	}
	stored, err := boundaryOfTx(tx, documentID)
	if err != nil {
		return 0, 0, err
	}
	if boundary <= stored {
		// Nothing new to trim (a repeat compaction, or a snapshot-less
		// document): report the standing boundary and remove nothing.
		return stored, 0, tx.Commit()
	}

	// Collect the rows leaving the online log before deleting them: each one
	// first moves its canonical summary into the identity table.
	type trimmed struct {
		cursor  int64
		id      string
		device  string
		payload []byte
	}
	rows, err := tx.Query(
		`SELECT cursor, id, device_id, payload FROM changes
		 WHERE document_id = ? AND cursor <= ?
		 ORDER BY cursor ASC`,
		documentID, boundary,
	)
	if err != nil {
		return 0, 0, err
	}
	var dropping []trimmed
	for rows.Next() {
		var t trimmed
		if err := rows.Scan(&t.cursor, &t.id, &t.device, &t.payload); err != nil {
			_ = rows.Close()
			return 0, 0, err
		}
		dropping = append(dropping, t)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, err
	}
	_ = rows.Close()

	for _, t := range dropping {
		// A restored change also carries its provenance into the summary, so a
		// repeated restore keeps its idempotency/conflict decision.
		var restoredFrom sql.NullInt64
		err := tx.QueryRow(
			`SELECT snapshot_cursor FROM restores WHERE document_id = ? AND change_id = ?`,
			documentID, t.id,
		).Scan(&restoredFrom)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			restoredFrom = sql.NullInt64{}
		case err != nil:
			return 0, 0, err
		}
		digest, err := payloadDigest(t.payload)
		if err != nil {
			return 0, 0, err
		}
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO change_identities
				(document_id, id, device_id, digest, cursor, restored_from)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			documentID, t.id, t.device, digest, t.cursor, restoredFrom,
		); err != nil {
			return 0, 0, err
		}
	}

	if _, err := tx.Exec(
		`DELETE FROM restores WHERE document_id = ? AND change_cursor <= ?`,
		documentID, boundary,
	); err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(
		`DELETE FROM changes WHERE document_id = ? AND cursor <= ?`,
		documentID, boundary,
	); err != nil {
		return 0, 0, err
	}
	if _, err := tx.Exec(
		`INSERT INTO change_boundaries (document_id, boundary) VALUES (?, ?)
		 ON CONFLICT (document_id) DO UPDATE SET boundary = excluded.boundary`,
		documentID, boundary,
	); err != nil {
		return 0, 0, err
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	// Compaction is not a change: no waiter is woken and no subscriber is
	// signaled, because no new change exists to observe.
	return boundary, int64(len(dropping)), nil
}

// boundaryOfTx returns the stored compaction boundary of documentID, or zero
// when the document was never compacted.
func boundaryOfTx(q store.DBTX, documentID string) (int64, error) {
	var boundary int64
	err := q.QueryRow(
		`SELECT boundary FROM change_boundaries WHERE document_id = ?`,
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

// currentCursorTx returns the document's high-water mark: the greatest online
// cursor, or the compaction boundary when every change up to it has left the
// online log. The cursor space never resets, so a fully trimmed document
// continues from its boundary rather than from zero.
func currentCursorTx(tx *sql.Tx, documentID string) (int64, error) {
	var maxOnline int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&maxOnline); err != nil {
		return 0, err
	}
	boundary, err := boundaryOfTx(tx, documentID)
	if err != nil {
		return 0, err
	}
	if boundary > maxOnline {
		maxOnline = boundary
	}
	return maxOnline, nil
}

// payloadDigest renders a payload in the canonical form JSONEqual compares —
// the decoded value re-encoded, so 1 and 1.0 or reordered object keys digest
// alike — and hashes it. The digest is the fixed-size stand-in for the payload
// in a trimmed change's retained summary.
func payloadDigest(payload json.RawMessage) ([]byte, error) {
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return sum[:], nil
}

// resolveTrimmedIdentity answers the submission question for an id whose
// online row is absent because compaction trimmed it. It returns found=true
// and the id's first cursor when the retained summary matches the incoming
// device and payload (an idempotent re-post), an *ErrConflict when a retained
// summary exists but differs, and found=false when no summary was retained (a
// genuinely new id).
func resolveTrimmedIdentity(tx *sql.Tx, documentID string, c Change) (found bool, cursor int64, err error) {
	var device string
	var digest []byte
	err = tx.QueryRow(
		`SELECT device_id, digest, cursor FROM change_identities WHERE document_id = ? AND id = ?`,
		documentID, c.ID,
	).Scan(&device, &digest, &cursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, 0, nil
	case err != nil:
		return false, 0, err
	}

	incoming, err := payloadDigest(c.Payload)
	if err != nil {
		return false, 0, err
	}
	if device != c.DeviceID || !bytes.Equal(digest, incoming) {
		return false, 0, &ErrConflict{ID: c.ID}
	}
	return true, cursor, nil
}
