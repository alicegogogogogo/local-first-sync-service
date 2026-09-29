package events

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// PutSnapshot stores state as the snapshot of documentID at cursor, creating
// it on first use and reporting whether this call created it.
//
// Only cursors of existing changes are valid snapshot points: an unknown
// document, a cursor below 1 or a cursor ahead of the document's current
// cursor yields ErrSnapshotBase and nothing is written. The current cursor is
// the document's high-water mark in the never-reset cursor space — the
// greater of the online maximum and the compaction boundary — so a cursor
// whose change has been trimmed by compaction is still a valid snapshot
// point, even after a full trim emptied the online log. Re-posting a state
// that decodes equal to the stored one is idempotent (created=false); a
// different state is an *ErrSnapshotConflict and the stored snapshot is left
// unchanged. Snapshots are independent of the change log: they neither move
// the document's cursor nor affect merges.
func (s *Service) PutSnapshot(documentID string, cursor int64, state json.RawMessage) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	current, err := currentCursorTx(tx, documentID)
	if err != nil {
		return false, err
	}
	if current == 0 || cursor < 1 || cursor > current {
		return false, ErrSnapshotBase
	}

	var existing []byte
	err = tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO snapshots (document_id, cursor, state) VALUES (?, ?, ?)`,
			documentID, cursor, []byte(state),
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	default:
		if !store.JSONEqual(existing, state) {
			return false, &ErrSnapshotConflict{Cursor: cursor}
		}
		return false, nil
	}
}

// GetSnapshot returns the state stored for documentID at cursor. Any miss —
// unknown document, unknown cursor or absent snapshot — yields
// ErrSnapshotNotFound.
func (s *Service) GetSnapshot(documentID string, cursor int64) (json.RawMessage, error) {
	var state []byte
	err := s.db.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrSnapshotNotFound
	case err != nil:
		return nil, err
	default:
		return json.RawMessage(state), nil
	}
}

// ExportSnapshots returns every snapshot of documentID whose cursor lies in
// the closed interval [from, to], in ascending cursor order. When to is nil no
// upper bound is applied. An unknown document, or a range without a snapshot,
// yields an empty list and no error: the export is a read that misses like any
// other. The (document_id, cursor) primary key guarantees a cursor appears at
// most once.
//
// The single SELECT runs as one SQLite statement over the serialized
// connection, so a snapshot committed concurrently is observed either with all
// of its row or not at all — never half of it. The read starts no
// transaction, writes nothing and notifies no waiters, so it cannot move a
// cursor or produce a change record.
func (s *Service) ExportSnapshots(documentID string, from int64, to *int64) ([]ExportedSnapshot, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if to == nil {
		rows, err = s.db.Query(
			`SELECT cursor, state FROM snapshots
			 WHERE document_id = ? AND cursor >= ?
			 ORDER BY cursor ASC`,
			documentID, from,
		)
	} else {
		rows, err = s.db.Query(
			`SELECT cursor, state FROM snapshots
			 WHERE document_id = ? AND cursor >= ? AND cursor <= ?
			 ORDER BY cursor ASC`,
			documentID, from, *to,
		)
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]ExportedSnapshot, 0)
	for rows.Next() {
		var snap ExportedSnapshot
		if err := rows.Scan(&snap.Cursor, &snap.State); err != nil {
			return nil, err
		}
		out = append(out, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// RestoreSnapshot appends one ordinary change whose payload is the state of
// documentID's snapshot at snapshotCursor, all within a single serialized
// transaction.
//
//   - A snapshot miss (unknown document or a cursor without a snapshot)
//     returns ErrSnapshotNotFound; nothing is written.
//   - If changeID already belongs to a prior restore, the call is idempotent
//     only when deviceId, snapshotCursor and the decoded source state all
//     match; the first result (created=false, original cursor) is returned.
//   - If changeID is taken by an ordinary change, or a restore whose provenance
//     differs, the call returns *ErrRestoreConflict; nothing is written.
//
// Otherwise a new change is appended with the next document cursor and the
// restore provenance is recorded. Old rows are never modified.
func (s *Service) RestoreSnapshot(documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := restoreSnapshotTx(tx, documentID, deviceID, changeID, snapshotCursor)
	if err != nil {
		return RestoreResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RestoreResult{}, err
	}
	// A genuinely new restore appended a change and is the only outcome that
	// wakes the long polls and pushes to subscribers; an idempotent repeat
	// appended nothing and notifies nothing.
	if result.Created {
		s.notifyWaiters(documentID)
	}
	return result, nil
}

// RestoreSnapshotAuthorized is the session-scoped restore: it has exactly the
// restore semantics of RestoreSnapshot, but enforces the gate (registration
// then permission) first in the same serialized transaction: an unregistered
// device yields store.ErrDeviceNotFound and a revoked device yields
// ErrPermissionDenied. None of those outcomes writes anything or observes
// snapshot or change content. The device identity is resolved from the session
// by the HTTP layer before the call.
func (s *Service) RestoreSnapshotAuthorized(documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first: the device row and the permission row are read before the
	// snapshot is looked up or any change content is observed, so a rejected
	// restore cannot reveal anything.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return RestoreResult{}, err
		}
	}

	result, err := restoreSnapshotTx(tx, documentID, deviceID, changeID, snapshotCursor)
	if err != nil {
		return RestoreResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RestoreResult{}, err
	}
	if result.Created {
		s.notifyWaiters(documentID)
	}
	return result, nil
}

// restoreSnapshotTx runs the snapshot lookup, the idempotency/conflict
// resolution and the single append inside the caller's serialized
// transaction. It is shared by the document-level restore and the
// session-scoped authorized restore so the two paths cannot drift: both take
// their snapshot miss (ErrSnapshotNotFound), conflict (*ErrRestoreConflict)
// and append decisions from one code path. The result's Created flag tells
// the caller whether a change-bearing transaction just committed and the push
// channels therefore need waking.
//
// A restore that already happened keeps its verdict from its stored
// provenance — the restores row, or the retained summary when its change was
// later compacted — even after the snapshot it came from has been pruned: the
// same device and snapshot cursor stay idempotent and any mismatch stays a
// conflict. Only a genuinely new id requires the snapshot to still exist; a
// pruned or otherwise absent snapshot is then ErrSnapshotNotFound.
func restoreSnapshotTx(tx *sql.Tx, documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	// Resolve the change id first. Three prior states are possible: an online
	// change, a change compaction trimmed into change_identities, or no row at
	// all. An id with a recorded restore provenance (a restores row, or a
	// retained summary whose restored_from is set) keeps its idempotency and
	// conflict verdict from that provenance even when the snapshot it came
	// from has since been pruned. An id occupied by an ordinary change keeps
	// the established precedence — a missing snapshot is still a miss
	// (ErrSnapshotNotFound) before the id conflict is reported.
	var existingCursor int64
	err := tx.QueryRow(
		`SELECT cursor FROM changes WHERE document_id = ? AND id = ?`,
		documentID, changeID,
	).Scan(&existingCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No online row: consult the retained summary compaction leaves behind.
		var summaryDevice string
		var summaryDigest []byte
		var summaryCursor int64
		var restoredFrom sql.NullInt64
		err := tx.QueryRow(
			`SELECT device_id, digest, cursor, restored_from FROM change_identities
			 WHERE document_id = ? AND id = ?`,
			documentID, changeID,
		).Scan(&summaryDevice, &summaryDigest, &summaryCursor, &restoredFrom)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// Genuinely new id: the snapshot must exist, then append below.
		case err != nil:
			return RestoreResult{}, err
		default:
			if !restoredFrom.Valid {
				// An ordinary trimmed change. With the snapshot present the id
				// is a conflict; a missing snapshot keeps its miss precedence.
				if err := snapshotExistsTx(tx, documentID, snapshotCursor); err != nil {
					return RestoreResult{}, err
				}
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			return recordedRestoreVerdictTx(tx, documentID, changeID, deviceID, summaryDevice,
				snapshotCursor, summaryCursor, restoredFrom.Int64, summaryDigest)
		}
	case err != nil:
		return RestoreResult{}, err
	default:
		var restDevice string
		var restSnapshotCursor int64
		var restState []byte
		err = tx.QueryRow(
			`SELECT device_id, snapshot_cursor, state FROM restores WHERE document_id = ? AND change_id = ?`,
			documentID, changeID,
		).Scan(&restDevice, &restSnapshotCursor, &restState)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The id belongs to an ordinary change, not a restore. With the
			// snapshot present the id is a conflict; a missing snapshot keeps
			// its miss precedence.
			if err := snapshotExistsTx(tx, documentID, snapshotCursor); err != nil {
				return RestoreResult{}, err
			}
			return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
		case err != nil:
			return RestoreResult{}, err
		default:
			digest, err := payloadDigest(restState)
			if err != nil {
				return RestoreResult{}, err
			}
			return recordedRestoreVerdictTx(tx, documentID, changeID, deviceID, restDevice,
				snapshotCursor, existingCursor, restSnapshotCursor, digest)
		}
	}

	// A genuinely new restore: the snapshot must exist; its state is the
	// payload to append. A pruned or never-existing snapshot misses here.
	var state []byte
	if err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, snapshotCursor,
	).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return RestoreResult{}, ErrSnapshotNotFound
		}
		return RestoreResult{}, err
	}

	nextCursor, err := currentCursorTx(tx, documentID)
	if err != nil {
		return RestoreResult{}, err
	}
	nextCursor++
	if _, err := tx.Exec(
		`INSERT INTO changes (document_id, cursor, id, device_id, payload) VALUES (?, ?, ?, ?, ?)`,
		documentID, nextCursor, changeID, deviceID, state,
	); err != nil {
		return RestoreResult{}, err
	}
	if _, err := tx.Exec(
		`INSERT INTO restores (document_id, change_id, device_id, snapshot_cursor, change_cursor, state)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		documentID, changeID, deviceID, snapshotCursor, nextCursor, state,
	); err != nil {
		return RestoreResult{}, err
	}
	return RestoreResult{
		ID:           changeID,
		Created:      true,
		Cursor:       nextCursor,
		RestoredFrom: snapshotCursor,
	}, nil
}

// recordedRestoreVerdictTx answers the idempotency/conflict question for an id
// that already names a recorded restore, from its stored provenance:
// originDevice is the recorded source device, firstCursor the cursor the
// change was first assigned, recordedSnapshotCursor the snapshot the recorded
// restore read and digest the canonical digest of the state that snapshot
// carried when the restore happened (the retained summary stores that digest;
// the online restores row stores the state, whose digest the caller passes).
//
// A different source device or snapshot cursor is a conflict. The same pair
// means the same immutable source state, so the recorded verdict survives the
// pruning of that snapshot and the caller repeats as idempotent with the first
// cursor; when the snapshot still exists its live state is compared to the
// recorded digest as well, exactly as a fresh restore would.
func recordedRestoreVerdictTx(tx *sql.Tx, documentID, changeID, deviceID, originDevice string, snapshotCursor, firstCursor, recordedSnapshotCursor int64, digest []byte) (RestoreResult, error) {
	if originDevice != deviceID || recordedSnapshotCursor != snapshotCursor {
		return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
	}
	if state, ok, err := snapshotStateIfExistsTx(tx, documentID, snapshotCursor); err != nil {
		return RestoreResult{}, err
	} else if ok {
		liveDigest, err := payloadDigest(state)
		if err != nil {
			return RestoreResult{}, err
		}
		if !bytes.Equal(digest, liveDigest) {
			return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
		}
	}
	return RestoreResult{
		ID:           changeID,
		Created:      false,
		Cursor:       firstCursor,
		RestoredFrom: snapshotCursor,
	}, nil
}

// snapshotStateIfExistsTx returns the stored snapshot state at cursor and
// ok=true when the snapshot exists. A pruned or never-existing snapshot
// returns nil, false, nil; only a real lookup error fails. It lets a recorded
// restore keep its verdict from stored provenance once the snapshot it came
// from has been pruned, while still comparing the live state when present.
func snapshotStateIfExistsTx(tx *sql.Tx, documentID string, cursor int64) ([]byte, bool, error) {
	var state []byte
	err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	default:
		return state, true, nil
	}
}
