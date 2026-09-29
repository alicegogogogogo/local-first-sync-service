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
// A snapshot retention prune hard-deletes snapshot rows while leaving every
// accepted restore's recorded provenance intact, so a missing snapshot is not
// treated as a miss until the change id is resolved: when the id names an
// already-accepted restore (an online restores row, or a compaction-retained
// summary carrying restore provenance), its idempotent (200) or conflict
// (409) verdict is read from that record alone and is unchanged by the
// snapshot's deletion. Every other case against a missing snapshot keeps the
// pre-prune order and answers ErrSnapshotNotFound first — a genuinely new id,
// an id held by an ordinary change and a compaction-retained summary of an
// ordinary change all miss exactly as they did before prune existed.
func restoreSnapshotTx(tx *sql.Tx, documentID, deviceID, changeID string, snapshotCursor int64) (RestoreResult, error) {
	// The snapshot supplies the state to append. A miss is not fatal yet: an
	// already-accepted restore answers from its recorded provenance below even
	// after a prune deleted the snapshot row; the miss only ends a genuinely
	// new id's restore.
	var state []byte
	err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, snapshotCursor,
	).Scan(&state)
	snapshotMissing := false
	switch {
	case errors.Is(err, sql.ErrNoRows):
		snapshotMissing = true
	case err != nil:
		return RestoreResult{}, err
	}

	// Resolve the change id: an ordinary row, a prior restore and a
	// compacted-away id's retained summary are handled differently, so consult
	// all three.
	var existingCursor int64
	var existingDevice string
	var existingPayload []byte
	err = tx.QueryRow(
		`SELECT cursor, device_id, payload FROM changes WHERE document_id = ? AND id = ?`,
		documentID, changeID,
	).Scan(&existingCursor, &existingDevice, &existingPayload)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No online row: the id may belong to a change compaction trimmed.
		// Its retained summary still decides — an idempotent restore needs the
		// same device, snapshot cursor and source state; an id left by an
		// ordinary trimmed change is a conflict, as is any mismatch.
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
			// Genuinely new id: the snapshot must exist to source the append.
			if snapshotMissing {
				return RestoreResult{}, ErrSnapshotNotFound
			}
			// Fall through to append below.
		case err != nil:
			return RestoreResult{}, err
		default:
			// The id is already recorded in a retained summary. A summary
			// carrying restore provenance keeps that restore's idempotency and
			// conflict verdicts on its own even after a prune removed the
			// snapshot: a matching device and source cursor is idempotent (the
			// source state also matches when the snapshot still exists),
			// anything else is the restore's conflict. A summary of an
			// ordinary trimmed change is not a restore record: against a
			// missing snapshot the snapshot miss keeps coming first, exactly
			// as it did before prune existed.
			if !restoredFrom.Valid {
				if snapshotMissing {
					return RestoreResult{}, ErrSnapshotNotFound
				}
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			if restoredFrom.Int64 != snapshotCursor || summaryDevice != deviceID {
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			if !snapshotMissing {
				stateDigest, err := payloadDigest(state)
				if err != nil {
					return RestoreResult{}, err
				}
				if !bytes.Equal(summaryDigest, stateDigest) {
					return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
				}
			}
			return RestoreResult{
				ID:           changeID,
				Created:      false,
				Cursor:       summaryCursor,
				RestoredFrom: snapshotCursor,
			}, nil
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
			// The id belongs to an ordinary change (or a batch/merge change),
			// not a restore. Against a missing (pruned) snapshot the snapshot
			// miss keeps coming first; otherwise it is the restore conflict.
			if snapshotMissing {
				return RestoreResult{}, ErrSnapshotNotFound
			}
			return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
		case err != nil:
			return RestoreResult{}, err
		default:
			if restDevice != deviceID || restSnapshotCursor != snapshotCursor {
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			// When the snapshot still exists its state must match the state
			// recorded at restore time; a pruned snapshot leaves the recorded
			// copy as the sole source of truth for the idempotent verdict.
			if !snapshotMissing && !store.JSONEqual(restState, state) {
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
			}
			return RestoreResult{
				ID:           changeID,
				Created:      false,
				Cursor:       existingCursor,
				RestoredFrom: snapshotCursor,
			}, nil
		}
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
