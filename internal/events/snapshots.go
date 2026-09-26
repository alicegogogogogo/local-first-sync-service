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

	// The snapshot must exist; its state is the payload to append.
	var state []byte
	err = tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, snapshotCursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RestoreResult{}, ErrSnapshotNotFound
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
			// Genuinely new id; fall through to append below.
		case err != nil:
			return RestoreResult{}, err
		default:
			stateDigest, err := payloadDigest(state)
			if err != nil {
				return RestoreResult{}, err
			}
			if !restoredFrom.Valid || restoredFrom.Int64 != snapshotCursor ||
				summaryDevice != deviceID || !bytes.Equal(summaryDigest, stateDigest) {
				return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
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
			// not a restore: never treat it as an idempotent restore.
			return RestoreResult{}, &ErrRestoreConflict{ID: changeID}
		case err != nil:
			return RestoreResult{}, err
		default:
			if restDevice != deviceID || restSnapshotCursor != snapshotCursor || !store.JSONEqual(restState, state) {
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

	if err := tx.Commit(); err != nil {
		return RestoreResult{}, err
	}
	s.notifyWaiters(documentID)
	return RestoreResult{
		ID:           changeID,
		Created:      true,
		Cursor:       nextCursor,
		RestoredFrom: snapshotCursor,
	}, nil
}
