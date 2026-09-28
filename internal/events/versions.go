package events

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// RegisterSnapshotVersion registers the client-supplied name as a pointer to
// the snapshot of documentID at snapshotCursor, all within one serialized
// transaction.
//
//   - The target snapshot must exist; an unknown document or a cursor without
//     a snapshot yields ErrSnapshotNotFound and nothing is written.
//   - Registering an unused name creates the marker and reports Created=true.
//   - Re-posting the same name against the same cursor is idempotent
//     (Created=false); the response body is identical to the first
//     registration.
//   - Re-posting the same name against another cursor is an
//     *ErrVersionConflict and writes nothing. Rebinding goes through
//     RenameSnapshotVersion.
//
// The marker stores no state of its own: it names a cursor, so registering a
// version neither copies snapshot state nor moves the document's cursor. The
// lookup, the conflict judgment and the insert run on the single serialized
// connection, so concurrent registrations of the same name cannot both create
// it.
func (s *Service) RegisterSnapshotVersion(documentID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	version, created, err := registerSnapshotVersionTx(tx, documentID, name, snapshotCursor)
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotVersion{}, false, err
	}
	return version, created, nil
}

// RegisterSnapshotVersionAuthorized is the session-scoped registration: it has
// exactly the marker semantics of RegisterSnapshotVersion, but enforces the
// gate (registration then permission) first in the same serialized
// transaction: an unregistered device yields store.ErrDeviceNotFound and a
// revoked device yields ErrPermissionDenied. None of those outcomes writes
// anything or observes snapshot or version content. The device identity is
// resolved from the session by the HTTP layer before the call.
func (s *Service) RegisterSnapshotVersionAuthorized(documentID, deviceID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first: the device row and the permission row are read before the
	// snapshot or a version marker is looked up, so a rejected registration
	// cannot reveal anything.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return SnapshotVersion{}, false, err
		}
	}

	version, created, err := registerSnapshotVersionTx(tx, documentID, name, snapshotCursor)
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotVersion{}, false, err
	}
	return version, created, nil
}

// registerSnapshotVersionTx runs the snapshot-existence lookup, the name
// resolution and the single insert inside the caller's serialized
// transaction. It is shared by the document-level registration and the
// session-scoped authorized registration so the two paths cannot drift.
func registerSnapshotVersionTx(tx *sql.Tx, documentID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	// The target snapshot must exist; the marker names a cursor, never state
	// of its own.
	if err := snapshotExistsTx(tx, documentID, snapshotCursor); err != nil {
		return SnapshotVersion{}, false, err
	}

	var existingCursor int64
	err := tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&existingCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO snapshot_versions (document_id, name, cursor) VALUES (?, ?, ?)`,
			documentID, name, snapshotCursor,
		); err != nil {
			return SnapshotVersion{}, false, err
		}
		return SnapshotVersion{Name: name, Cursor: snapshotCursor}, true, nil
	case err != nil:
		return SnapshotVersion{}, false, err
	default:
		if existingCursor != snapshotCursor {
			return SnapshotVersion{}, false, &ErrVersionConflict{Name: name}
		}
		return SnapshotVersion{Name: name, Cursor: existingCursor}, false, nil
	}
}

// ListSnapshotVersions returns every registered version of documentID in
// ascending name order. An unknown document or a document without any marker
// yields an empty list and no error: the list is a read that misses like any
// other. Markers carry only names and cursors, never snapshot state.
//
// The single SELECT starts no transaction and writes nothing, so a marker
// committed concurrently is observed either with its row or not at all.
func (s *Service) ListSnapshotVersions(documentID string) ([]SnapshotVersion, error) {
	rows, err := s.db.Query(
		`SELECT name, cursor FROM snapshot_versions
		 WHERE document_id = ?
		 ORDER BY name ASC`,
		documentID,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]SnapshotVersion, 0)
	for rows.Next() {
		var v SnapshotVersion
		if err := rows.Scan(&v.Name, &v.Cursor); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetSnapshotVersion returns the cursor the version name points at for
// documentID. Any miss — unknown document or an unregistered name — yields
// ErrVersionNotFound. It reads no state: the state is read through the
// returned cursor with GetSnapshot, so a read by name is byte-identical to a
// read by cursor.
func (s *Service) GetSnapshotVersion(documentID, name string) (SnapshotVersion, error) {
	var v SnapshotVersion
	err := s.db.QueryRow(
		`SELECT name, cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&v.Name, &v.Cursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return SnapshotVersion{}, ErrVersionNotFound
	case err != nil:
		return SnapshotVersion{}, err
	default:
		return v, nil
	}
}

// RenameSnapshotVersion moves the version name onto another existing snapshot
// cursor of documentID, within one serialized transaction.
//
//   - The name must already be registered; an unknown name yields
//     ErrVersionNotFound and nothing is written.
//   - The target snapshot must exist; a cursor without a snapshot yields
//     ErrSnapshotNotFound and nothing is written.
//   - Rebinding to the cursor the name already points at is idempotent
//     (Created=false).
//   - Otherwise the marker is moved (Created=true). Renaming changes only the
//     name-to-cursor row; the snapshots themselves are never modified, copied
//     or deleted.
func (s *Service) RenameSnapshotVersion(documentID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	version, moved, err := renameSnapshotVersionTx(tx, documentID, name, snapshotCursor)
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotVersion{}, false, err
	}
	return version, moved, nil
}

// RenameSnapshotVersionAuthorized is the session-scoped rename with exactly
// the marker semantics of RenameSnapshotVersion, but enforcing the gate first
// in the same serialized transaction.
func (s *Service) RenameSnapshotVersionAuthorized(documentID, deviceID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	defer func() { _ = tx.Rollback() }()

	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return SnapshotVersion{}, false, err
		}
	}

	version, moved, err := renameSnapshotVersionTx(tx, documentID, name, snapshotCursor)
	if err != nil {
		return SnapshotVersion{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotVersion{}, false, err
	}
	return version, moved, nil
}

// renameSnapshotVersionTx runs the marker lookup, the target-snapshot lookup
// and the single update inside the caller's serialized transaction. It is
// shared by the document-level and session-scoped renames so their judgments
// cannot drift.
func renameSnapshotVersionTx(tx *sql.Tx, documentID, name string, snapshotCursor int64) (SnapshotVersion, bool, error) {
	var currentCursor int64
	err := tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&currentCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return SnapshotVersion{}, false, ErrVersionNotFound
	case err != nil:
		return SnapshotVersion{}, false, err
	}

	if err := snapshotExistsTx(tx, documentID, snapshotCursor); err != nil {
		return SnapshotVersion{}, false, err
	}

	if currentCursor == snapshotCursor {
		return SnapshotVersion{Name: name, Cursor: currentCursor}, false, nil
	}
	if _, err := tx.Exec(
		`UPDATE snapshot_versions SET cursor = ? WHERE document_id = ? AND name = ?`,
		snapshotCursor, documentID, name,
	); err != nil {
		return SnapshotVersion{}, false, err
	}
	return SnapshotVersion{Name: name, Cursor: snapshotCursor}, true, nil
}

// DeleteSnapshotVersion removes the marker of documentID and name. The
// snapshot it pointed at is untouched; after the delete the name can be
// registered again as a brand-new marker (RegisterSnapshotVersion reports
// Created=true). A repeat delete or an unknown name misses with
// ErrVersionNotFound and writes nothing. The check and the delete run in one
// serialized transaction.
func (s *Service) DeleteSnapshotVersion(documentID, name string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := deleteSnapshotVersionTx(tx, documentID, name); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteSnapshotVersionAuthorized is the session-scoped delete with the same
// miss semantics as DeleteSnapshotVersion, enforcing the gate first in the
// same serialized transaction.
func (s *Service) DeleteSnapshotVersionAuthorized(documentID, deviceID, name string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return err
		}
	}

	if err := deleteSnapshotVersionTx(tx, documentID, name); err != nil {
		return err
	}
	return tx.Commit()
}

// deleteSnapshotVersionTx runs the existence check and the marker removal
// inside the caller's serialized transaction. It is shared by the
// document-level and session-scoped deletes.
func deleteSnapshotVersionTx(tx *sql.Tx, documentID, name string) error {
	var exists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM snapshot_versions WHERE document_id = ? AND name = ?)`,
		documentID, name,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrVersionNotFound
	}
	if _, err := tx.Exec(
		`DELETE FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	); err != nil {
		return err
	}
	return nil
}

// RestoreSnapshotVersion appends one ordinary change whose payload is the
// state of the snapshot the version name points at, all within one serialized
// transaction. The restore judgment is byte-identical to
// RestoreSnapshot/RestoreSnapshotAuthorized: the named marker is resolved to
// its cursor first, and a miss (an unknown document or an unregistered name)
// yields ErrVersionNotFound; everything past the resolution runs through the
// shared restoreSnapshotTx, so an id occupied by an ordinary change still
// yields *ErrRestoreConflict and an identical repeat is idempotent.
func (s *Service) RestoreSnapshotVersion(documentID, deviceID, changeID, name string) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	result, err := restoreSnapshotVersionTx(tx, documentID, deviceID, changeID, name)
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

// RestoreSnapshotVersionAuthorized is the session-scoped named restore: it has
// exactly the restore semantics of RestoreSnapshotVersion, but enforces the
// gate (registration then permission) first in the same serialized
// transaction.
func (s *Service) RestoreSnapshotVersionAuthorized(documentID, deviceID, changeID, name string) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first: the device row and the permission row are read before the
	// version marker or the snapshot state is looked up.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return RestoreResult{}, err
		}
	}

	result, err := restoreSnapshotVersionTx(tx, documentID, deviceID, changeID, name)
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

// restoreSnapshotVersionTx resolves the named marker to its snapshot cursor
// and then runs the shared restore judgment inside the caller's transaction.
func restoreSnapshotVersionTx(tx *sql.Tx, documentID, deviceID, changeID, name string) (RestoreResult, error) {
	var snapshotCursor int64
	err := tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&snapshotCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RestoreResult{}, ErrVersionNotFound
	case err != nil:
		return RestoreResult{}, err
	}
	return restoreSnapshotTx(tx, documentID, deviceID, changeID, snapshotCursor)
}

// snapshotExistsTx reports whether the snapshots table holds documentID's
// snapshot at cursor, inside the caller's transaction. A miss is
// ErrSnapshotNotFound, the same verdict a direct snapshot lookup gives.
func snapshotExistsTx(tx *sql.Tx, documentID string, cursor int64) error {
	var state json.RawMessage
	err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrSnapshotNotFound
	case err != nil:
		return err
	default:
		return nil
	}
}
