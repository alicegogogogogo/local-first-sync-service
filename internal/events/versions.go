package events

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// PutSnapshotVersion binds an existing snapshot of documentID to the stable
// version name, all inside one serialized transaction.
//
// The gate (registration then permission) is taken first in the transaction:
// an unregistered device yields store.ErrDeviceNotFound and a revoked device
// store.ErrPermissionDenied, before any snapshot or version is observed. Then
// the target snapshot must exist (a miss, an unknown document included, is
// ErrSnapshotNotFound), and finally the name is resolved:
//
//   - an unbound name is inserted and reports the binding;
//   - a name already bound to the same cursor is idempotent and reports the
//     very same name/cursor pair (the HTTP body is byte-identical to the first
//     registration);
//   - a name already bound to another cursor is *ErrVersionConflict with zero
//     writes.
//
// A version marker never copies or moves snapshot bytes and creates no
// change.
func (s *Service) PutSnapshotVersion(documentID, deviceID, name string, cursor int64) (VersionResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return VersionResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return VersionResult{}, err
	}
	if err := snapshotExistsTx(tx, documentID, cursor); err != nil {
		return VersionResult{}, err
	}

	var existing sql.NullInt64
	err = tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// An unbound name: insert below.
	case err != nil:
		return VersionResult{}, err
	case existing.Int64 != cursor:
		return VersionResult{}, &ErrVersionConflict{Name: name}
	default:
		// Already bound to the same cursor: idempotent, zero writes.
		if err := tx.Commit(); err != nil {
			return VersionResult{}, err
		}
		return VersionResult{Name: name, Cursor: cursor}, nil
	}
	if _, err := tx.Exec(
		`INSERT INTO snapshot_versions (document_id, name, cursor) VALUES (?, ?, ?)`,
		documentID, name, cursor,
	); err != nil {
		return VersionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return VersionResult{}, err
	}
	return VersionResult{Name: name, Cursor: cursor}, nil
}

// ListSnapshotVersions returns every version marker of documentID in
// ascending name order, together with the snapshot cursor each points at. The
// gate runs first in the transaction (404 for an unregistered device, 403 for
// a revoked one); an unknown document or a document without markers then
// reads as an empty list rather than an error. It writes nothing.
func (s *Service) ListSnapshotVersions(documentID, deviceID string) ([]SnapshotVersion, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return nil, err
	}

	rows, err := tx.Query(
		`SELECT name, cursor FROM snapshot_versions
		 WHERE document_id = ? ORDER BY name ASC`,
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
	return out, tx.Commit()
}

// GetSnapshotVersionState resolves a version name to its snapshot and returns
// that snapshot's cursor and stored state, exactly as GetSnapshot would for
// the cursor. The gate runs first; a name without a marker is
// ErrVersionNotFound and a marker whose snapshot is gone is
// ErrSnapshotNotFound — the caller maps both to 404 and exposes no content.
func (s *Service) GetSnapshotVersionState(documentID, deviceID, name string) (int64, json.RawMessage, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return 0, nil, err
	}

	var cursor int64
	err = tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&cursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil, ErrVersionNotFound
	case err != nil:
		return 0, nil, err
	}

	var state []byte
	if err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, ErrSnapshotNotFound
		}
		return 0, nil, err
	}
	return cursor, json.RawMessage(state), tx.Commit()
}

// RebindSnapshotVersion moves an existing version name of documentID to
// another existing snapshot cursor in one serialized transaction. The gate
// (404/403) is taken first; the named marker must exist (ErrVersionNotFound)
// and the target snapshot must exist (ErrSnapshotNotFound, an unknown
// document included). Rebinding to the cursor already bound is an idempotent
// no-op reporting the same pair; otherwise the marker is updated. The
// snapshot itself is never modified or copied.
func (s *Service) RebindSnapshotVersion(documentID, deviceID, name string, cursor int64) (VersionResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return VersionResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return VersionResult{}, err
	}

	var existing int64
	err = tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return VersionResult{}, ErrVersionNotFound
	case err != nil:
		return VersionResult{}, err
	}
	if err := snapshotExistsTx(tx, documentID, cursor); err != nil {
		return VersionResult{}, err
	}
	if existing != cursor {
		if _, err := tx.Exec(
			`UPDATE snapshot_versions SET cursor = ? WHERE document_id = ? AND name = ?`,
			cursor, documentID, name,
		); err != nil {
			return VersionResult{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return VersionResult{}, err
	}
	return VersionResult{Name: name, Cursor: cursor}, nil
}

// DeleteSnapshotVersion hard-deletes one version marker of documentID. The
// gate (404/403) is taken first; a name without a marker is
// ErrVersionNotFound. The snapshot is left untouched; once the marker commits
// the name is free to bind again.
func (s *Service) DeleteSnapshotVersion(documentID, deviceID, name string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return err
	}

	var bound int64
	if err := tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&bound); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrVersionNotFound
		}
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// RestoreSnapshotVersion resolves a version name of documentID to its
// snapshot and appends that snapshot's state as one ordinary change, with
// exactly the semantics of RestoreSnapshot: the gate is taken first in the
// transaction, a missing marker yields ErrVersionNotFound and a missing
// snapshot ErrSnapshotNotFound, after which the shared restore path decides
// idempotency and conflict (an id occupied by an ordinary change is 409). The
// document cursor advances by one on a genuinely new restore and the long
// polls and subscribers are woken.
func (s *Service) RestoreSnapshotVersion(documentID, deviceID, changeID, name string) (RestoreResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return RestoreResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return RestoreResult{}, err
	}

	var snapshotCursor int64
	err = tx.QueryRow(
		`SELECT cursor FROM snapshot_versions WHERE document_id = ? AND name = ?`,
		documentID, name,
	).Scan(&snapshotCursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RestoreResult{}, ErrVersionNotFound
	case err != nil:
		return RestoreResult{}, err
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

// gateTx runs the service's registration/permission gate inside tx. A service
// built without a gate (some service-level tests) skips the verdict.
func gateTx(s *Service, tx *sql.Tx, documentID, deviceID string) error {
	if s.gate == nil {
		return nil
	}
	return s.gate.DeviceAuthorizedTx(tx, documentID, deviceID)
}

// snapshotExistsTx reports whether documentID has a snapshot at cursor,
// returning ErrSnapshotNotFound on a miss. It runs inside the caller's
// serialized transaction so the existence verdict and the following write are
// one judgment.
func snapshotExistsTx(tx *sql.Tx, documentID string, cursor int64) error {
	var state []byte
	if err := tx.QueryRow(
		`SELECT state FROM snapshots WHERE document_id = ? AND cursor = ?`,
		documentID, cursor,
	).Scan(&state); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSnapshotNotFound
		}
		return err
	}
	return nil
}
