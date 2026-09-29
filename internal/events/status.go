// The change-log status read is the change event service's read-only answer
// to "how much of this document's change log is still left to catch up on":
// the number of changes still in the online log, the greatest cursor that has
// a saved snapshot (the compaction boundary), the greatest cursor among the
// online rows, and the number of ids compaction moved out of the online log.
// Like every other read it sees only durable rows; the retained summaries in
// change_identities are counted solely as trimmed ids and never restored. The
// read starts a transaction, writes nothing and notifies no waiter or
// subscriber.

package events

// ChangeStatus is the status summary of one document's change log:
//
//   - OnlineCount is the number of change rows still in the online log;
//   - Boundary is the greatest cursor with a saved snapshot, zero when the
//     document has none;
//   - MaxCursor is the greatest cursor among the still-online rows, zero when
//     the online log is empty;
//   - Compacted is the total number of ids compaction moved out of the online
//     log (the rows retained as canonical summaries in change_identities).
//
// Every field is zero for an unknown document: an empty status is a normal
// answer, not an error, and observing it creates neither a change nor a
// snapshot.
type ChangeStatus struct {
	OnlineCount int64
	Boundary    int64
	MaxCursor   int64
	Compacted   int64
}

// GetChangeStatus answers the read-only status summary of documentID's change
// log. The gate (registration then permission) is taken first inside one
// serialized transaction — an unregistered device yields
// store.ErrDeviceNotFound and a revoked device store.ErrPermissionDenied,
// before any change content is observed. Then the four numbers are read from
// durable rows:
//
//   - the online count and the maximum online cursor come from the rows still
//     in changes;
//   - the boundary is the greatest cursor with a saved snapshot, zero when the
//     document has no snapshot;
//   - the compacted count is the number of retained summaries in
//     change_identities — exactly the ids genuinely moved out by compaction;
//     it never decreases as changes are appended and only drops when the whole
//     document is deleted.
//
// An unknown document answers four zeros and no error: the status creates no
// change, allocates no cursor, writes no snapshot and notifies no waiter or
// subscriber, so it cannot move a cursor or wake a parked poll. The verdicts
// derive solely from durable rows and are therefore byte-stable across a
// process restart.
func (s *Service) GetChangeStatus(documentID, deviceID string) (ChangeStatus, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return ChangeStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the other gated reads do: the device row and the
	// permission row are read before any change content is observed, so a
	// rejected status read cannot reveal content.
	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return ChangeStatus{}, err
	}

	var status ChangeStatus
	if err := tx.QueryRow(
		`SELECT COUNT(*), COALESCE(MAX(cursor), 0) FROM changes WHERE document_id = ?`,
		documentID,
	).Scan(&status.OnlineCount, &status.MaxCursor); err != nil {
		return ChangeStatus{}, err
	}
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM snapshots WHERE document_id = ?`,
		documentID,
	).Scan(&status.Boundary); err != nil {
		return ChangeStatus{}, err
	}
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM change_identities WHERE document_id = ?`,
		documentID,
	).Scan(&status.Compacted); err != nil {
		return ChangeStatus{}, err
	}

	if err := tx.Commit(); err != nil {
		return ChangeStatus{}, err
	}
	return status, nil
}
