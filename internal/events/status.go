// The change-log status summary is the change event service's read-only
// answer to "how much of a document's change history is still online and
// where does the online log end": four numbers describing the online log, its
// snapshot boundary and the trimmed tail. Like every other change-log read it
// sees only the online changes table for its content; the retained summaries
// in change_identities exist for idempotency alone and contribute only the
// trimmed count. The read starts a transaction but writes nothing, allocates
// no cursor and notifies no waiter or subscriber.

package events

// ChangeStatus is one document's change-log status summary:
//
//   - OnlineCount counts only the changes still held in the online log
//     (changes compaction moved out never count);
//   - Boundary is the greatest cursor carrying a saved snapshot of the
//     document, zero when the document has no snapshot;
//   - MaxCursor is the greatest cursor among the changes still online, zero
//     when the online log holds no row;
//   - TrimmedCount is the total number of change ids compaction has moved out
//     of the online log (the rows in the retained-identity table).
type ChangeStatus struct {
	OnlineCount  int64
	Boundary     int64
	MaxCursor    int64
	TrimmedCount int64
}

// GetChangesStatus answers a read-only status summary of documentID's change
// log. The gate (registration then permission) is taken first inside one
// serialized transaction — an unregistered device yields
// store.ErrDeviceNotFound and a revoked device store.ErrPermissionDenied,
// before any change content is observed. Then the four numbers are read:
//
//   - the count and the greatest cursor of the rows still in the online
//     changes table, each COALESCEd to zero when the table holds no row for
//     the document (an unknown document and a fully trimmed document alike);
//   - the greatest cursor with a saved snapshot, zero when the document has
//     none;
//   - the count of retained identities, which equals the number of change ids
//     compaction has actually moved out of the online log (identities are
//     written once per trimmed id and never deleted except with the whole
//     document).
//
// An unknown document is therefore a successful all-zero summary: empty
// state is an answer, not an error, and the read neither creates a change nor
// a snapshot. It starts a transaction but writes nothing, allocates no cursor,
// creates no change and notifies no waiter or subscriber, so the same request
// after a repeat call or a process restart renders byte-for-byte the same
// four numbers while the committed history is unchanged.
func (s *Service) GetChangesStatus(documentID, deviceID string) (ChangeStatus, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return ChangeStatus{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated reads do: the device row and the
	// permission row are read before any change content is observed, so a
	// rejected summary cannot reveal whether the document holds anything.
	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return ChangeStatus{}, err
	}

	var status ChangeStatus
	if err := tx.QueryRow(
		`SELECT COALESCE(COUNT(*), 0), COALESCE(MAX(cursor), 0)
		 FROM changes WHERE document_id = ?`,
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
		`SELECT COALESCE(COUNT(*), 0) FROM change_identities WHERE document_id = ?`,
		documentID,
	).Scan(&status.TrimmedCount); err != nil {
		return ChangeStatus{}, err
	}

	if err := tx.Commit(); err != nil {
		return ChangeStatus{}, err
	}
	return status, nil
}
