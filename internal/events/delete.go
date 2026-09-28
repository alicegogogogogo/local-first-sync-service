package events

import (
	"errors"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// ErrDocumentNotFound reports that documentID has no durable trace in the
// change event layer — no online or compacted-away change, no boundary, no
// snapshot, no restore and no retained identity summary. A document-level
// delete maps it to 404; nothing is written.
var ErrDocumentNotFound = errors.New("document not found")

// DocumentExistsTx reports whether documentID has any durable change-event
// state: an online change, a compaction boundary (including a fully trimmed
// log), a snapshot, a restore record or a retained trimmed-identity summary.
// It is evaluated inside the caller's serialized transaction, so the
// existence verdict and the wipe commit as one judgment.
func DocumentExistsTx(q store.DBTX, documentID string) (bool, error) {
	var exists bool
	if err := q.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM change_boundaries WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM snapshots WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM restores WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM change_identities WHERE document_id = ?)`,
		documentID, documentID, documentID, documentID, documentID,
	).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// DeleteDocumentTx removes every change-event row of documentID inside the
// caller's serialized transaction: the online log and the parked long-poll
// boundary, the snapshots, the restore provenance, the compaction boundary and
// every retained idempotency summary. With all rows gone the document's
// high-water mark is zero again, so a later same-named document allocates
// cursors from 1 and inherits no summary, boundary or snapshot.
func DeleteDocumentTx(q store.DBTX, documentID string) error {
	stmts := []string{
		`DELETE FROM restores WHERE document_id = ?`,
		`DELETE FROM change_identities WHERE document_id = ?`,
		`DELETE FROM changes WHERE document_id = ?`,
		`DELETE FROM snapshots WHERE document_id = ?`,
		`DELETE FROM change_boundaries WHERE document_id = ?`,
	}
	for _, stmt := range stmts {
		if _, err := q.Exec(stmt, documentID); err != nil {
			return err
		}
	}
	return nil
}
