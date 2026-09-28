// Document-level deletion removes every durable row a document owns in the
// change event service: the online change log, the compaction boundary, the
// retained idempotency summaries of trimmed changes, snapshots and restore
// provenance. After the deletion the same document id is a brand-new document:
// an unknown document reads as an empty list at cursor 0, and the next commit
// allocates cursor 1 again, because every row — the boundary included — that
// remembered the old cursor space is gone.
//
// The work runs inside the caller's serialized transaction, sharing it with the
// CRDT state and permission-ledger deletions of the same operation, so a
// document deletion either removes every row or none; a document with no row
// in any of the services misses (ErrDocumentNotFound) and writes nothing.

package events

import (
	"errors"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// ErrDocumentNotFound reports that a delete targets a document id that has no
// durable data anywhere — it was never created or was already deleted. The
// caller maps it to 404; nothing is written.
var ErrDocumentNotFound = errors.New("document not found")

// DocumentDataExistsTx reports whether the change event service holds any row
// for documentID: an online change, a compaction boundary, a retained
// trimmed-id summary, a snapshot, a snapshot version marker or a restore
// record. It runs inside the caller's transaction so the existence verdict is
// one judgment with the deletion that follows it.
func (s *Service) DocumentDataExistsTx(q store.DBTX, documentID string) (bool, error) {
	var exists bool
	if err := q.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM change_boundaries WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM change_identities WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM snapshots WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM snapshot_versions WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM restores WHERE document_id = ?)`,
		documentID, documentID, documentID, documentID, documentID, documentID,
	).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// DeleteDocumentDataTx removes every change-event row of documentID inside the
// caller's serialized transaction: the online log and restore records first
// (the rows that reference cursors), then snapshots and their named version
// markers, the retained idempotency summaries and finally the compaction
// boundary. With the boundary gone the cursor space restarts from 1 when the
// id is created again, and with the markers gone a re-created document of the
// same name inherits no version. It does not judge existence — the caller
// checks DocumentDataExistsTx (and the CRDT service's equivalent) once before
// deleting — so it never returns ErrDocumentNotFound on its own.
func (s *Service) DeleteDocumentDataTx(q store.DBTX, documentID string) error {
	stmts := []string{
		`DELETE FROM restores WHERE document_id = ?`,
		`DELETE FROM changes WHERE document_id = ?`,
		`DELETE FROM snapshot_versions WHERE document_id = ?`,
		`DELETE FROM snapshots WHERE document_id = ?`,
		`DELETE FROM change_identities WHERE document_id = ?`,
		`DELETE FROM change_boundaries WHERE document_id = ?`,
	}
	for _, stmt := range stmts {
		if _, err := q.Exec(stmt, documentID); err != nil {
			return err
		}
	}
	return nil
}
