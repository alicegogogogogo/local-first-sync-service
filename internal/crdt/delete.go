// Document-level deletion removes every durable row a document owns in the
// CRDT state layer: the fixed type row, the operation log and the four
// type-specific merge tables, the OR-set tombstones and the retained
// idempotency digests of compacted-away operations. After the deletion the
// same document id is a brand-new document: the next batch fixes its type
// afresh, and every operation id and the cursor-free merge state start over.
//
// The work runs inside the caller's serialized transaction, sharing it with
// the change-event and permission-ledger deletions of the same operation, so
// a document deletion either removes every row or none.

package crdt

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// DocumentDataExistsTx reports whether the CRDT state service holds any row
// for documentID — the fixed type row or anything derived from it. It runs
// inside the caller's transaction so the existence verdict is one judgment
// with the deletion that follows it.
func (s *Service) DocumentDataExistsTx(q store.DBTX, documentID string) (bool, error) {
	var exists bool
	if err := q.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM crdt_documents WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_ops WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_counter_values WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_set_elements WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_register_ops WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_orset_tags WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_orset_tombstones WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM crdt_op_identities WHERE document_id = ?)`,
		documentID, documentID, documentID, documentID,
		documentID, documentID, documentID, documentID,
	).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

// DeleteDocumentDataTx removes every CRDT row of documentID inside the
// caller's serialized transaction. It does not judge existence — the caller
// checks DocumentDataExistsTx (and the change event service's equivalent) once
// before deleting — so it never reports a miss on its own.
func (s *Service) DeleteDocumentDataTx(q store.DBTX, documentID string) error {
	stmts := []string{
		`DELETE FROM crdt_orset_tombstones WHERE document_id = ?`,
		`DELETE FROM crdt_orset_tags WHERE document_id = ?`,
		`DELETE FROM crdt_register_ops WHERE document_id = ?`,
		`DELETE FROM crdt_set_elements WHERE document_id = ?`,
		`DELETE FROM crdt_counter_values WHERE document_id = ?`,
		`DELETE FROM crdt_op_identities WHERE document_id = ?`,
		`DELETE FROM crdt_ops WHERE document_id = ?`,
		`DELETE FROM crdt_documents WHERE document_id = ?`,
	}
	for _, stmt := range stmts {
		if _, err := q.Exec(stmt, documentID); err != nil {
			return err
		}
	}
	return nil
}
