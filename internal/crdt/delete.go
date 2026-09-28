package crdt

import (
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// DocumentExistsTx reports whether documentID has any durable CRDT state: the
// fixed type row, any accepted operation, a per-device counter maximum, a set
// element, an accepted register operation, an orset tag or tombstone, or a
// retained trimmed-identity summary. It runs inside the caller's serialized
// transaction, so the existence verdict and the wipe commit as one judgment.
func DocumentExistsTx(q store.DBTX, documentID string) (bool, error) {
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

// DeleteDocumentTx removes every CRDT row of documentID inside the caller's
// serialized transaction: the type row, the accepted operation log, the merge
// tables of all four types and every retained compaction identity. With the
// type row gone a same-named document later declares its type anew and its
// first batch allocates a fresh history that inherits nothing.
func DeleteDocumentTx(q store.DBTX, documentID string) error {
	stmts := []string{
		`DELETE FROM crdt_op_identities WHERE document_id = ?`,
		`DELETE FROM crdt_orset_tombstones WHERE document_id = ?`,
		`DELETE FROM crdt_orset_tags WHERE document_id = ?`,
		`DELETE FROM crdt_register_ops WHERE document_id = ?`,
		`DELETE FROM crdt_set_elements WHERE document_id = ?`,
		`DELETE FROM crdt_counter_values WHERE document_id = ?`,
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
