// Session sync checkpoints are the durable record of the last change cursor
// each session has fully applied locally on one document. The kernel owns the
// rows because they are keyed by the registration layer's sessions and share
// the cascades of that layer: deleting a session or deregistering a device
// clears the checkpoints those sessions held, and the permission service
// clears one device's checkpoints on a document inside the same serialized
// transaction as the revoke itself. The confirmation logic — the monotonic,
// boundary and high-water judgments — lives in the change event service,
// which composes these helpers inside its own transactions.
package store

import (
	"database/sql"
	"errors"
)

// CheckpointTx returns the checkpoint cursor recorded for the (session,
// document) pair, or recorded=false when the session never confirmed one. It
// runs inside q so the caller's judgment reads on the same serialized
// connection as the write it guards.
func CheckpointTx(q DBTX, sessionID, documentID string) (cursor int64, recorded bool, err error) {
	err = q.QueryRow(
		`SELECT cursor FROM session_checkpoints WHERE session_id = ? AND document_id = ?`,
		sessionID, documentID,
	).Scan(&cursor)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, err
	default:
		return cursor, true, nil
	}
}

// PutCheckpointTx records cursor as the (session, document) pair's confirmed
// checkpoint, inserting or replacing the row inside the caller's transaction.
// The monotonic judgment (a smaller value never replaces a larger one) is the
// caller's, made earlier in the same transaction.
func PutCheckpointTx(q DBTX, sessionID, documentID string, cursor int64) error {
	_, err := q.Exec(
		`INSERT INTO session_checkpoints (session_id, document_id, cursor) VALUES (?, ?, ?)
		 ON CONFLICT (session_id, document_id) DO UPDATE SET cursor = excluded.cursor`,
		sessionID, documentID, cursor,
	)
	return err
}

// DeleteDocumentCheckpointsTx removes every checkpoint recorded on documentID,
// inside the caller's transaction. It is the checkpoint share of document
// deletion: a re-created document of the same id inherits no confirmations.
func DeleteDocumentCheckpointsTx(q DBTX, documentID string) error {
	_, err := q.Exec(
		`DELETE FROM session_checkpoints WHERE document_id = ?`,
		documentID,
	)
	return err
}

// DeletePermissionCheckpointsTx removes the checkpoints every session owned by
// deviceID recorded on documentID, inside the caller's transaction. It runs as
// part of a permission revoke, so a re-authorized device finds its sessions
// unconfirmed on the document and confirms afresh.
func DeletePermissionCheckpointsTx(q DBTX, documentID, deviceID string) error {
	_, err := q.Exec(
		`DELETE FROM session_checkpoints
		 WHERE document_id = ?
		 AND session_id IN (SELECT id FROM sessions WHERE device_id = ?)`,
		documentID, deviceID,
	)
	return err
}
