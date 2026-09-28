// Read-only ordered listing over a document's online CRDT operations. Where
// the batch lookup answers a caller-named set of ids, the listing browses the
// operations still in the online tables in their stable order, paged by
// limit/offset: the caller gets one page plus the total number of operations
// compaction has trimmed away (reported through their retained identities).
// The read starts a transaction but writes nothing: it allocates no cursor,
// creates no operation, notifies no subscriber and changes neither the merge
// state nor any idempotency decision.

package crdt

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// OpList is one page of ListOps: the online operations of the requested page
// in their stable (ascending id) order and Compacted, the total number of the
// document's operation ids compaction has trimmed away. Ops is always a
// non-nil slice so an empty page serializes as [] rather than null.
//
// Every listed operation is an online row, so each entry has LookupFound
// status together with its originating device and the type-specific
// comparison content exactly as it was saved at commit time — the same content
// shape GetOpsByIDs gives a found answer. A trimmed id never appears in Ops
// and its content is never restored; the retained identity only contributes to
// Compacted.
type OpList struct {
	Ops       []OpLookup
	Compacted int64
}

// ListOps answers a read-only, paged listing of the online CRDT operations of
// documentID, ordered by operation id ascending: the page skips offset rows
// and then returns at most limit rows. The gate (registration then
// permission) is taken first inside one transaction — an unregistered device
// yields store.ErrDeviceNotFound and a revoked device
// store.ErrPermissionDenied, before any CRDT content is observed. A document
// with no committed CRDT operation then yields ErrNotFound (the caller answers
// 404, the same verdict the state read gives).
//
// The per-type online table is the single source of the page: crdt_ops for a
// counter, a gset and an orset (the orset operation log is never compacted),
// crdt_register_ops for a register. Compacted is the count of retained
// identities compaction left behind for trimmed ids; those ids are not
// restored into the page and their digests are never returned.
//
// The read writes nothing, allocates no cursor and notifies no waiter or
// subscriber, so the same request after a repeat call or a process restart
// renders byte-for-byte the same page while the committed history is
// unchanged.
func (s *Service) ListOps(documentID, deviceID string, limit, offset int64) (OpList, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return OpList{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated commits and the batch lookup do: the
	// device row and the permission row are read before any CRDT content is
	// observed, so a rejected listing cannot reveal type or state.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return OpList{}, err
		}
	}

	// The document's type row exists exactly when it has ever held a CRDT
	// operation; a listing against a document with none gets the same 404 the
	// state read gives, rather than an empty page.
	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return OpList{}, ErrNotFound
	case err != nil:
		return OpList{}, err
	}

	ops, err := listOnlineOps(tx, documentID, docType, limit, offset)
	if err != nil {
		return OpList{}, err
	}

	// The retained identities count exactly the operations compaction has
	// trimmed; the table never participates in a merge and its rows are never
	// restored into a listing.
	var compacted int64
	if err := tx.QueryRow(
		`SELECT COUNT(*) FROM crdt_op_identities WHERE document_id = ?`,
		documentID,
	).Scan(&compacted); err != nil {
		return OpList{}, err
	}

	if err := tx.Commit(); err != nil {
		return OpList{}, err
	}
	return OpList{Ops: ops, Compacted: compacted}, nil
}

// listOnlineOps reads one page of the document's online operations in
// ascending id order and renders them as found OpLookup answers carrying the
// stored type-specific comparison content. The page derives from a single
// query on the type's online table, so a page never mixes in a trimmed id.
func listOnlineOps(tx *sql.Tx, documentID, docType string, limit, offset int64) ([]OpLookup, error) {
	ops := make([]OpLookup, 0)

	switch docType {
	case TypeCounter, TypeGSet:
		rows, err := tx.Query(
			`SELECT id, device_id, value FROM crdt_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, device string
			var value []byte
			if err := rows.Scan(&id, &device, &value); err != nil {
				_ = rows.Close()
				return nil, err
			}
			op := OpLookup{ID: id, Status: LookupFound, DeviceID: device, Type: docType}
			if docType == TypeCounter {
				op.Value = json.RawMessage(value)
			} else {
				op.Elements = json.RawMessage(value)
			}
			ops = append(ops, op)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	case TypeRegister:
		rows, err := tx.Query(
			`SELECT id, device_id, version, value FROM crdt_register_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var op OpLookup
			var value []byte
			op.Status = LookupFound
			op.Type = docType
			if err := rows.Scan(&op.ID, &op.DeviceID, &op.Version, &value); err != nil {
				_ = rows.Close()
				return nil, err
			}
			op.Value = json.RawMessage(value)
			ops = append(ops, op)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	case TypeORSet:
		// The orset operation log is never compacted, so every accepted add or
		// remove is browsable; the value column stores {action,element}.
		rows, err := tx.Query(
			`SELECT id, device_id, value FROM crdt_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, device string
			var content []byte
			if err := rows.Scan(&id, &device, &content); err != nil {
				_ = rows.Close()
				return nil, err
			}
			var stored struct {
				Action  string `json:"action"`
				Element string `json:"element"`
			}
			if err := json.Unmarshal(content, &stored); err != nil {
				_ = rows.Close()
				return nil, err
			}
			ops = append(ops, OpLookup{
				ID:       id,
				Status:   LookupFound,
				DeviceID: device,
				Type:     docType,
				Action:   stored.Action,
				Element:  stored.Element,
			})
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	default:
		return nil, errors.New("unknown crdt type stored for document")
	}

	return ops, nil
}
