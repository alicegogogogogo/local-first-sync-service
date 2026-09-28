// Read-only ordered listing over a document's online CRDT operations. It is
// the browse counterpart of the by-id batch lookup: instead of naming ids the
// caller pages over every operation still in the online tables in ascending
// operation-id order, one stable page at a time. Operations compaction trimmed
// never appear; only their total count is reported, so the listing restores
// neither their device nor their comparison content. The read starts a
// transaction but writes nothing: it allocates no cursor, creates no operation
// and notifies no subscriber.

package crdt

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// OpListItem is one online operation of a ListOps page, in ascending
// operation-id order. It carries the same fields as an OpLookup "found"
// answer: the originating device in DeviceID, the document's fixed type in
// Type, and the type-specific comparison content exactly as it is stored —
// for a counter the contribution in Value (a JSON integer); for a gset the
// element set in Elements (the stored JSON array); for a register the
// version in Version and the value in Value; for an orset the action in
// Action and the element in Element.
type OpListItem struct {
	ID       string
	DeviceID string
	Type     string

	Value    json.RawMessage // counter: contribution; register: the value, verbatim
	Elements json.RawMessage // gset: the stored JSON array of elements
	Version  int64           // register: logical version
	Action   string          // orset: ORSetAdd or ORSetRemove
	Element  string          // orset: the element
}

// OpsPage is one ListOps answer: the online operations of the requested page
// and the total number of operation ids compaction trimmed for the document.
// The trimmed count is page-independent and excludes the online rows; orset
// compaction never trims an operation, so it is always zero there.
type OpsPage struct {
	Ops       []OpListItem
	Compacted int64
}

// ListOps answers a read-only ordered page of online CRDT operations for
// documentID. The page holds at most limit rows, skipping the first offset
// online operations, ordered by ascending operation id; paging with
// non-overlapping offsets visits every online operation exactly once. The
// gate (registration then permission) is taken first inside one serialized
// transaction — an unregistered device yields store.ErrDeviceNotFound and a
// revoked device store.ErrPermissionDenied, before any CRDT content is
// observed. A document with no committed CRDT operation then yields
// ErrNotFound (the caller answers 404, the same verdict the state read
// gives).
//
// Trimmed operations never appear in a page; their total number is reported
// in OpsPage.Compacted from the retained-identity table alone, without
// restoring any retained content. The read writes nothing, allocates no
// cursor and notifies no waiter or subscriber, so it cannot move the merged
// state or wake a subscription. The answer derives solely from durable rows
// and is therefore byte-stable across a process restart.
func (s *Service) ListOps(documentID, deviceID string, limit, offset int64) (OpsPage, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return OpsPage{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated commits and the by-id lookup do: the
	// device row and the permission row are read before any CRDT content is
	// observed, so a rejected listing cannot reveal type or state.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return OpsPage{}, err
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
		return OpsPage{}, ErrNotFound
	case err != nil:
		return OpsPage{}, err
	}

	page := OpsPage{Ops: make([]OpListItem, 0)}

	switch docType {
	case TypeCounter, TypeGSet:
		rows, err := tx.Query(
			`SELECT id, device_id, value FROM crdt_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return OpsPage{}, err
		}
		for rows.Next() {
			var item OpListItem
			var value []byte
			if err := rows.Scan(&item.ID, &item.DeviceID, &value); err != nil {
				_ = rows.Close()
				return OpsPage{}, err
			}
			item.Type = docType
			if docType == TypeCounter {
				item.Value = json.RawMessage(value)
			} else {
				item.Elements = json.RawMessage(value)
			}
			page.Ops = append(page.Ops, item)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return OpsPage{}, err
		}
		_ = rows.Close()
	case TypeRegister:
		rows, err := tx.Query(
			`SELECT id, device_id, version, value FROM crdt_register_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return OpsPage{}, err
		}
		for rows.Next() {
			var item OpListItem
			var value []byte
			if err := rows.Scan(&item.ID, &item.DeviceID, &item.Version, &value); err != nil {
				_ = rows.Close()
				return OpsPage{}, err
			}
			item.Type = docType
			item.Value = json.RawMessage(value)
			page.Ops = append(page.Ops, item)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return OpsPage{}, err
		}
		_ = rows.Close()
	case TypeORSet:
		rows, err := tx.Query(
			`SELECT id, device_id, value FROM crdt_ops
			 WHERE document_id = ? ORDER BY id ASC LIMIT ? OFFSET ?`,
			documentID, limit, offset,
		)
		if err != nil {
			return OpsPage{}, err
		}
		for rows.Next() {
			var item OpListItem
			var content []byte
			if err := rows.Scan(&item.ID, &item.DeviceID, &content); err != nil {
				_ = rows.Close()
				return OpsPage{}, err
			}
			var stored struct {
				Action  string `json:"action"`
				Element string `json:"element"`
			}
			if err := json.Unmarshal(content, &stored); err != nil {
				_ = rows.Close()
				return OpsPage{}, err
			}
			item.Type = docType
			item.Action = stored.Action
			item.Element = stored.Element
			page.Ops = append(page.Ops, item)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return OpsPage{}, err
		}
		_ = rows.Close()
	default:
		return OpsPage{}, errors.New("unknown crdt type stored for document")
	}

	// The trimmed total is a page-independent count reported from the
	// retained-identity table alone. Orset compaction retains no identity, so
	// its count is always zero.
	if docType != TypeORSet {
		if err := tx.QueryRow(
			`SELECT COUNT(*) FROM crdt_op_identities WHERE document_id = ?`,
			documentID,
		).Scan(&page.Compacted); err != nil {
			return OpsPage{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return OpsPage{}, err
	}
	return page, nil
}
