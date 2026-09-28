// Read-only batch lookup of CRDT operations by their stable ids. It mirrors
// the change-log batch lookup: answers arrive in the exact request order, an
// id the document has never known is a normal "missing" answer, and an id
// whose merge row compaction trimmed is answered "compacted" from the
// retained identity summary alone — the digest is never reversed into
// content and the identity table takes no part in any other read.

package crdt

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// CRDT operation lookup statuses reported per queried id.
const (
	// OpLookupFound names an id whose operation is still stored online.
	OpLookupFound = "found"
	// OpLookupMissing names an id the document has never held an operation or
	// a retained identity for.
	OpLookupMissing = "missing"
	// OpLookupCompacted names an id whose online row was trimmed by
	// compaction; only its retained identity summary survives.
	OpLookupCompacted = "compacted"
)

// OpLookup is the answer for one id of a GetOpsByIDs batch, in the batch's
// request order. Status is OpLookupFound, OpLookupMissing or
// OpLookupCompacted.
//
// A found answer carries the online row's originating device, the document's
// fixed Type and the type-specific comparison content exactly as the row
// saved it: a counter carries the contribution in Value; a gset carries the
// element set in Elements (the stored array order); a register carries the
// Version and the Value stored verbatim; an orset carries the Action and the
// Element. A compacted or missing answer carries nothing but the id and its
// status: the retained identity summary is only consulted to tell the two
// apart and no content is ever restored from its digest.
type OpLookup struct {
	ID       string
	Status   string
	DeviceID string
	Type     string

	Value    json.RawMessage
	Elements []string
	Version  int64
	Action   string
	Element  string
}

// GetOpsByIDs answers a read-only batch lookup of CRDT operation ids for
// documentID in exactly the order given. The gate (registration then
// permission) is taken first inside one serialized transaction — an
// unregistered device yields store.ErrDeviceNotFound and a revoked device
// store.ErrPermissionDenied, before any CRDT content is observed. A document
// with no committed CRDT operation then yields ErrNotFound, the same verdict
// the state and snapshot reads give. Each remaining id is answered
// independently:
//
//   - an id with an online row is OpLookupFound with the originating device
//     and the comparison content saved with the row;
//   - an id whose merge row compaction trimmed is OpLookupCompacted: only the
//     retained identity summary is consulted, reporting the status alone — no
//     content is restored and the summary takes no part in any merge or other
//     read;
//   - any other id is OpLookupMissing.
//
// The HTTP layer rejects in-batch duplicates before this runs. The read starts
// a transaction but writes nothing, allocates no cursor, creates no operation
// and notifies no waiter or subscriber, so it cannot move the merge or wake a
// parked subscription. The verdicts derive solely from durable rows and are
// therefore byte-stable across a process restart.
func (s *Service) GetOpsByIDs(documentID, deviceID string, ids []string) ([]OpLookup, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated reads do: the device row and the
	// permission row are read before any CRDT content is observed, so a
	// rejected query cannot reveal content.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return nil, err
		}
	}

	// The document-existence verdict is the state read's: no type row means no
	// committed CRDT operation, which answers 404 rather than a page of
	// missing entries.
	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrNotFound
	case err != nil:
		return nil, err
	}

	out := make([]OpLookup, len(ids))
	for i, id := range ids {
		out[i] = OpLookup{ID: id, Status: OpLookupMissing}

		switch docType {
		case TypeCounter, TypeGSet:
			var device string
			var raw []byte
			err := tx.QueryRow(
				`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&device, &raw)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				retained, err := identityRetainedTx(tx, documentID, id)
				if err != nil {
					return nil, err
				}
				if retained {
					out[i].Status = OpLookupCompacted
				}
			case err != nil:
				return nil, err
			default:
				found := OpLookup{ID: id, Status: OpLookupFound, DeviceID: device, Type: docType}
				if docType == TypeCounter {
					// The stored blob is the contribution saved with the row.
					found.Value = json.RawMessage(raw)
				} else {
					var elements []string
					if err := json.Unmarshal(raw, &elements); err != nil {
						return nil, err
					}
					found.Elements = elements
				}
				out[i] = found
			}
		case TypeRegister:
			var device string
			var version int64
			var value []byte
			err := tx.QueryRow(
				`SELECT device_id, version, value FROM crdt_register_ops WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&device, &version, &value)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				retained, err := identityRetainedTx(tx, documentID, id)
				if err != nil {
					return nil, err
				}
				if retained {
					out[i].Status = OpLookupCompacted
				}
			case err != nil:
				return nil, err
			default:
				out[i] = OpLookup{
					ID:       id,
					Status:   OpLookupFound,
					DeviceID: device,
					Type:     docType,
					Version:  version,
					Value:    json.RawMessage(value),
				}
			}
		case TypeORSet:
			var device string
			var raw []byte
			err := tx.QueryRow(
				`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&device, &raw)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				// OR-set compaction never trims the operation log, so an absent
				// row has no retained identity; the generic fallback still
				// classifies any future trimmed row correctly.
				retained, err := identityRetainedTx(tx, documentID, id)
				if err != nil {
					return nil, err
				}
				if retained {
					out[i].Status = OpLookupCompacted
				}
			case err != nil:
				return nil, err
			default:
				var content struct {
					Action  string `json:"action"`
					Element string `json:"element"`
				}
				if err := json.Unmarshal(raw, &content); err != nil {
					return nil, err
				}
				out[i] = OpLookup{
					ID:       id,
					Status:   OpLookupFound,
					DeviceID: device,
					Type:     docType,
					Action:   content.Action,
					Element:  content.Element,
				}
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// identityRetainedTx reports whether a retained identity summary exists for
// id — i.e. whether the id was accepted and later trimmed by compaction. The
// summary's device and digest are deliberately not read: a compacted answer
// reports its status alone and never restores content.
func identityRetainedTx(tx *sql.Tx, documentID, id string) (bool, error) {
	var one int
	err := tx.QueryRow(
		`SELECT 1 FROM crdt_op_identities WHERE document_id = ? AND id = ?`,
		documentID, id,
	).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
