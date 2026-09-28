// Read-only batch lookup over a document's accepted CRDT operations. It
// mirrors the change-log batch lookup: the caller names a non-empty set of
// operation ids and receives one answer per id, in request order, each either
// still online ("found"), trimmed by compaction ("compacted", reported from
// the retained identity alone) or never known to the document ("missing").
// The read starts a transaction but writes nothing: it allocates no cursor,
// creates no operation and notifies no subscriber.

package crdt

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// Lookup statuses reported per queried operation id.
const (
	// LookupFound names an id whose operation is still in the online tables.
	LookupFound = "found"
	// LookupMissing names an id the document has never held an operation or a
	// retained identity for.
	LookupMissing = "missing"
	// LookupCompacted names an id whose online row compaction trimmed; only
	// its fixed-size retained identity survives, and the lookup reports no
	// content for it.
	LookupCompacted = "compacted"
)

// OpLookup is the answer for one id of a GetOpsByIDs batch, in the batch's
// request order. Status is LookupFound, LookupMissing or LookupCompacted.
//
// A found answer carries the online row's originating device in DeviceID and
// the document's fixed type in Type, together with the type-specific
// comparison content exactly as it is stored: for a counter the contribution
// in Value (a JSON integer); for a gset the element set in Elements (the
// stored JSON array); for a register the version in Version and the value in
// Value; for an orset the action in Action and the element in Element. A
// compacted or missing answer carries nothing but the id and its status: the
// retained identity stores only a device and a content digest, neither of
// which this read restores.
type OpLookup struct {
	ID       string
	Status   string
	DeviceID string
	Type     string

	Value    json.RawMessage // counter: contribution; register: the value, verbatim
	Elements json.RawMessage // gset: the stored JSON array of elements
	Version  int64           // register: logical version
	Action   string          // orset: ORSetAdd or ORSetRemove
	Element  string          // orset: the element
}

// GetOpsByIDs answers a read-only batch lookup of CRDT operation ids for
// documentID in exactly the order given. The gate (registration then
// permission) is taken first inside one serialized transaction — an
// unregistered device yields store.ErrDeviceNotFound and a revoked device
// store.ErrPermissionDenied, before any CRDT content is observed. A document
// with no committed CRDT operation then yields ErrNotFound (the caller answers
// 404, the same verdict the state read gives).
//
// Each id is answered independently:
//
//   - an id with an online row is LookupFound with its device and the stored
//     type-specific comparison content;
//   - an id whose row compaction trimmed is LookupCompacted, reported from
//     the retained identity alone — no device or content is restored and the
//     identity table takes no part in any other read;
//   - any other id (one the document has never known) is LookupMissing.
//
// The read writes nothing, allocates no cursor and notifies no waiter or
// subscriber, so it cannot move the merged state or wake a subscription. The
// verdicts derive solely from durable rows and are therefore byte-stable
// across a process restart.
func (s *Service) GetOpsByIDs(documentID, deviceID string, ids []string) ([]OpLookup, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated commits and the change-log lookup do:
	// the device row and the permission row are read before any CRDT content
	// is observed, so a rejected query cannot reveal type or state.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return nil, err
		}
	}

	// The document's type row exists exactly when it has ever held a CRDT
	// operation; a query against a document with none gets the same 404 the
	// state read gives, rather than a page of missing answers.
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
		out[i] = OpLookup{ID: id, Status: LookupMissing}

		switch docType {
		case TypeCounter, TypeGSet:
			var device string
			var value []byte
			err := tx.QueryRow(
				`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&device, &value)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				if compacted, cErr := opTrimmed(tx, documentID, id); cErr != nil {
					return nil, cErr
				} else if compacted {
					out[i] = OpLookup{ID: id, Status: LookupCompacted}
				}
			case err != nil:
				return nil, err
			default:
				out[i] = OpLookup{
					ID:       id,
					Status:   LookupFound,
					DeviceID: device,
					Type:     docType,
				}
				if docType == TypeCounter {
					out[i].Value = json.RawMessage(value)
				} else {
					out[i].Elements = json.RawMessage(value)
				}
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
				if compacted, cErr := opTrimmed(tx, documentID, id); cErr != nil {
					return nil, cErr
				} else if compacted {
					out[i] = OpLookup{ID: id, Status: LookupCompacted}
				}
			case err != nil:
				return nil, err
			default:
				out[i] = OpLookup{
					ID:       id,
					Status:   LookupFound,
					DeviceID: device,
					Type:     docType,
					Version:  version,
					Value:    json.RawMessage(value),
				}
			}
		case TypeORSet:
			// The orset operation log is never compacted and compaction
			// retains no orset identity, so an orset id is either online or
			// missing.
			var device string
			var content []byte
			err := tx.QueryRow(
				`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&device, &content)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				// Stays LookupMissing: orset compaction trims no operation.
			case err != nil:
				return nil, err
			default:
				var stored struct {
					Action  string `json:"action"`
					Element string `json:"element"`
				}
				if err := json.Unmarshal(content, &stored); err != nil {
					return nil, err
				}
				out[i] = OpLookup{
					ID:       id,
					Status:   LookupFound,
					DeviceID: device,
					Type:     docType,
					Action:   stored.Action,
					Element:  stored.Element,
				}
			}
		default:
			return nil, errors.New("unknown crdt type stored for document")
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// opTrimmed reports whether a retained identity exists for id, i.e. whether an
// id missing from the online tables was trimmed by compaction. It restores
// neither the originating device nor the digest content.
func opTrimmed(tx *sql.Tx, documentID, id string) (bool, error) {
	var dummy []byte
	err := tx.QueryRow(
		`SELECT digest FROM crdt_op_identities WHERE document_id = ? AND id = ?`,
		documentID, id,
	).Scan(&dummy)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}
