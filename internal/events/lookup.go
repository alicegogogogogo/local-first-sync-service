package events

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// Lookup statuses reported per queried id.
const (
	// LookupFound names an id whose change is still in the online log.
	LookupFound = "found"
	// LookupMissing names an id the document has never held a row or a
	// retained summary for.
	LookupMissing = "missing"
	// LookupCompacted names an id whose online row was trimmed by
	// compaction; only its retained summary survives.
	LookupCompacted = "compacted"
)

// ChangeLookup is the answer for one id of a GetChangesByIDs batch, in the
// batch's request order. Status is LookupFound, LookupMissing or
// LookupCompacted.
//
// A found answer carries the online row's originating device, the payload
// exactly as it was stored at commit time and the cursor it was first
// assigned. A compacted answer carries only that first cursor from the
// retained summary — DeviceID and Payload are left empty and never restored.
// A missing answer carries nothing but the id and its status.
type ChangeLookup struct {
	ID       string
	Status   string
	DeviceID string
	Payload  json.RawMessage
	Cursor   int64
}

// GetChangesByIDs answers a read-only batch lookup of change ids for
// documentID in exactly the order given. The gate (registration then
// permission) is taken first inside one serialized transaction — an
// unregistered device yields store.ErrDeviceNotFound and a revoked device
// store.ErrPermissionDenied, before any change content is observed. Then each
// id is answered independently:
//
//   - an id with an online row is LookupFound with its device, verbatim
//     stored payload and first cursor;
//   - an id whose row compaction trimmed is LookupCompacted, reporting only
//     the first cursor held in its retained summary — no payload is restored
//     and the summary takes no part in any read other than this verdict;
//   - any other id (one the document has never known, an unknown document
//     included) is LookupMissing.
//
// Ids may repeat positionally in the answer only when the caller supplied
// them repeatedly; the HTTP layer rejects in-batch duplicates before this
// runs. The read starts a transaction but writes nothing, allocates no
// cursor, creates no change and notifies no waiter or subscriber, so it
// cannot move a cursor or wake a parked poll. The verdicts derive solely from
// durable rows and are therefore byte-stable across a process restart.
func (s *Service) GetChangesByIDs(documentID, deviceID string, ids []string) ([]ChangeLookup, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly as the gated reads do: the device row and the
	// permission row are read before any change id is observed, so a rejected
	// query cannot reveal content.
	if err := gateTx(s, tx, documentID, deviceID); err != nil {
		return nil, err
	}

	out := make([]ChangeLookup, len(ids))
	for i, id := range ids {
		out[i] = ChangeLookup{ID: id, Status: LookupMissing}

		var device string
		var payload []byte
		var cursor int64
		err := tx.QueryRow(
			`SELECT device_id, payload, cursor FROM changes
			 WHERE document_id = ? AND id = ?`,
			documentID, id,
		).Scan(&device, &payload, &cursor)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// No online row: a compaction-trimmed id keeps a retained summary
			// that still records its first cursor. Anything else never existed.
			var trimmedCursor int64
			err := tx.QueryRow(
				`SELECT cursor FROM change_identities
				 WHERE document_id = ? AND id = ?`,
				documentID, id,
			).Scan(&trimmedCursor)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				// Stays LookupMissing with a zero value.
			case err != nil:
				return nil, err
			default:
				out[i] = ChangeLookup{
					ID:     id,
					Status: LookupCompacted,
					Cursor: trimmedCursor,
				}
			}
		case err != nil:
			return nil, err
		default:
			out[i] = ChangeLookup{
				ID:       id,
				Status:   LookupFound,
				DeviceID: device,
				Payload:  json.RawMessage(payload),
				Cursor:   cursor,
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}
