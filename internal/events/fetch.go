package events

import (
	"database/sql"
	"encoding/json"
)

// Per-id fetch outcomes. A hit on an online change row is "online"; an id
// compaction trimmed is "compacted" and answers from its retained summary
// alone; an id that never appeared in the document is "missing".
const (
	FetchStatusOnline    = "online"
	FetchStatusCompacted = "compacted"
	FetchStatusMissing   = "missing"
)

// FetchedChange is one element of a FetchChanges result, answered in request
// order. An online hit carries the originating device, the payload exactly as
// it was stored and the change's cursor. A compacted id carries only its first
// cursor beside the trimmed marker — no device, no payload: the retained
// summary never participates in reads. A missing id carries no content at all.
type FetchedChange struct {
	ID       string          `json:"id"`
	Status   string          `json:"status"`
	DeviceID string          `json:"deviceId,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Cursor   int64           `json:"cursor,omitempty"`
}

// fetchIDChunk bounds the number of bound ids in one IN clause, staying clear
// of SQLite's variable limit regardless of how large the request body cap
// lets a batch grow.
const fetchIDChunk = 400

// FetchChangesAuthorized returns one answer per requested id, in the given
// order, after taking the same gate the document-level commit uses: an
// unregistered device yields store.ErrDeviceNotFound and a revoked device
// yields store.ErrPermissionDenied, both before any change content is read.
//
// The fetch is a pure read: it opens one transaction, takes the verdict and
// runs its SELECTs inside it, then rolls back. It inserts no row, allocates no
// cursor and notifies no waiter or subscriber, so a batch committed
// concurrently is observed either with all of its rows or none.
func (s *Service) FetchChangesAuthorized(documentID, deviceID string, ids []string) ([]FetchedChange, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, exactly like CompactChanges: the device and permission rows
	// are read before any change id is resolved, so a rejected fetch cannot
	// reveal content.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return nil, err
		}
	}

	out, err := fetchChangesTx(tx, documentID, ids)
	if err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

// fetchChangesTx resolves every id against the online log first and, for the
// misses, against the retained compaction summaries, assembling the answers in
// request order. A summary row exists only for an id compaction trimmed, so a
// miss there is a genuinely unknown id.
func fetchChangesTx(tx *sql.Tx, documentID string, ids []string) ([]FetchedChange, error) {
	online := make(map[string]ListedChange, len(ids))
	trimmed := make(map[string]int64, len(ids))

	for start := 0; start < len(ids); start += fetchIDChunk {
		end := start + fetchIDChunk
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]

		rows, err := tx.Query(
			`SELECT id, device_id, payload, cursor FROM changes
			 WHERE document_id = ? AND id IN (`+placeholders(len(chunk))+`)`,
			append([]any{documentID}, stringsToAny(chunk)...)...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var c ListedChange
			if err := rows.Scan(&c.ID, &c.DeviceID, &c.Payload, &c.Cursor); err != nil {
				_ = rows.Close()
				return nil, err
			}
			online[c.ID] = c
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	// Only ids absent online can be trimmed; look those up in the retained
	// summary table, in chunks over the same bound.
	misses := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := online[id]; !ok {
			misses = append(misses, id)
		}
	}
	for start := 0; start < len(misses); start += fetchIDChunk {
		end := start + fetchIDChunk
		if end > len(misses) {
			end = len(misses)
		}
		chunk := misses[start:end]

		rows, err := tx.Query(
			`SELECT id, cursor FROM change_identities
			 WHERE document_id = ? AND id IN (`+placeholders(len(chunk))+`)`,
			append([]any{documentID}, stringsToAny(chunk)...)...,
		)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var cursor int64
			if err := rows.Scan(&id, &cursor); err != nil {
				_ = rows.Close()
				return nil, err
			}
			trimmed[id] = cursor
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}

	out := make([]FetchedChange, len(ids))
	for i, id := range ids {
		if c, ok := online[id]; ok {
			out[i] = FetchedChange{
				ID:       c.ID,
				Status:   FetchStatusOnline,
				DeviceID: c.DeviceID,
				Payload:  c.Payload,
				Cursor:   c.Cursor,
			}
			continue
		}
		if cursor, ok := trimmed[id]; ok {
			out[i] = FetchedChange{ID: id, Status: FetchStatusCompacted, Cursor: cursor}
			continue
		}
		out[i] = FetchedChange{ID: id, Status: FetchStatusMissing}
	}
	return out, nil
}

// placeholders returns n comma-separated '?' bind markers.
func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	b := make([]byte, 0, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '?')
	}
	return string(b)
}

// stringsToAny widens a string slice for the variadic argument bind.
func stringsToAny(ss []string) []any {
	a := make([]any, len(ss))
	for i, s := range ss {
		a[i] = s
	}
	return a
}
