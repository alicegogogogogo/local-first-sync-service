// Package changelog is the change-event service: the durable, per-document
// log of client-supplied changes with its contiguous cursor space.
//
// Within a document every change has a client-supplied id. New ids receive a
// monotonically increasing cursor; re-posting an existing id is idempotent
// only when the deviceId and decoded JSON payload match, otherwise it is a
// conflict. Valid batches commit atomically in one serialized transaction, so
// cursors never repeat and no record is partially written, even under
// concurrent writers. The same transaction and cursor space serve ordinary
// posts, offline replays, three-way merges and snapshot restores.
//
// The service also owns:
//
//   - paged, cursor-ordered reads (ListChanges) and the long-poll wait over
//     them (WaitForChanges);
//   - snapshots: caller-supplied JSON states pinned to existing change
//     cursors, write-once per cursor;
//   - the in-memory change subscriptions pushed to after a change-bearing
//     commit.
//
// The service owns its tables (changes, snapshots, restores) and never reads
// or writes another service's storage or fields. The two things it needs from
// the rest of the system arrive through its Gate boundary — device
// registration and the permission decision — both enforced *inside* the
// service's own transaction, so a replay commit, a compaction elsewhere and a
// permission change are always judged in one serialized transaction and a
// rejected call never observes content it is not allowed to see.
package changelog

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrStoreClosing reports that the service is shutting down and parked long
// polls have been woken so they can return without writing anything. The
// caller maps it to 503.
var ErrStoreClosing = errors.New("store is closing")

// ErrStaleCursor reports that a merge targets a base cursor for an unknown
// document, or a base cursor greater than the document's current cursor. The
// caller maps it to 400; nothing is written.
var ErrStaleCursor = errors.New("baseCursor is unknown or ahead of the current cursor")

// ErrConflict reports that a batch references an existing change id with a
// different deviceId or payload. The whole batch is rejected.
type ErrConflict struct {
	ID string
}

func (e *ErrConflict) Error() string {
	return fmt.Sprintf("conflicting change %q for document", e.ID)
}

// ErrSnapshotBase reports that a snapshot targets an unknown document or a
// cursor that is not an existing change cursor of the document. The caller
// maps it to 400; nothing is written.
var ErrSnapshotBase = errors.New("snapshot cursor is not an existing cursor of the document")

// ErrSnapshotNotFound reports that no snapshot exists for the document and
// cursor. The caller maps it to 404.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// ErrSnapshotConflict reports that a snapshot already exists for the document
// and cursor with a different state. The stored snapshot is left unchanged.
type ErrSnapshotConflict struct {
	Cursor int64
}

func (e *ErrSnapshotConflict) Error() string {
	return fmt.Sprintf("conflicting snapshot for cursor %d", e.Cursor)
}

// ErrRestoreConflict reports that a restore references a change id that is
// already taken by an ordinary change, or by a prior restore whose deviceId,
// snapshot cursor or source state differs. Nothing is written; the caller
// maps it to 409.
type ErrRestoreConflict struct {
	ID string
}

func (e *ErrRestoreConflict) Error() string {
	return fmt.Sprintf("change id %q conflicts with the change log", e.ID)
}

// Change is one element of an inbound batch or one row of a listing.
type Change struct {
	ID       string          // client-supplied change id, unique per document
	DeviceID string          // originating device
	Payload  json.RawMessage // decoded JSON value, stored verbatim
}

// Result reports the outcome for one element of an accepted batch.
type Result struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
	Cursor  int64  `json:"cursor"`
}

// ListedChange is one row in a ListChanges response.
type ListedChange struct {
	ID       string          `json:"id"`
	DeviceID string          `json:"deviceId"`
	Payload  json.RawMessage `json:"payload"`
	Cursor   int64           `json:"cursor"`
}

// MergeResult reports the outcome of an accepted MergeChange. Outcome is one
// of "idempotent", "applied" or "merged".
type MergeResult struct {
	ID      string  `json:"id"`
	Outcome string  `json:"outcome"`
	Cursor  int64   `json:"cursor"`
	Result  *Result `json:"result,omitempty"`
}

// RestoreResult reports the outcome of an accepted RestoreSnapshot.
type RestoreResult struct {
	ID           string `json:"id"`
	Created      bool   `json:"created"`
	Cursor       int64  `json:"cursor"`
	RestoredFrom int64  `json:"restoredFrom"`
}

// jsonEqual reports whether two payloads are equal after JSON decoding, so
// 1 and 1.0, or {"a":1,"b":2} and {"b":2,"a":1}, compare equal.
func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return deepEqual(va, vb)
}

func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			w, exists := bv[k]
			if !exists || !deepEqual(v, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
