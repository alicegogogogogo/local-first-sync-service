// Snapshot pruning keeps a long-running document's snapshot history from
// growing without bound: a prune keeps the most recent few snapshot cursors
// and hard-deletes every older snapshot, while leaving the change log, the
// compaction boundary, the retained idempotency summaries, the restore
// provenance and the named-version name-to-cursor markers exactly as they
// were.
//
// Pruning is not a change: it allocates no cursor, writes no change record and
// wakes no waiter or subscriber. The cursor space and the compaction boundary
// are untouched, so snapshot creation and change compaction keep their
// existing semantics around the prune. A pruned cursor is simply absent from
// then on: the single snapshot read, the interval export and a restore treat
// it like a cursor that never had a snapshot. A name marker still records its
// cursor, so reading or restoring by a name whose snapshot was pruned misses
// the same way.
//
// The gate (registration then permission) is taken first in the prune
// transaction, before the retention argument is validated and any snapshot is
// observed, exactly as in the other gated write entries: an unregistered
// device yields store.ErrDeviceNotFound and a revoked one
// store.ErrPermissionDenied, and neither reveals or removes anything. Only
// then is keep judged.

package events

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
)

// maxPruneKeep is the largest retention count a prune accepts.
const maxPruneKeep = 1000

// ErrInvalidKeep reports that a prune request carries no retention count, or
// one that is not an integer in 1..1000 (a fraction, string, boolean, null,
// zero or negative included). The caller maps it to 400; nothing is written.
var ErrInvalidKeep = errors.New("keep must be an integer between 1 and 1000")

// PruneSnapshotsResult is the prune answer: MaxCursor is the greatest snapshot
// cursor still stored after the prune (zero when none remain) and Deleted is
// the number of snapshots this call removed.
type PruneSnapshotsResult struct {
	MaxCursor int64 `json:"maxCursor"`
	Deleted   int64 `json:"deleted"`
}

// PruneSnapshots keeps the keep highest snapshot cursors of documentID and
// deletes every older snapshot, all in one serialized transaction, returning
// the greatest surviving cursor and the number of snapshots this call
// removed.
//
// The gate runs first (store.ErrDeviceNotFound for an unregistered device,
// store.ErrPermissionDenied for a revoked one); only then is keepRaw parsed —
// it must be present and an integer literal in 1..1000, anything else
// (missing, a fraction, string, boolean, null, zero or negative) yielding
// ErrInvalidKeep with zero writes — so a malformed argument against an
// unknown or unauthorized device still reports the gate failure first. A
// document without snapshots (an unknown document included) prunes
// successfully with a zero maximum cursor and nothing deleted; when the
// document holds no more than keep snapshots the call is an idempotent
// no-op that deletes nothing. Pruning writes no change, moves no boundary and
// notifies no waiter or subscriber; a repeat prune reports the same maximum
// with zero deleted.
func (s *Service) PruneSnapshots(documentID, deviceID string, keepRaw json.RawMessage) (PruneSnapshotsResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return PruneSnapshotsResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration and permission are enforced before the retention argument
	// is validated or any snapshot content is observed, so a rejected prune
	// cannot reveal or remove anything.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return PruneSnapshotsResult{}, err
		}
	}
	keep, err := parsePruneKeep(keepRaw)
	if err != nil {
		return PruneSnapshotsResult{}, err
	}

	// The cutoff is the smallest cursor among the keep highest cursors — the
	// keep-th row in descending order. When fewer than keep snapshots exist the
	// row is absent and nothing is deleted; every cursor strictly below the
	// cutoff is older than the retained tail, even when snapshot cursors have
	// gaps.
	var cutoff int64
	haveCutoff := true
	switch err := tx.QueryRow(
		`SELECT cursor FROM snapshots
		 WHERE document_id = ?
		 ORDER BY cursor DESC
		 LIMIT 1 OFFSET ?`,
		documentID, keep-1,
	).Scan(&cutoff); {
	case errors.Is(err, sql.ErrNoRows):
		haveCutoff = false
	case err != nil:
		return PruneSnapshotsResult{}, err
	}

	var deleted int64
	if haveCutoff {
		res, err := tx.Exec(
			`DELETE FROM snapshots WHERE document_id = ? AND cursor < ?`,
			documentID, cutoff,
		)
		if err != nil {
			return PruneSnapshotsResult{}, err
		}
		deleted, err = res.RowsAffected()
		if err != nil {
			return PruneSnapshotsResult{}, err
		}
	}

	// The answer is the greatest surviving snapshot cursor, zero when the
	// document now has none (an unknown document included).
	var maxCursor int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM snapshots WHERE document_id = ?`,
		documentID,
	).Scan(&maxCursor); err != nil {
		return PruneSnapshotsResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PruneSnapshotsResult{}, err
	}
	// Pruning is not a change: no waiter is woken and no subscriber is
	// signaled, because no new change exists to observe.
	return PruneSnapshotsResult{MaxCursor: maxCursor, Deleted: deleted}, nil
}

// parsePruneKeep validates the raw JSON retention count: it must be present
// and an integer literal of decimal digits in 1..1000. Fractions, exponents,
// strings, booleans, null, a missing field, zero and negatives are rejected.
func parsePruneKeep(raw json.RawMessage) (int64, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed[0] == '-' {
		return 0, ErrInvalidKeep
	}
	for _, c := range trimmed {
		if c < '0' || c > '9' {
			return 0, ErrInvalidKeep
		}
	}
	var keep int64
	if err := json.Unmarshal(raw, &keep); err != nil || keep < 1 || keep > maxPruneKeep {
		return 0, ErrInvalidKeep
	}
	return keep, nil
}
