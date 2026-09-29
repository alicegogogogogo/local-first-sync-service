// Snapshot pruning keeps a long-running document's snapshot history from
// growing without a way to shrink it: a prune keeps only the most recent
// snapshots (by cursor) and hard-deletes every older one. Pruning is a
// retention layer over the snapshots table alone — it never moves the change
// cursor space, never rewrites the compaction boundary and never touches the
// provenance an already-accepted restore needs for its idempotency/conflict
// decision. A named version keeps mapping its name to the same cursor; when
// the snapshot that cursor named is pruned, the by-name read and by-name
// restore answer 404 exactly like the by-cursor ones.

package events

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// maxSnapshotKeep is the largest retention count a prune accepts.
const maxSnapshotKeep = 1000

// ErrPruneKeepInvalid reports that a prune request's keep is not a positive
// integer in 1..1000 (missing, zero, negative, a fraction, a string, a
// boolean, null or too large). It is checked inside the prune transaction
// after the registration/permission gate, so an unregistered or revoked
// caller fails first. The HTTP layer maps it to 400; nothing is written.
var ErrPruneKeepInvalid = errors.New("keep must be an integer between 1 and 1000")

// PruneResult reports the outcome of a prune: MaxCursor is the greatest cursor
// among the snapshots still stored for the document (zero when none remain),
// and Removed is the number of snapshots this call deleted.
type PruneResult struct {
	MaxCursor int64 `json:"maxCursor"`
	Removed   int64 `json:"removed"`
}

// PruneSnapshots keeps the keep most recent snapshots of documentID (the
// greatest keep cursors) and hard-deletes every older one, all in one
// serialized transaction, returning the greatest surviving cursor and the
// number deleted.
//
// The gate (registration then permission) is enforced first in the
// transaction, exactly as in CompactChanges: an unregistered device yields
// store.ErrDeviceNotFound and a revoked device store.ErrPermissionDenied,
// before the retention argument is validated or any snapshot row is observed.
// Only then must keep be an integer in 1..1000; an invalid value yields
// ErrPruneKeepInvalid and writes nothing.
//
// A document without snapshots — an unknown document included — prunes
// successfully with MaxCursor 0 and Removed 0 and creates no row. When the
// document holds no more than keep snapshots nothing is deleted and the call
// reports the standing maximum cursor with Removed 0, so a repeated prune is
// idempotent. Pruning allocates no cursor, writes no change record, leaves the
// compaction boundary and the change cursor space untouched and wakes no
// waiter or subscriber.
func (s *Service) PruneSnapshots(documentID, deviceID string, keep json.RawMessage) (PruneResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return PruneResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration and permission are enforced before the retention argument
	// is validated or any snapshot content is observed, matching the fixed
	// verdict order shape, device, permission, parameter.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return PruneResult{}, err
		}
	}

	n, ok := parsePruneKeep(keep)
	if !ok {
		return PruneResult{}, ErrPruneKeepInvalid
	}

	// The retained set is the newest n cursors: resolve the oldest retained
	// cursor (the n-th row in descending order) and hard-delete every snapshot
	// older than it. When fewer than n rows exist that lookup yields no row, so
	// no cutoff exists and the document is left intact. The whole judgment runs
	// on the single serialized connection inside this transaction, so a
	// snapshot created concurrently is either entirely among the rows this
	// prune observed or not present.
	var cutoff sql.NullInt64
	err = tx.QueryRow(
		`SELECT cursor FROM snapshots
		 WHERE document_id = ?
		 ORDER BY cursor DESC
		 LIMIT 1 OFFSET ?`,
		documentID, n-1,
	).Scan(&cutoff)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return PruneResult{}, err
	}

	var removed int64
	if cutoff.Valid {
		result, err := tx.Exec(
			`DELETE FROM snapshots WHERE document_id = ? AND cursor < ?`,
			documentID, cutoff.Int64,
		)
		if err != nil {
			return PruneResult{}, err
		}
		removed, err = result.RowsAffected()
		if err != nil {
			return PruneResult{}, err
		}
	}

	var maxCursor int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(cursor), 0) FROM snapshots WHERE document_id = ?`,
		documentID,
	).Scan(&maxCursor); err != nil {
		return PruneResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return PruneResult{}, err
	}
	return PruneResult{MaxCursor: maxCursor, Removed: removed}, nil
}

// parsePruneKeep reports whether raw is a JSON integer literal in the range
// 1..1000. Floats (1.0), exponents, strings, booleans, null, zero and
// negatives are rejected, mirroring the strict integer parsing the HTTP
// layer uses for the other positive-integer fields.
func parsePruneKeep(raw json.RawMessage) (int64, bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed[0] == '-' {
		return 0, false
	}
	for _, c := range trimmed {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || n < 1 || n > maxSnapshotKeep {
		return 0, false
	}
	return n, true
}
