// Session sync checkpoints are the durable answer to "which change cursor has
// this session fully applied locally on this document": one monotonically
// advancing cursor per (session, document) pair, persisted alongside the rest
// of the SQLite data so a client recovers its consumption position after a
// disconnect or a service restart.
//
// A confirmation is not a change: it allocates no cursor from the document's
// cursor space, writes no change row, wakes no long poll and signals no
// subscriber — the only row it touches is the checkpoint itself. Reading the
// change log never advances a checkpoint; only an explicit confirmation does.
//
// The cursor judgments, in their fixed order after the registration and
// permission gates:
//
//   - a confirmation below the recorded checkpoint is a regression
//     (ErrCheckpointRegression) and changes nothing;
//   - a confirmation above the document's high-water mark is out of range
//     (ErrCheckpointBeyond) and changes nothing;
//   - a first confirmation below the compaction boundary is rejected
//     (ErrCheckpointBoundary): the changes it skips have left the online log,
//     so the client must restore a snapshot first. Confirming exactly the
//     boundary is allowed — the snapshot covers everything up to it.
//
// Re-confirming the recorded cursor is idempotent (advanced=false) and writes
// nothing. Every judgment runs in one serialized transaction on the shared
// connection, so concurrent confirmations of the same pair stay monotonic: a
// smaller cursor can never overwrite a larger one. An unknown document has
// high-water mark and boundary zero, so only cursor 0 confirms on it, and the
// checkpoint row itself never makes the document exist.
package events

import (
	"fmt"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// ErrCheckpointRegression reports a confirmation below the checkpoint cursor
// already recorded for the (session, document) pair. The caller maps it to
// 409 with Current as currentCursor; nothing is written.
type ErrCheckpointRegression struct {
	Current int64
}

func (e *ErrCheckpointRegression) Error() string {
	return fmt.Sprintf("checkpoint cursor regresses below the recorded cursor %d", e.Current)
}

// ErrCheckpointBeyond reports a confirmation above the document's high-water
// mark: the cursor names a change that does not exist. The caller maps it to
// 409 with Max as maxCursor; nothing is written.
type ErrCheckpointBeyond struct {
	Max int64
}

func (e *ErrCheckpointBeyond) Error() string {
	return fmt.Sprintf("checkpoint cursor is beyond the document's maximum cursor %d", e.Max)
}

// ErrCheckpointBoundary reports a first confirmation below the compaction
// boundary: the changes it would skip have left the online log, so the client
// must restore a snapshot before confirming. The caller maps it to 409 with
// Boundary as boundary; nothing is written.
type ErrCheckpointBoundary struct {
	Boundary int64
}

func (e *ErrCheckpointBoundary) Error() string {
	return fmt.Sprintf("first checkpoint cursor is below the compaction boundary %d", e.Boundary)
}

// CheckpointState is the read model of one (session, document) pair: the
// recorded checkpoint cursor (zero and Recorded=false when the pair was never
// confirmed), the document's current compaction boundary and its high-water
// mark. An unknown document reports Boundary and MaxCursor zero.
type CheckpointState struct {
	Cursor    int64
	Recorded  bool
	Boundary  int64
	MaxCursor int64
}

// ConfirmCheckpoint records cursor as the (session, document) pair's confirmed
// checkpoint and reports whether the stored position advanced.
//
// The gate (registration, then permission) is enforced inside the same
// serialized transaction as the cursor judgments and the write, so a revoked
// device confirms nothing and a rejected request observes no checkpoint state.
// A regression, an out-of-range cursor and a first confirmation below the
// compaction boundary are rejected with zero writes. Re-confirming the
// recorded cursor commits nothing and reports advanced=false. A successful
// confirmation writes only the checkpoint row: no change is created, no
// document cursor is allocated and no waiter or subscriber is signaled.
func (s *Service) ConfirmCheckpoint(sessionID, deviceID, documentID string, cursor int64) (advanced bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first: the device row and the permission row are read before any
	// checkpoint or change-log state is observed.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return false, err
		}
	}

	stored, recorded, err := store.CheckpointTx(tx, sessionID, documentID)
	if err != nil {
		return false, err
	}
	if recorded && cursor < stored {
		return false, &ErrCheckpointRegression{Current: stored}
	}
	maxCursor, err := currentCursorTx(tx, documentID)
	if err != nil {
		return false, err
	}
	if cursor > maxCursor {
		return false, &ErrCheckpointBeyond{Max: maxCursor}
	}
	if !recorded {
		boundary, err := boundaryOfTx(tx, documentID)
		if err != nil {
			return false, err
		}
		if cursor < boundary {
			return false, &ErrCheckpointBoundary{Boundary: boundary}
		}
	}

	if recorded && cursor == stored {
		// Idempotent re-confirmation: nothing is written.
		return false, tx.Commit()
	}
	if err := store.PutCheckpointTx(tx, sessionID, documentID, cursor); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	// A checkpoint is not a change: no waiter is woken and no subscriber is
	// signaled, because no new change exists to observe.
	return true, nil
}

// ReadCheckpoint returns the (session, document) pair's checkpoint state: the
// recorded cursor (zero with Recorded=false when never confirmed) together
// with the document's compaction boundary and high-water mark. The read runs
// in one serialized transaction and writes nothing; the session existence and
// permission checks are the HTTP layer's, exactly as on the session change
// reads.
func (s *Service) ReadCheckpoint(sessionID, documentID string) (CheckpointState, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return CheckpointState{}, err
	}
	defer func() { _ = tx.Rollback() }()

	cursor, recorded, err := store.CheckpointTx(tx, sessionID, documentID)
	if err != nil {
		return CheckpointState{}, err
	}
	boundary, err := boundaryOfTx(tx, documentID)
	if err != nil {
		return CheckpointState{}, err
	}
	maxCursor, err := currentCursorTx(tx, documentID)
	if err != nil {
		return CheckpointState{}, err
	}
	if err := tx.Commit(); err != nil {
		return CheckpointState{}, err
	}
	return CheckpointState{
		Cursor:    cursor,
		Recorded:  recorded,
		Boundary:  boundary,
		MaxCursor: maxCursor,
	}, nil
}
