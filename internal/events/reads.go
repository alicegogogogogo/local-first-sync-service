package events

import (
	"context"
	"time"
)

// DocumentExists reports whether documentID is a known document rather than
// an empty namespace: it has any online change row, or a compaction state row
// (a fully compacted document stays known even with an empty online log).
func (s *Service) DocumentExists(documentID string) (bool, error) {
	var known bool
	if err := s.db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM changes WHERE document_id = ?)
		 OR EXISTS(SELECT 1 FROM change_log_state WHERE document_id = ?)`,
		documentID, documentID,
	).Scan(&known); err != nil {
		return false, err
	}
	return known, nil
}

// ListChanges returns at most limit changes for documentID whose cursor is
// greater than after and still online, in cursor order, together with the
// cursor to pass as the next after value. An unknown document yields an empty
// list and cursor 0; a known document with no rows past after yields
// nextCursor == max(after, compaction boundary), so a caller resuming inside
// the trimmed range lands past the boundary instead of below it.
func (s *Service) ListChanges(documentID string, after, limit int64) ([]ListedChange, int64, error) {
	rows, err := s.db.Query(
		`SELECT id, device_id, payload, cursor FROM changes
		 WHERE document_id = ? AND cursor > ?
		 ORDER BY cursor ASC
		 LIMIT ?`,
		documentID, after, limit,
	)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]ListedChange, 0)
	var maxCursor int64 = after
	for rows.Next() {
		var c ListedChange
		if err := rows.Scan(&c.ID, &c.DeviceID, &c.Payload, &c.Cursor); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
		maxCursor = c.Cursor
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	// Distinguish an unknown document from a known one with nothing new:
	// unknown documents must report nextCursor 0. A known document reports
	// the larger of the requested cursor and the compaction boundary, since
	// everything at or below the boundary left the online log for good.
	if len(out) == 0 {
		known, err := s.DocumentExists(documentID)
		if err != nil {
			return nil, 0, err
		}
		if !known {
			return out, 0, nil
		}
		boundary, err := s.compactionBoundary(documentID)
		if err != nil {
			return nil, 0, err
		}
		if boundary > maxCursor {
			maxCursor = boundary
		}
	}
	return out, maxCursor, nil
}

// WaitForChanges blocks until documentID has a change with cursor greater than
// after, a change is committed while waiting, wait elapses, ctx is canceled
// (the client disconnected) or the service stops. It then returns the current
// page of up to limit changes exactly as ListChanges would, with timedOut=true
// only when the wait deadline expired with no new rows.
//
// Registration precedes the first query: a commit landing between the read and
// the park still notifies a registered channel, so no change is missed.
//
// An unknown document is never parked on: it returns immediately with an empty
// list and nextCursor 0, timedOut=false. Data already past after is likewise
// returned immediately. wait <= 0 is an immediately expired deadline.
func (s *Service) WaitForChanges(ctx context.Context, documentID string, after, limit int64, wait time.Duration) (changes []ListedChange, nextCursor int64, timedOut bool, err error) {
	ch, remove := s.registerWait(documentID)
	defer remove()

	changes, nextCursor, err = s.ListChanges(documentID, after, limit)
	if err != nil || len(changes) > 0 {
		return changes, nextCursor, false, err
	}

	// Empty page: distinguish an unknown document (immediate empty/0, never
	// parked on) from a known document caught up to after.
	known, err := s.DocumentExists(documentID)
	if err != nil || !known {
		return changes, 0, false, err
	}

	// Known document, nothing new: a zero wait is an immediately expired
	// deadline; ListChanges already echoes nextCursor == after.
	if wait <= 0 {
		return changes, nextCursor, true, nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ch:
		// Woken by a committed change or by InterruptWaits.
		s.mu.Lock()
		closing := s.closed
		s.mu.Unlock()
		if closing {
			return nil, 0, false, ErrStoreClosing
		}
		changes, nextCursor, err = s.ListChanges(documentID, after, limit)
		return changes, nextCursor, len(changes) == 0, err
	case <-timer.C:
		// Timeout with no new rows: echo the caller's cursor without
		// advancing it. The document was known above, so ListChanges returns
		// nextCursor == after for an empty page.
		changes, nextCursor, err = s.ListChanges(documentID, after, limit)
		return changes, nextCursor, len(changes) == 0, err
	case <-ctx.Done():
		return nil, 0, false, ctx.Err()
	}
}

// registerWait parks a channel for documentID and returns it together with a
// removal function. The channel is closed on the next committed change to the
// document, or when the service stops.
func (s *Service) registerWait(documentID string) (ch chan struct{}, remove func()) {
	ch = make(chan struct{}, 1)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		// Shutdown beat the registration: signal immediately so the caller does
		// not park on a channel nobody will close.
		close(ch)
		return ch, func() {}
	}
	s.nextWait++
	id := s.nextWait
	set := s.waits[documentID]
	if set == nil {
		set = make(map[uint64]chan struct{})
		s.waits[documentID] = set
	}
	set[id] = ch
	return ch, func() {
		s.mu.Lock()
		if set, ok := s.waits[documentID]; ok {
			delete(set, id)
			if len(set) == 0 {
				delete(s.waits, documentID)
			}
		}
		s.mu.Unlock()
	}
}

// notifyWaiters signals every long poll parked on documentID and every
// subscription open on it. It is called only after a change-bearing
// transaction has committed, so parked readers observe the new rows when they
// re-query and pushed frames describe committed changes.
func (s *Service) notifyWaiters(documentID string) {
	s.mu.Lock()
	set := s.waits[documentID]
	delete(s.waits, documentID)
	s.mu.Unlock()
	for _, ch := range set {
		close(ch)
	}
	s.signalSubscribers(documentID)
}
