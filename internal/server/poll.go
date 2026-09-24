package server

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// maxWaitMs bounds a long poll (inclusive): the spec allows 0..30000 ms.
const maxWaitMs = 30000

// errWaitCanceled reports that a long poll ended because the client
// disconnected or the server shut down, rather than with data or a timeout.
// No response is written in that case and nothing was recorded.
var errWaitCanceled = errors.New("long poll canceled")

// handlePollChanges is the waiting counterpart of handleListChanges. It
// returns committed changes past after in cursor order, using the same after/
// limit constraints and the same page shape, plus a timedOut flag.
//
//   - Changes already present return immediately, one page, timedOut=false.
//   - An unknown document returns immediately with an empty list, cursor 0
//     and timedOut=false; it never waits.
//   - A known document caught up at its tail holds the request for at most
//     waitMs milliseconds; the first committed change wakes it and the same
//     page is returned. Expiry yields an empty list, nextCursor == after and
//     timedOut=true — waiting never advances the cursor.
//
// The wait is canceled by a client disconnect (r.Context) or server shutdown
// (the store's shutdown channel); a canceled wait writes and records nothing.
func handlePollChanges(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	// after/limit share the exact rules of the non-waiting change listing.
	after, limit, ok := parseChangesQuery(w, r)
	if !ok {
		return
	}

	var waitMs int64
	if raw := r.URL.Query().Get("waitMs"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 || v > maxWaitMs {
			writeError(w, http.StatusBadRequest, "waitMs must be an integer between 0 and 30000")
			return
		}
		waitMs = v
	}

	changes, nextCursor, err := s.ListChanges(documentID, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list changes")
		return
	}

	timedOut := false
	if len(changes) == 0 {
		known, err := s.DocumentExists(documentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to look up document")
			return
		}
		if known {
			if waitMs == 0 {
				// A zero-length deadline ends immediately with no data; the
				// cursor is unchanged and the end is reported as a timeout.
				timedOut = true
			} else {
				changes, nextCursor, timedOut, err = waitForChanges(r, s, documentID, after, limit, waitMs)
				if err != nil {
					if errors.Is(err, errWaitCanceled) {
						return // disconnected or shutting down: no response
					}
					writeError(w, http.StatusInternalServerError, "failed to list changes")
					return
				}
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"changes":    changes,
		"nextCursor": nextCursor,
		"timedOut":   timedOut,
	})
}

// waitForChanges subscribes to the document's change notifications and blocks
// until the first committed change wakes it, the deadline elapses, the client
// disconnects or the server shuts down, then returns the fresh page.
//
// The subscription is established before the confirming re-read, so a commit
// landing in the gap between the handler's list and the subscription cannot be
// lost: the row is either visible to the re-read or its notification is
// buffered on the capacity-1 wake channel.
func waitForChanges(r *http.Request, s *store.Store, documentID string, after, limit, waitMs int64) (
	changes []store.ListedChange, nextCursor int64, timedOut bool, err error,
) {
	wake := make(chan struct{}, 1)
	unsubscribe := s.SubscribeChanges(documentID, wake)
	defer unsubscribe()

	// Close the subscribe/list race before settling in to wait.
	changes, nextCursor, err = s.ListChanges(documentID, after, limit)
	if err != nil {
		return nil, after, false, err
	}
	if len(changes) > 0 {
		return changes, nextCursor, false, nil
	}

	timer := time.NewTimer(time.Duration(waitMs) * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case <-wake:
			// A commit landed (signals coalesce): read the authoritative page.
			changes, nextCursor, err = s.ListChanges(documentID, after, limit)
			if err != nil {
				return nil, after, false, err
			}
			if len(changes) > 0 {
				return changes, nextCursor, false, nil
			}
			// A wake with no new rows is the shutdown fan-out; loop so the
			// shutdown/cancel cases below end the wait.
		case <-timer.C:
			// Nothing committed in time: empty page, original cursor, timeout.
			return changes, after, true, nil
		case <-r.Context().Done():
			return nil, after, false, errWaitCanceled
		case <-s.ShutdownChannel():
			return nil, after, false, errWaitCanceled
		}
	}
}

type replayRequest struct {
	DeviceID string     `json:"deviceId"`
	Changes  []changeIn `json:"changes"`
}

// handleReplay retries an offline batch against the change log. Each element
// is an ordinary change under the request's deviceId and shares the change
// log's idempotency rules, so the feature is one more caller of the same
// atomic batch commit normal posts use.
//
// Malformed input (wrong media type, invalid or trailing JSON, an empty or
// mistyped deviceId, a missing/empty changes array, an element without a
// non-empty string id or payload, or a duplicate id within the batch) is a
// 400 JSON error with zero writes. An unregistered device is 404; a revoked
// device is 403; an existing id whose deviceId or decoded payload differs is a
// 409 JSON error naming the conflicting id, with the batch untouched. Error
// responses never contain change contents.
func handleReplay(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req replayRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if req.Changes == nil || len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}

	changes := make([]store.Change, len(req.Changes))
	seen := make(map[string]struct{}, len(req.Changes))
	for i, c := range req.Changes {
		if c.ID == "" {
			writeError(w, http.StatusBadRequest, "each change must have a non-empty string id")
			return
		}
		if len(c.Payload) == 0 {
			writeError(w, http.StatusBadRequest, "each change must carry a JSON payload")
			return
		}
		if _, dup := seen[c.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id within batch: "+c.ID)
			return
		}
		seen[c.ID] = struct{}{}
		changes[i] = store.Change{ID: c.ID, DeviceID: req.DeviceID, Payload: c.Payload}
	}

	results, err := s.ReplayChanges(documentID, req.DeviceID, changes)
	if err != nil {
		var conflict *store.ErrConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "change id already exists with different deviceId or payload: "+conflict.ID)
		default:
			writeError(w, http.StatusInternalServerError, "failed to replay changes")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}
