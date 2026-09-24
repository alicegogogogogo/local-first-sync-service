package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
	"github.com/alicegogogogogo/local-first-sync-service/internal/ws"
)

// subscribePageLimit bounds how many backlog rows one read returns; the push
// loop reads page after page in cursor order, so a large backlog is delivered
// in the same shape and order as paginated GET reads rather than in one query.
const subscribePageLimit = 1000

// handleSubscribe is the WebSocket document subscription mounted under the
// session view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/subscribe?after=<cursor>
//
// The client opens the upgrade with its existing session id, the document id
// and a non-negative start cursor. Validation is entirely pre-upgrade and
// answers ordinary JSON HTTP errors: a negative/fractional/non-numeric after,
// a non-GET method, an empty path identifier or a missing/invalid handshake is
// 400; an unknown or deleted session is 404; a session device whose document
// permission was revoked is 403. None of those establish a connection or write
// anything.
//
// Once upgraded, every change strictly after the start cursor is delivered one
// text frame per record, each frame exactly the shape of one row of GET
// changes (id, deviceId, payload, cursor), in ascending cursor order with no
// reordering or coalescing. Rows already committed are backfilled first; the
// subscription registers with the store before its first read so a commit that
// lands during catch-up is not missed, and then parks until the next commit —
// ordinary post, merge, restore or replay all wake it. Revocation after
// connect ends the subscription with close code 4403; the termination signal
// ends every subscription with 1001. The channel is push-only: it writes no
// change, cursor or other record, and inbound messages are ignored.
func handleSubscribe(s *store.Store, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// The start cursor is a non-negative integer, default 0. Shape validation
	// precedes the resource lookup, matching the session-scoped GET changes.
	after := int64(0)
	if raw := r.URL.Query().Get("after"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "after must be a non-negative integer")
			return
		}
		after = v
	}

	// The WebSocket handshake must be present before the upgrade. Checking it
	// here (still an ordinary HTTP response) keeps the 400 JSON contract.
	if err := ws.ValidateUpgradeHeaders(r); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Session must currently exist; its owner is the subscribing device.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	// Pre-connect permission gate, identical to the session-scoped read.
	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return
	}

	// Register before the upgrade and the first read: registration is the
	// point after which no commit can land unseen. If the store is already
	// shutting down, answer 503 rather than upgrading a doomed connection.
	sub, ok := s.Subscribe(documentID, deviceID)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "service is shutting down")
		return
	}
	defer sub.Close()

	conn, err := ws.Accept(w, r)
	if err != nil {
		// The response has already been hijacked (or the 101 failed mid-write);
		// no HTTP error can be emitted. Releasing the subscription is the only
		// cleanup left.
		return
	}
	defer conn.Close()

	cursor := after
	for {
		// A wake may be a revocation or a shutdown rather than a commit; check
		// both before reading so no further change is pushed after access is
		// withdrawn and a 1001 wins over any late read.
		authorized, err := s.DocumentAuthorized(documentID, deviceID)
		if err != nil {
			_ = conn.WriteClose(1011, "internal error")
			return
		}
		if !authorized {
			_ = conn.WriteClose(ws.ClosePermissionRevoked, "permission for this document revoked")
			return
		}
		if s.Closing() {
			_ = conn.WriteClose(ws.CloseGoingAway, "service is shutting down")
			return
		}

		// Read the next page from the last delivered cursor. Backlog pages are
		// drained in a tight loop so the wire order is exactly the ascending
		// order of paginated reads.
		changes, _, err := s.ListChanges(documentID, cursor, subscribePageLimit)
		if err != nil {
			_ = conn.WriteClose(1011, "internal error")
			return
		}
		for i := range changes {
			data, err := json.Marshal(changes[i])
			if err != nil {
				_ = conn.WriteClose(1011, "internal error")
				return
			}
			if err := conn.WriteText(data); err != nil {
				// The peer is gone; release the subscription without writing
				// anything. The read loop has already marked the conn done.
				return
			}
			cursor = changes[i].Cursor
		}
		if int64(len(changes)) == subscribePageLimit {
			continue
		}

		// Caught up. Park until a committed change, a revocation or shutdown
		// pings the subscription, or until the peer disconnects. The buffered,
		// coalescing signal collapses a burst of commits into one wake; the
		// re-read above still delivers every row from the committed cursor.
		select {
		case <-sub.Signal():
		case <-conn.Done():
			return
		case <-r.Context().Done():
			return
		}
	}
}
