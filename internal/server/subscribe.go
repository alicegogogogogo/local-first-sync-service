package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// subscribePageLimit is the page size used while replaying changes already
// committed after the subscription cursor. It matches the largest page the
// paginated read endpoint accepts, so catch-up and reads stay page-aligned.
const subscribePageLimit = 1000

// wsCloseEchoWait bounds how long a server-initiated close waits for the
// client's close echo before the deferred TCP teardown.
const wsCloseEchoWait = 500 * time.Millisecond

// handleSubscribe is the WebSocket document subscription mounted under the
// session view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/changes/subscribe?cursor=N
//
// The subscription identity is entirely the existing session: the session's
// owning device is the only credential. Validation happens entirely before
// the 101 handshake and writes nothing:
//
//  1. cursor must be a non-negative decimal integer (400 JSON otherwise),
//  2. the request must carry a valid WebSocket upgrade handshake (400 JSON),
//  3. the session must currently exist (404 JSON),
//  4. its device must currently hold permission for the document (403 JSON).
//
// After the upgrade the server first sends every committed change with a
// cursor greater than the starting cursor — paged in cursor order, one text
// frame per record in the exact {"id","deviceId","payload","cursor"} shape of
// a paginated read row — then seamlessly continues with changes committed by
// any write path (batch post, merge, restore, replay). Frames are never
// reordered or coalesced; a later read from the last observed cursor returns
// the same rows. The connection is push-only: inbound data frames are read
// and discarded, only close/ping control frames are honored.
//
// A permission revoked after the upgrade ends the subscription with close
// code 4403 and no further frame follows; a termination signal ends every
// subscription with 1001. A client disconnect simply unregisters — like long
// polling, a subscription writes no change, cursor or other record.
func handleSubscribe(s *store.Store, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Parameter shape first: a bad cursor is a 400 even when the session or
	// handshake is also bad, matching the session-scoped read ordering.
	cursor, ok := parseSubscribeCursor(w, r)
	if !ok {
		return
	}
	if !checkWebSocketUpgrade(r) {
		writeError(w, http.StatusBadRequest, "a WebSocket upgrade handshake is required")
		return
	}

	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}
	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return
	}

	conn, err := upgradeWebSocket(w, r)
	if err != nil {
		// The 101 response could not be written and the connection has been
		// hijacked (or is gone), so no JSON error can be produced. Nothing
		// was registered or written to the store.
		return
	}

	serveSubscription(r, s, conn, documentID, deviceID, cursor)
}

// parseSubscribeCursor validates the mandatory cursor query parameter: it
// must be present and a decimal non-negative integer. Negative, fractional
// and non-numeric values are a 400 JSON error; signs and adornments are
// rejected.
func parseSubscribeCursor(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := r.URL.Query().Get("cursor")
	// parseCursorPath accepts only decimal digits (no sign, fraction or
	// exponent); a missing parameter is an empty raw and is rejected too.
	cursor, ok := parseCursorPath(raw)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return 0, false
	}
	return cursor, true
}

// serveSubscription runs one upgraded connection to completion. It registers
// before the first log read (a commit landing during catch-up is therefore
// never missed), then alternates a flush phase — drain every page of changes
// past the last sent cursor — and a park phase on the wake channel. A revoke
// is acted on only after the committed tail is flushed, so a commit that was
// authorized when it landed is still delivered ahead of the 4403. The method
// returns when the client leaves, permission is revoked or the store starts
// closing; the hijacked connection is closed on exit.
func serveSubscription(r *http.Request, s *store.Store, conn *wsConn, documentID, deviceID string, cursor int64) {
	// Registration precedes the first read: the wake channel is buffered, so
	// a commit during a read leaves a pending wake that makes the next park
	// return immediately — no wake is lost. The revoked channel closes once
	// if access is withdrawn, and that event survives a racing re-grant.
	wakeCh, revokedCh, unregister := s.AddSubscription(documentID, deviceID)
	defer func() {
		unregister()
		_ = conn.NetClose()
	}()

	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				// A malformed frame ends the connection with the RFC
				// protocol-error code; a client close is already echoed
				// inside ReadMessage; a network drop needs no frame.
				var protoErr *wsProtocolErrorString
				if errors.As(err, &protoErr) {
					_ = conn.Close(wsCloseProtocolError, "protocol error")
				}
				return
			}
			// Push-only channel: an inbound data frame is read and
			// discarded; the client cannot mutate the log over it.
		}
	}()

	// checkEnd non-blockingly maps a closing store to 1001 and a sticky
	// revoke to 4403; it returns true when the subscription must end. A
	// termination signal takes precedence: SIGTERM must end every
	// subscription with 1001 even if a revoke is also pending.
	checkEnd := func() bool {
		if s.Closing() {
			endSubscription(conn, clientGone, wsCloseGoingAway, "going away")
			return true
		}
		select {
		case <-revokedCh:
			endSubscription(conn, clientGone, wsClosePermissionRevoked, "permission revoked")
			return true
		default:
		}
		return false
	}

	position := cursor
	first := true
	for {
		if !first {
			// Park until a commit, a sticky revoke or the termination signal
			// wakes us. r.Context() covers a non-shutdown cancel; the read
			// pump covers a client disconnect.
			select {
			case <-wakeCh:
				// Drain any coalesced follow-up signal as well; the flush
				// below reconciles every case by re-reading from the last
				// sent cursor.
				select {
				case <-wakeCh:
				default:
				}
			case <-revokedCh:
				if s.Closing() {
					endSubscription(conn, clientGone, wsCloseGoingAway, "going away")
					return
				}
				endSubscription(conn, clientGone, wsClosePermissionRevoked, "permission revoked")
				return
			case <-clientGone:
				return
			case <-r.Context().Done():
				return
			}
			if s.Closing() {
				endSubscription(conn, clientGone, wsCloseGoingAway, "going away")
				return
			}
		}
		first = false

		// Flush phase: page every change past position in cursor order. A
		// termination signal or a revoke is honored between pages/rows so a
		// large catch-up cannot delay the closing frame. A revoke arriving
		// while the final already-authorized tail is in flight is checked
		// after the inner loop, so that tail still goes out first.
		for {
			if checkEnd() {
				return
			}
			select {
			case <-clientGone:
				return
			case <-r.Context().Done():
				return
			default:
			}

			changes, _, err := s.ListChanges(documentID, position, subscribePageLimit)
			if err != nil {
				_ = conn.Close(wsCloseInternalError, "internal error")
				return
			}
			for i := range changes {
				if checkEnd() {
					return
				}
				raw, marshalErr := json.Marshal(changes[i])
				if marshalErr != nil {
					_ = conn.Close(wsCloseInternalError, "internal error")
					return
				}
				if writeErr := conn.WriteText(raw); writeErr != nil {
					return
				}
				position = changes[i].Cursor
			}
			if len(changes) < subscribePageLimit {
				break
			}
		}

		// Caught up. A revoke that arrived during the last page (its tail was
		// just flushed) now ends the subscription; otherwise loop and park.
		// Closing still wins over a pending revoke.
		if checkEnd() {
			return
		}
	}
}

// endSubscription sends the mandated closing frame and briefly waits for the
// read pump to observe the peer's close echo, so a graceful close reaches the
// client instead of being truncated by the deferred TCP teardown. It returns
// promptly if the peer is already gone; the caller's deferred NetClose drops
// the socket either way.
func endSubscription(conn *wsConn, clientGone <-chan struct{}, code int, reason string) {
	_ = conn.Close(code, reason)
	timer := time.NewTimer(wsCloseEchoWait)
	defer timer.Stop()
	select {
	case <-clientGone:
	case <-timer.C:
	}
}
