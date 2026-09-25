package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleCRDTStateSubscribe is the WebSocket subscription over a document's
// merged CRDT state, mounted under the session view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe
//
// The subscription identity is entirely the existing session: the session's
// owning device is the only credential; no new auth is introduced. As with
// the change-log subscription, validation happens entirely before the 101
// handshake and writes nothing:
//
//  1. the request must carry a valid WebSocket upgrade handshake (400 JSON),
//  2. the session must currently exist (404 JSON),
//  3. its device must currently hold permission for the document (403 JSON).
//
// After the upgrade the server first sends the current merged state when one
// exists. A document with no committed CRDT operations produces no frame: the
// connection waits silently and the first state to appear is its first
// frame. Every later batch that actually changes the merged value is then
// pushed, one text frame per distinct value, in transaction commit order.
// Idempotent repeats, rejected counter regressions and set additions of
// already-present elements change nothing and produce no frame.
//
// Each frame is byte-for-byte the body of GET .../crdt/state: compact,
// single-line JSON with keys in type/value order and a trailing newline. The
// connection is push-only: inbound data frames are read and discarded, only
// close/ping control frames are honored. A permission revoked after the
// upgrade ends the subscription with close code 4403 (sticky: a re-grant
// never revives it); a termination signal ends every subscription with 1001.
func handleCRDTStateSubscribe(s *store.Store, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Handshake shape first: a malformed upgrade is a 400 even when the
	// session is also missing.
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

	serveCRDTStateSubscription(r, s, conn, documentID, deviceID)
}

// crdtStateFrame is the ordered on-wire shape of a CRDT state frame. An
// explicit struct guarantees the documented type/value key order without
// relying on encoding/json's map-key sorting.
type crdtStateFrame struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// encodeCRDTStateFrame renders a state exactly like the state-read body:
// compact JSON followed by one newline.
func encodeCRDTStateFrame(state store.CRDTState) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(crdtStateFrame{
		Type:  state.Type,
		Value: state.Value,
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// serveCRDTStateSubscription runs one upgraded CRDT subscription to
// completion. Registration and the initial merged-state read are one atomic
// step under the store's CRDT lock (a commit is reflected either in the
// initial state or in the subscription's queue, never in both or neither),
// so the handler only has to send the initial state and then drain queued
// states in commit order. It returns when the client leaves, permission is
// revoked or the store starts closing; the hijacked connection is closed on
// exit.
func serveCRDTStateSubscription(r *http.Request, s *store.Store, conn *wsConn, documentID, deviceID string) {
	sub, initial, hasState, err := s.SubscribeCRDT(documentID, deviceID)
	if err != nil {
		_ = conn.Close(wsCloseInternalError, "internal error")
		_ = conn.NetClose()
		return
	}
	defer func() {
		sub.Unregister()
		_ = conn.NetClose()
	}()

	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				// A malformed frame ends the connection with the RFC
				// protocol-error code; a client close is already echoed
				// inside ReadMessage; a network drop needs no frame.
				var protoErr *wsProtocolErrorString
				if errors.As(readErr, &protoErr) {
					_ = conn.Close(wsCloseProtocolError, "protocol error")
				}
				return
			}
			// Push-only channel: an inbound data frame is read and
			// discarded; the client cannot submit or mutate operations over
			// it.
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
		case <-sub.Revoked():
			endSubscription(conn, clientGone, wsClosePermissionRevoked, "permission revoked")
			return true
		default:
		}
		return false
	}

	// First frame: the current merged state. A document without any CRDT
	// operation waits silently; its first queued state becomes the first
	// frame via the loop below.
	if hasState {
		if checkEnd() {
			return
		}
		raw, marshalErr := encodeCRDTStateFrame(initial)
		if marshalErr != nil {
			_ = conn.Close(wsCloseInternalError, "internal error")
			return
		}
		if writeErr := conn.WriteText(raw); writeErr != nil {
			return
		}
	}

	for {
		// Park until a state-changing commit queues a value, a sticky revoke
		// fires, or the termination signal arrives. r.Context() covers a
		// non-shutdown cancel; the read pump covers a client disconnect.
		select {
		case <-sub.Wakes():
		case <-sub.Revoked():
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

		// Flush every queued merged value in commit order. A termination
		// signal or revoke is honored between frames, so close delivery is
		// never held off behind a long queue.
		for _, state := range sub.Drain() {
			if checkEnd() {
				return
			}
			raw, marshalErr := encodeCRDTStateFrame(state)
			if marshalErr != nil {
				_ = conn.Close(wsCloseInternalError, "internal error")
				return
			}
			if writeErr := conn.WriteText(raw); writeErr != nil {
				return
			}
		}
	}
}
