package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleCRDTStateSubscribe is the WebSocket subscription to a document's
// merged CRDT state, mounted under the session view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state/subscribe
//
// The subscription identity is entirely the existing session: the session's
// owning device is the only credential and no new auth is introduced. The
// validation order is fixed and entirely pre-upgrade, writing nothing:
//
//  1. the request must be a GET carrying a valid WebSocket upgrade handshake
//     (400 JSON otherwise; a malformed path is a 400 JSON too),
//  2. the session must currently exist (404 JSON),
//  3. its device must currently hold permission for the document (403 JSON).
//
// After the upgrade the server first pushes the current merged state; a
// document without any CRDT operation yet sends nothing and waits silently
// until its first state appears. From then on every commit that actually
// changes the merge — a counter's per-device maximum advancing, the set union
// growing — is pushed as one text frame in transaction-commit order.
// Idempotent repeats, rejected regressions and wholly invalid batches change
// no state and produce no frame; re-adding an existing set element pushes
// nothing either.
//
// Each message is one text frame whose payload is byte-for-byte the state
// read endpoint's body: compact single-line JSON with the keys in type, value
// order, terminated by a newline. The connection is push-only: inbound data
// frames are read and discarded, ping is answered with pong. A permission
// revoke after the upgrade ends the subscription with close code 4403 —
// stickily, a re-grant never revives this connection — and a termination
// signal ends every subscription with 1001. Subscriptions are not persisted;
// after a restart a fresh subscription immediately receives the consistent
// current merged state.
func handleCRDTStateSubscribe(s *store.Store, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

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
		// The 101 could not be written and the connection has been hijacked
		// (or is gone); no JSON error can be produced and nothing was
		// registered.
		return
	}

	serveCRDTStateSubscription(r, s, conn, documentID, deviceID)
}

// serveCRDTStateSubscription runs one upgraded CRDT-state connection to
// completion. Registration and the initial state read are one atomic step in
// the store, so a state-changing commit lands either inside that initial
// state or in the subscription's queue: the first frame is the current merge
// and every later frame is strictly newer, in commit order. The method
// returns when the client leaves, permission is revoked or the store starts
// closing; the hijacked connection is closed on exit.
func serveCRDTStateSubscription(r *http.Request, s *store.Store, conn *wsConn, documentID, deviceID string) {
	initial, sub, unregister, err := s.OpenCRDTSubscription(documentID, deviceID)
	if err != nil {
		_ = conn.Close(wsCloseInternalError, "internal error")
		_ = conn.NetClose()
		return
	}
	defer func() {
		unregister()
		_ = conn.NetClose()
	}()

	clientGone := make(chan struct{})
	go func() {
		defer close(clientGone)
		for {
			if _, _, readErr := conn.ReadMessage(); readErr != nil {
				// A malformed frame ends with the RFC protocol-error code; a
				// client close is already echoed inside ReadMessage; a network
				// drop needs no frame.
				var protoErr *wsProtocolErrorString
				if errors.As(readErr, &protoErr) {
					_ = conn.Close(wsCloseProtocolError, "protocol error")
				}
				return
			}
			// Push-only channel: an inbound data frame is read and discarded;
			// the client cannot submit or mutate any operation over it.
		}
	}()

	// endNow maps a closing store to 1001 and a sticky revoke to 4403, waiting
	// briefly for the peer's close echo. Termination takes precedence over a
	// racing revoke. It returns true when the subscription must end.
	endNow := func() bool {
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

	// closingOnly honors a termination signal before the first frame; a revoke
	// racing the initial push is left for the main loop so the state that was
	// authorized at handshake still reaches the client ahead of the 4403.
	closingOnly := func() bool {
		if s.Closing() {
			endSubscription(conn, clientGone, wsCloseGoingAway, "going away")
			return true
		}
		return false
	}

	// lastSent skips any queued frame that encodes the same state the
	// connection already sent. Registration-vs-commit atomicity already makes
	// this unnecessary (a queued state is always strictly newer), so this is
	// only a belt-and-braces guard for the register/read boundary.
	var lastSent []byte
	send := func(state store.CRDTState) bool {
		frame := encodeCRDTStateFrame(state)
		if bytes.Equal(frame, lastSent) {
			return true
		}
		if writeErr := conn.WriteText(frame); writeErr != nil {
			return false
		}
		lastSent = frame
		return true
	}

	// First push: the current merged state. A document with no operation yet
	// stays silent until its first state arrives. A termination signal that
	// raced the registration is honored before anything is sent.
	if closingOnly() {
		return
	}
	if initial != nil {
		if !send(*initial) {
			return
		}
	}

	for {
		if endNow() {
			return
		}

		// Drain every committed state queued since the last park, in commit
		// order; a revoke or termination is honored between frames.
		for _, state := range sub.Drain() {
			if endNow() {
				return
			}
			if !send(state) {
				return
			}
		}
		if endNow() {
			return
		}

		select {
		case <-sub.Wake():
			// A coalescing "drain now" signal; loop and flush the whole queue.
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
	}
}

// encodeCRDTStateFrame renders one state message exactly as the state read
// endpoint renders its body: the same encoder (HTML escaping included, map
// keys sorted to type then value) and the same trailing newline, so a pushed
// frame can be compared byte-for-byte with a GET .../crdt/state response.
func encodeCRDTStateFrame(state store.CRDTState) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	_ = enc.Encode(map[string]any{
		"type":  state.Type,
		"value": json.RawMessage(state.Value),
	})
	return buf.Bytes()
}
