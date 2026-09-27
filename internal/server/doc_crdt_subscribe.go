package server

import (
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// handleDocumentCRDTStateSubscribe is the document-level WebSocket subscription
// to a document's merged CRDT state, the session view's counterpart that needs
// no session:
//
//	GET /v1/documents/{documentID}/crdt/state/subscribe?deviceId=D
//
// The calling device is declared by the deviceId query parameter; no new
// authentication is introduced. The validation order is fixed and entirely
// pre-upgrade, writing nothing:
//
//  1. the request must be a GET carrying a valid WebSocket upgrade handshake
//     (400 JSON otherwise; a malformed path is a 400 JSON too),
//  2. the declared device must currently be registered — a missing or empty
//     deviceId is treated as an unregistered caller (404 JSON),
//  3. it must currently hold permission for the document (403 JSON).
//
// Everything after the upgrade is byte-for-byte the session CRDT
// subscription: the current merged state is pushed first (a document with no
// CRDT operation stays silent until its first state), later commits push one
// text frame only when the merge really changes, inbound data frames are read
// and discarded, a later sticky revoke ends the connection with 4403 and a
// termination signal ends it with 1001. The subscription is not persisted.
func handleDocumentCRDTStateSubscribe(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	if !checkWebSocketUpgrade(r) {
		writeError(w, http.StatusBadRequest, "a WebSocket upgrade handshake is required")
		return
	}

	// Device existence. A missing or empty deviceId is indistinguishable from
	// an unregistered one: 404 JSON, before any CRDT content is observed.
	deviceID := r.URL.Query().Get("deviceId")
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}
	exists, err := s.DeviceExists(deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up device")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "device not found")
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

	// The post-upgrade connection shares the session CRDT subscription's exact
	// pump: initial state, commit-order fan-out, push-only inbound frames, 4403
	// revoke and 1001 shutdown.
	serveCRDTStateSubscription(r, s, conn, documentID, deviceID)
}
