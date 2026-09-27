package server

import (
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// handleDocumentSubscribe is the document-level WebSocket change subscription,
// the session view's counterpart that needs no session:
//
//	GET /v1/documents/{documentID}/changes/subscribe?deviceId=D&cursor=N
//
// The calling device is declared by the deviceId query parameter; no new
// authentication is introduced. Validation is fixed and entirely pre-upgrade,
// writing nothing:
//
//  1. cursor must be a non-negative decimal integer and the request must carry
//     a valid WebSocket upgrade handshake (400 JSON otherwise; a malformed path
//     is a 400 JSON too),
//  2. the declared device must currently be registered — a missing or empty
//     deviceId is treated as an unregistered caller (404 JSON),
//  3. it must currently hold permission for the document (403 JSON).
//
// Everything after the upgrade is byte-for-byte the session subscription:
// committed changes past the starting cursor are paged out in cursor order as
// one {"id","deviceId","payload","cursor"} text frame per row, then live
// commits continue seamlessly. The connection is push-only, ends with 4403 on a
// later sticky revoke and with 1001 on a termination signal, and leaves no
// durable trace.
func handleDocumentSubscribe(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	// Handshake and parameter shape first: a bad cursor is a 400 even when the
	// device is also unknown, matching the fixed validation order.
	cursor, ok := parseSubscribeCursor(w, r)
	if !ok {
		return
	}
	if !checkWebSocketUpgrade(r) {
		writeError(w, http.StatusBadRequest, "a WebSocket upgrade handshake is required")
		return
	}

	// Device existence. A missing or empty deviceId is indistinguishable from
	// an unregistered one: 404 JSON, before any document content is observed.
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

	// The post-upgrade connection shares the session subscription's exact
	// pump: catch-up paging, live fan-out, push-only inbound frames, 4403
	// revoke and 1001 shutdown.
	serveSubscription(r, s, conn, documentID, deviceID, cursor)
}
