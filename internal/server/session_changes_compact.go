package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleSessionCompactChanges is the session-scoped change-log compaction
// entry, mounted one segment below the session document's change collection:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/changes/compact
//
// It is the session view's counterpart to
// POST /v1/documents/{documentID}/changes/compact and shares its boundary,
// trimming, idempotency and persistence semantics byte-for-byte; only the
// caller identity differs. The calling device is the session's owning device,
// resolved from the path with the same identity rule as the session reads,
// exports, commits, replays, merges, polls and subscriptions — no new
// credential is introduced. The body carries no device field: a stray
// deviceId is decoded and ignored like every other unknown field, so it can
// never override the session owner. The body only has to be one legal JSON
// value — an object, array, scalar or null all compact identically; its
// content never affects the boundary or the removed count.
//
// Checks run in the fixed order request shape (400), session existence (404),
// document permission (403). The permission verdict is taken inside the
// compaction transaction, so a revoked device writes nothing and a rejected
// request observes no change content. On success the answer is 200 with one
// compact JSON line — boundary, then removed — terminated by a newline, the
// same compactChangesResponse the document-level entry emits. Compaction is
// idempotent, allocates no cursor, writes no change and wakes no waiter or
// subscriber.
func handleSessionCompactChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// The body carries no meaningful field: it only has to be exactly one
	// legal JSON value. Decoding into a RawMessage accepts an object, array,
	// scalar or null, while a missing/empty body, malformed JSON, trailing
	// content or a wrong Content-Type still fail the strict shared decoder.
	var body json.RawMessage
	if !decodeJSONBody(w, r, &body) {
		return
	}

	// Request shape is settled; only now does the session lookup run, so a
	// malformed body against an unknown session is still a 400.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	boundary, removed, err := s.CompactSessionChanges(documentID, deviceID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// compaction; its cascade removed the session too, so the identity
			// the caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeError(w, http.StatusInternalServerError, "failed to compact changes")
		}
		return
	}

	writeJSON(w, http.StatusOK, compactChangesResponse{Boundary: boundary, Removed: removed})
}
