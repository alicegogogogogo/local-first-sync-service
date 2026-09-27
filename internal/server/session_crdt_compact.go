package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleSessionCRDTCompact is the session-scoped CRDT compaction entry,
// mounted one segment below the session document's CRDT state path:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/crdt/compact
//
// It is the session view's counterpart to
// POST /v1/documents/{documentID}/crdt/compact and shares its per-type
// trimming rule, retained identities, merged result and persistence semantics
// byte-for-byte; only the caller identity differs. The calling device is the
// session's owning device, resolved from the path with the same identity rule
// as the session CRDT submission, state read and subscription — no new
// credential is introduced. The body carries no meaningful field: a stray
// deviceId is decoded and ignored like every other unknown field, so it can
// never override the session owner. The body only has to be one legal JSON
// value — an object, array, scalar or null all compact identically; its
// content never affects the trim.
//
// Checks run in the fixed order request shape (400), session existence (404),
// document permission (403, taken inside the compaction transaction) and CRDT
// state existence (404 for a document with no committed CRDT operation). On
// success the answer is 200 with the post-compaction snapshot, rendered
// byte-for-byte like a document-level CRDT snapshot read immediately
// afterward (writeCRDTSnapshot). Compaction is idempotent — a repeat trims
// nothing and returns the same body — and touches only the CRDT state layer:
// it allocates no change cursor, writes no change record and, because the
// merged value never moves, wakes no waiter and pushes no notification.
func handleSessionCRDTCompact(s *app.App, w http.ResponseWriter, r *http.Request) {
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

	snapshot, err := s.CompactSessionCRDT(documentID, deviceID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// compaction; its cascade removed the session too, so the identity
			// the caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, crdt.ErrNotFound):
			writeError(w, http.StatusNotFound, "crdt state not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to compact crdt state")
		}
		return
	}

	writeCRDTSnapshot(w, snapshot)
}
