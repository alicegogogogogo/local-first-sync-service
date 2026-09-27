package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleSessionCompactChanges is the session-scoped change-log compaction,
// mounted one segment below the session document's change collection:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/changes/compact
//
// It has exactly the trimming semantics of the document-level compaction —
// the boundary is the greatest cursor with a saved snapshot (zero when the
// document has none), changes at or below it leave the online log and each
// trimmed id keeps only its source/payload summary — but the calling device is
// the session's owning device resolved from the path with the same identity
// rule as the session reads, exports, commits, replays, merges, restores,
// polls and subscriptions. No new credential is introduced, and the request
// body only has to be one legal JSON value: a stray deviceId or any other
// field is decoded and ignored like every other unknown field, never
// overriding the session device.
//
// On success the answer is the same one compact line the document-level
// compaction emits, {"boundary":N,"removed":M} with a trailing newline.
// Compaction is idempotent — a repeat reports the same boundary and zero
// removed — allocates no cursor, writes no change and wakes no waiter or
// subscriber. The checks run in the fixed order request shape (400), session
// existence (404), document permission (403) — the permission verdict is taken
// inside the compaction transaction, so a revoked device writes nothing — and
// every rejection leaves the log untouched.
func handleSessionCompactChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// The body carries no interpreted fields at all: one legal JSON value is
	// the whole contract, so it is captured verbatim and discarded. Invalid
	// JSON, trailing content and a wrong content type are rejected by the
	// shared strict decoder before any resource is looked up.
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

// malformedSessionCompactPath reports whether p targets the session-scoped
// change-log compaction endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/compact): a missing
// "documents"/"changes" segment, an extra segment, or a "compact" segment in a
// position short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "compact" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "compact" occupies an identifier position (index 0 or 2) and
// is therefore left to the ordinary changes routes like any other id.
func malformedSessionCompactPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "compact" there is an ordinary identifier, not the
	// changes-compaction keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	for i, seg := range segs {
		if seg != "compact" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "compact" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly
		// "compact" with the documents/changes scaffolding around it, and no
		// segment may follow. Any other occurrence is a malformed path.
		if i == 4 && len(segs) == 5 &&
			segs[0] != "" &&
			segs[1] == "documents" &&
			segs[2] != "" &&
			segs[3] == "changes" {
			return false
		}
		return true
	}
	return false
}
