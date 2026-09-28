package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionChangeLookupRequest is the body of POST
// .../sessions/{sessionId}/documents/{documentId}/changes/query: only the
// non-empty, in-batch-unique set of change ids travels in the request. The
// calling device is the session's owning device, resolved from the path with
// the same identity rule as the session reads, exports, commits, replays,
// merges, compactions, polls and subscriptions — no new credential is
// introduced. A stray deviceId is decoded and ignored like every other unknown
// field, so it can never override the session owner.
type sessionChangeLookupRequest struct {
	IDs []string `json:"ids"`
}

// handleSessionQueryChanges is the session view's counterpart to the
// document-level read-only batch lookup:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/changes/query
//
// The strict application/json body names a non-empty array of non-empty,
// in-batch-unique change ids; answers arrive in the exact order the ids were
// given and use the same changeLookupItem/changeLookupResponse shape the
// document-level lookup emits. Each id is answered independently:
//
//   - "found": the change is still in the online log, so the answer carries
//     its originating device, the payload verbatim as it was saved at commit
//     time and its first cursor;
//   - "compacted": compaction trimmed the row, so the answer reports only the
//     first cursor held in its retained summary — no payload is restored and
//     the summary never participates in an ordinary read;
//   - "missing": the id has never belonged to the document, so the answer
//     carries no content. A missing id is a normal answer, not an error.
//
// Checks run in the fixed order request shape (400) first — content type,
// JSON validity, trailing content, a missing or empty ids array, an empty or
// mistyped element and an in-batch duplicate — then session existence (404)
// and document permission (403). A 404 or 403 exposes no change content. The
// query creates no change, allocates no cursor and wakes neither a long poll
// nor a push subscriber. Success is one compact JSON line ending in a newline
// with results then count.
func handleSessionQueryChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionChangeLookupRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// A missing (nil) or empty ids array is the same request-shape error; a
	// non-string element was already rejected while decoding the JSON.
	if len(req.IDs) == 0 {
		writeError(w, http.StatusBadRequest, "ids must be a non-empty array of non-empty strings")
		return
	}
	seen := make(map[string]struct{}, len(req.IDs))
	for _, id := range req.IDs {
		if id == "" {
			writeError(w, http.StatusBadRequest, "each id must be a non-empty string")
			return
		}
		if _, dup := seen[id]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id within batch: "+id)
			return
		}
		seen[id] = struct{}{}
	}

	// Request shape is settled; only now does the session lookup run, so a
	// malformed query against an unknown session is still a 400.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	// The lookup is the same gated read the document-level batch query uses:
	// the registration/permission verdict is retaken inside its serialized
	// transaction, so a session whose device was deregistered between the two
	// lookups answers 404 and a revoked device answers 403 before any change
	// content is observed.
	lookups, err := s.GetChangesByIDs(documentID, deviceID, req.IDs)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// query; its cascade removed the session too, so the identity the
			// caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeError(w, http.StatusInternalServerError, "failed to look up changes")
		}
		return
	}

	items := make([]changeLookupItem, len(lookups))
	for i, l := range lookups {
		items[i] = changeLookupItem{
			ID:       l.ID,
			Status:   l.Status,
			DeviceID: l.DeviceID,
			Payload:  l.Payload,
			Cursor:   l.Cursor,
		}
	}
	writeJSON(w, http.StatusOK, changeLookupResponse{Results: items, Count: len(items)})
}

// malformedSessionQueryPath reports whether p targets the session-scoped
// change-log batch lookup but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/query): a missing
// "documents"/"changes" segment, an extra segment, a trailing slash (an empty
// trailing segment), or a "query" segment in a position short of the
// registered shape. ServeMux would answer those with a redirect or a
// plain-text 404/405; every failure of this endpoint must be a JSON 400
// instead and never a redirect. Empty segments are already rejected by the
// guard itself.
//
// "query" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "query" occupies an identifier position (index 0 or 2) and
// keeps its ordinary routes.
func malformedSessionQueryPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "query" there is an ordinary identifier, not the
	// changes-query keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict; a
	// version name coinciding with "query" is an ordinary identifier there.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "query" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "query" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly "query"
		// with the documents/changes scaffolding around it, and no segment may
		// follow. Any other occurrence is a malformed path.
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
