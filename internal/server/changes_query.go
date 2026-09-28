package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// changeLookupRequest is the body of POST .../changes/query: the calling
// device and the non-empty, in-batch-unique set of change ids to answer. The
// device identity uses the same declaration the document-level change commit
// uses — no new authentication is introduced.
type changeLookupRequest struct {
	DeviceID string   `json:"deviceId"`
	IDs      []string `json:"ids"`
}

// changeLookupItem fixes the on-the-wire key order of one answer to id,
// status, then (for a found row) deviceId, payload and cursor. The
// device/payload/cursor fields are omitted entirely on missing and compacted
// answers: a compacted id reports only its first cursor, so it carries
// cursor alone alongside id and status, and a missing id carries nothing but
// its id and status.
type changeLookupItem struct {
	ID       string          `json:"id"`
	Status   string          `json:"status"`
	DeviceID string          `json:"deviceId,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Cursor   int64           `json:"cursor,omitempty"`
}

// changeLookupResponse is the query body with the top-level key order fixed
// to results then count; count always equals the number of result entries,
// which equals the number of ids asked for.
type changeLookupResponse struct {
	Results []changeLookupItem `json:"results"`
	Count   int                `json:"count"`
}

// handleQueryChanges is the read-only batch lookup over a document's change
// collection:
//
//	POST /v1/documents/{documentID}/changes/query
//
// The strict application/json body names the calling device and a non-empty
// array of non-empty, in-batch-unique change ids; answers arrive in the exact
// order the ids were given. Each id is answered independently:
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
// The verdict order is request shape (400) first — content type, JSON
// validity, trailing content, a missing or empty ids array, an empty or
// mistyped element and an in-batch duplicate — then device existence (404)
// and document permission (403). A 404 or 403 exposes no change content. The
// query creates no change, allocates no cursor and wakes neither a long poll
// nor a push subscriber. Success is one compact JSON line ending in a
// newline with results then count.
func handleQueryChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req changeLookupRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// Shape checks all precede the device existence verdict, so a malformed
	// query against an unregistered device is still a 400.
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
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

	lookups, err := s.GetChangesByIDs(documentID, req.DeviceID, req.IDs)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
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

// malformedChangeQueryPath reports whether p targets the change-log batch
// lookup but is not at its exact location
// (/v1/documents/{documentID}/changes/query): a missing "changes" segment, an
// extra segment, a trailing slash (an empty trailing segment), or a "query"
// segment in a position short of the registered shape. ServeMux would answer
// those with a redirect or a plain-text 404/405; every failure of this
// endpoint must be a JSON 400 instead and never a redirect. Empty segments
// are already rejected by the guard itself.
//
// "query" is treated as the endpoint keyword only in its terminal segment
// position (the third segment, index 2); a document identifier literally
// named "query" occupies an identifier position (index 0) and keeps its
// ordinary routes.
func malformedChangeQueryPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guards; "query" there is an ordinary identifier.
	if len(segs) >= 2 && segs[1] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its own shape verdict.
	if len(segs) >= 3 && segs[0] != "" && segs[1] == "snapshots" && segs[2] == "versions" {
		return false
	}
	for i, seg := range segs {
		if seg != "query" {
			continue
		}
		// Identifier position: a document named "query" is an ordinary id.
		if i == 0 {
			continue
		}
		// Endpoint keyword position: the third segment must be exactly
		// "query" with a non-empty documentID and "changes" around it, and no
		// segment may follow. Any other occurrence is a malformed path.
		if i == 2 && len(segs) == 3 &&
			segs[0] != "" &&
			segs[1] == "changes" {
			return false
		}
		return true
	}
	return false
}
