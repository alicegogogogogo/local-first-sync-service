package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// changesCompactRequest is the body of POST .../changes/compact.
type changesCompactRequest struct {
	DeviceID string `json:"deviceId"`
}

// changesCompactResponse is the compaction report. The struct field order
// fixes the on-the-wire key order to boundary then removed, and the JSON
// encoder terminates the single compact line with exactly one newline.
type changesCompactResponse struct {
	Boundary int64 `json:"boundary"`
	Removed  int64 `json:"removed"`
}

// handleCompactChanges moves the document's changes at or below the
// compaction boundary — the greatest saved snapshot cursor, 0 when the
// document has no snapshot — out of the online change log.
//
// The request contract matches the CRDT compaction endpoint: strict
// application/json, exactly one JSON value, no trailing content and a
// non-empty deviceId — any violation is a 400 JSON error with zero writes.
// The checks then run in the submission's order: an unregistered device is a
// 404 and a device whose permission for the document was revoked is a 403;
// neither writes anything. A document with no snapshot is not an error: it
// compacts to boundary 0 and removes nothing.
//
// On success the answer is 200 with one compact JSON line naming the boundary
// and the number of removed rows. Repeating the compaction is not an error:
// it reports the same boundary and zero removed. Compaction writes no change
// record, allocates no cursor and wakes no waiter or subscriber; the trimmed
// ids keep only their idempotency/conflict identities, so the log does not
// grow back.
func handleCompactChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req changesCompactRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}

	result, err := s.CompactChanges(documentID, req.DeviceID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeError(w, http.StatusInternalServerError, "failed to compact changes")
		}
		return
	}

	writeJSON(w, http.StatusOK, changesCompactResponse{Boundary: result.Boundary, Removed: result.Removed})
}

// malformedChangesCompactPath reports whether p targets the changes
// compaction endpoint but is not at its exact location
// (/v1/documents/{documentID}/changes/compact): a trailing slash, extra
// segments, or a "compact" segment in any position short of the registered
// shape. ServeMux would answer those with a redirect or a plain-text
// 404/405; the endpoint promises a JSON 400 and never a redirect. The keyword
// is matched only past the documentID position, so a document literally named
// "compact" keeps its ordinary routes.
func malformedChangesCompactPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "compact" || i == 0 {
			continue
		}
		// The CRDT namespace has its own guard; "compact" there is the CRDT
		// compaction keyword, not the change-log one.
		if i == 2 && segs[1] == "crdt" {
			continue
		}
		return !(len(segs) == 3 && segs[0] != "" && segs[1] == "changes")
	}
	return false
}
