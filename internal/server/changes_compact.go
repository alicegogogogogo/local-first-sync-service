package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// compactChangesRequest is the body of POST .../changes/compact: the same
// device field the CRDT compaction endpoint carries.
type compactChangesRequest struct {
	DeviceID string `json:"deviceId"`
}

// compactChangesResponse is the success body of POST .../changes/compact. The
// struct field order fixes the key order (boundary, then removed), and the
// shared JSON writer emits it as one compact line with a trailing newline.
type compactChangesResponse struct {
	Boundary int64 `json:"boundary"`
	Removed  int64 `json:"removed"`
}

// handleCompactChanges moves the document's changes at or below the
// compaction boundary — the greatest cursor with a saved snapshot, zero when
// the document has none — out of the online change log.
//
// The request contract matches the CRDT compaction endpoint: strict
// application/json, exactly one JSON value, no trailing content and a
// non-empty deviceId — any violation is a 400 JSON error with zero writes.
// The gate then applies in the submission's order: an unregistered device is
// a 404 and a device whose permission for the document was revoked is a 403;
// neither writes anything. A snapshot-less or unknown document compacts
// successfully with boundary 0 and nothing removed.
//
// On success the answer is 200 with one compact JSON line naming the boundary
// and the number of changes this call removed. Compaction is idempotent — a
// repeat reports the same boundary and zero removed — and is not itself a
// change: it allocates no cursor, writes no change record and wakes no waiter
// or subscriber.
func handleCompactChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req compactChangesRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}

	boundary, removed, err := s.CompactChanges(documentID, req.DeviceID)
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

	writeJSON(w, http.StatusOK, compactChangesResponse{Boundary: boundary, Removed: removed})
}
