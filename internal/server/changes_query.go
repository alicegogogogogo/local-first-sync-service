package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// queryChangeIn is one element of the batch query request: only the change id
// travels in the body, so an element missing id or carrying a non-string one is
// a request-shape error exactly like the commit elements.
type queryChangeIn struct {
	ID string `json:"id"`
}

// changesQueryRequest is the batch-by-id query body. The calling device is
// declared in the body with the same deviceId judgment the document-level
// commit uses; changes is the non-empty, in-batch-unique set of ids to answer.
type changesQueryRequest struct {
	DeviceID string          `json:"deviceId"`
	Changes  []queryChangeIn `json:"changes"`
}

// changesQueryResponse fixes the on-the-wire key order of the batch body to
// results then count. Results is always a non-nil slice, one entry per
// requested id in request order.
type changesQueryResponse struct {
	Results []events.FetchedChange `json:"results"`
	Count   int                    `json:"count"`
}

// handleQueryChanges is the read-only batch lookup over a document's change
// collection:
//
//	POST /v1/documents/{documentID}/changes/query
//
// The body names the calling device and a non-empty, in-batch-unique ordered
// list of change ids; the answer gives one result per id in the given order.
// An online hit reports the originating device, the payload exactly as it was
// stored and the change's cursor. An id compaction trimmed reports only its
// first cursor and the trimmed marker — the retained summary never restores
// content or participates in reads. An id that never appeared reports the
// missing marker and no content. Missing and trimmed entries are ordinary
// results, not errors: count always equals the number of ids and of results.
//
// Request shape is validated before the device is looked up: a bad content
// type, malformed JSON, trailing content, a missing or empty changes array, a
// non-string element, an empty id, or an in-batch duplicate is a 400 and
// writes nothing. Only then does the gate run: an unregistered device is 404
// and a device whose document permission was revoked is 403, neither exposing
// any change content. The read creates no change, allocates no cursor and
// wakes neither a long poll nor a push subscriber.
func handleQueryChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req changesQueryRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	// A missing changes key decodes to a nil slice and an empty one to a
	// zero-length slice; both are a request-shape error ahead of the device
	// lookup. A non-array value fails while the strict decoder decodes it.
	if len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}

	ids := make([]string, len(req.Changes))
	seen := make(map[string]struct{}, len(req.Changes))
	for i, c := range req.Changes {
		if c.ID == "" {
			writeError(w, http.StatusBadRequest, "each change must have a non-empty string id")
			return
		}
		if _, dup := seen[c.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id within batch: "+c.ID)
			return
		}
		seen[c.ID] = struct{}{}
		ids[i] = c.ID
	}

	// Shape is settled; the registration/permission verdict and the reads run
	// in one serialized transaction, so a rejected request observes no change
	// content and a concurrent batch is seen either whole or not at all.
	results, err := s.FetchChanges(documentID, req.DeviceID, ids)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeError(w, http.StatusInternalServerError, "failed to query changes")
		}
		return
	}

	writeJSON(w, http.StatusOK, changesQueryResponse{Results: results, Count: len(results)})
}
