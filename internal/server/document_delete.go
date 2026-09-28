package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleDeleteDocument is the document-level data-cleanup entry:
//
//	DELETE /v1/documents/{documentID}?deviceId=D
//
// It removes one document's whole persisted data in a single serialized
// transaction. The calling device is declared by the deviceId query
// parameter — no new authentication is introduced and the call carries no
// body. The fixed verdict order is request shape (400, handled by routing and
// the empty-id guard) -> device existence (404) -> document permission (403)
// -> document existence (404); every failure writes nothing and cleans
// nothing. Success answers one compact JSON line naming the document id and
// the deletion marker and nothing else.
func handleDeleteDocument(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	deviceID := r.URL.Query().Get("deviceId")
	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller; the gate also returns ErrDeviceNotFound for it, but judging here
	// keeps the 404 ahead of any document-content observation.
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	if err := s.DeleteDocument(documentID, deviceID); err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrDocumentNotFound):
			writeError(w, http.StatusNotFound, "document not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to delete document")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"documentId": documentID, "deleted": true})
}

// malformedDocumentDelete reports whether a DELETE targets the document item
// resource /v1/documents/{documentID} but its path is not that exact shape:
// the document id segment is missing (the bare /v1/documents) or empty (a
// trailing slash), or one or more segments follow a non-empty id that are not
// a known document sub-resource with its own routes. Such a request is a
// malformed 400 JSON rather than ServeMux's redirect or plain-text 404; the
// delete surface never redirects or emits HTML.
//
// It only considers DELETE: wrong methods on the exact item path are rejected
// by the item route's method-less pattern, and non-DELETE requests keep their
// pre-existing routing. Known sub-resources (changes, snapshots, crdt,
// permissions, replay, merge, restore) own their shape checks and are left
// alone here; doubled-slash empty segments are already rejected by the guard
// itself before this runs.
func malformedDocumentDelete(method, p string) bool {
	if method != http.MethodDelete {
		return false
	}
	if p == "/v1/documents" {
		// Missing id segment entirely.
		return true
	}
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	if len(segs) == 1 {
		// Exactly one segment past the prefix: a trailing slash makes it the
		// empty id; a non-empty id is the well-formed item and is routed.
		return segs[0] == ""
	}
	// Two or more segments: known sub-resources keep their own endpoints and
	// malformed-shape guards; anything else past a non-empty id is a malformed
	// delete of the document item (extra segments).
	if segs[0] == "" {
		return false // doubled slash, already rejected upstream
	}
	switch segs[1] {
	case "changes", "snapshots", "crdt", "permissions", "replay", "merge", "restore":
		return false
	default:
		return true
	}
}
