package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleDeleteDocument is the document-level data-clearing entry:
//
//	DELETE /v1/documents/{documentID}?deviceId=D
//
// The calling device is declared by the deviceId query parameter — the same
// identity rule the document-level subscriptions use; no new authentication is
// introduced and the call carries no body. Path shape (an empty id, a missing
// or extra segment, a non-DELETE verb) is rejected with a JSON 400 by the
// routes and the empty-id guard before this handler runs, so a shape failure
// always precedes the existence and permission verdicts below.
//
// The verdict order is fixed once the shape is settled: an unregistered
// caller (a missing or empty deviceId included) is a 404; a caller whose
// permission for the document was revoked is a 403; a document that never
// existed or was already deleted is a 404. None of those failures writes
// anything. A successful call returns one compact JSON line naming the
// document id and the deletion marker:
//
//	{"documentId":"doc-1","deleted":true}
func handleDeleteDocument(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller, exactly as on the document-level subscription handshakes.
	deviceID := r.URL.Query().Get("deviceId")
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	if err := s.DeleteDocument(deviceID, documentID); err != nil {
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

	// A struct fixes the field order: the document identifier then the deletion
	// marker, on one compact line with a trailing newline.
	writeJSON(w, http.StatusOK, struct {
		DocumentID string `json:"documentId"`
		Deleted    bool   `json:"deleted"`
	}{DocumentID: documentID, Deleted: true})
}
