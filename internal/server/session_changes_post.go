package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionPostChangesRequest is the body of the session-scoped batch commit:
// it carries only the change array. The originating device is never taken
// from the body; it is the device that owns the session named in the path.
type sessionPostChangesRequest struct {
	Changes []changeIn `json:"changes"`
}

// handleSessionPostChanges is the session-scoped batch commit mounted on the
// same change collection path the session view reads:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/changes
//
// The request body is just {"changes":[{"id","payload"}, ...]}; unlike the
// document-level POST it carries no deviceId. The caller identity is the
// existing session named by the path, resolved with exactly the same verdict
// the session read, export and subscription use — the session must exist and
// its owning device must currently hold permission for the document.
//
// The checks run in a fixed order: request shape (400) — content type, a
// single JSON value with no trailing content, a non-empty changes array whose
// elements each carry a non-empty string id and a JSON payload, and no
// duplicated in-batch id — then session existence (404), then the session
// device's document permission (403). The commit itself goes through the same
// gated, serialized transaction as an offline replay, so an existing id is
// idempotent only when the session device and the decoded payload match and a
// mismatch is a 409 naming the conflicting id; the whole batch either commits
// or leaves nothing. Success is byte-for-byte the document-level commit
// result: {"results":[{"id","created","cursor"}, ...]} in request order,
// cursors allocated from the document's one contiguous cursor space. A
// change-bearing commit wakes parked long polls and pushes to subscribers
// exactly like every other write path.
func handleSessionPostChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionPostChangesRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.Changes == nil {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}
	if len(req.Changes) == 0 {
		writeError(w, http.StatusBadRequest, "changes must be a non-empty array")
		return
	}

	changes := make([]events.Change, len(req.Changes))
	seen := make(map[string]struct{}, len(req.Changes))
	for i, c := range req.Changes {
		if c.ID == "" {
			writeError(w, http.StatusBadRequest, "each change must have a non-empty string id")
			return
		}
		if len(c.Payload) == 0 {
			writeError(w, http.StatusBadRequest, "each change must carry a JSON payload")
			return
		}
		if _, dup := seen[c.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate change id within batch: "+c.ID)
			return
		}
		seen[c.ID] = struct{}{}
		// DeviceID is filled in below from the session owner; the body never
		// supplies it.
		changes[i] = events.Change{ID: c.ID, Payload: c.Payload}
	}

	// Session existence precedes the permission/registration gate, matching
	// the session read's and export's fixed check order.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}
	for i := range changes {
		changes[i].DeviceID = deviceID
	}

	// ReplayChanges runs the registration/permission gate and the idempotency
	// resolution inside one serialized transaction that shares the document's
	// cursor space with every other write path, then notifies waiters and
	// subscribers after commit.
	results, err := s.ReplayChanges(documentID, changes)
	if err != nil {
		var conflict *events.ErrConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":      "change id already exists with different deviceId or payload",
				"conflictId": conflict.ID,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to commit changes")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}
