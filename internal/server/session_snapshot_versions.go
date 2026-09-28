package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionVersionRegisterRequest is the body of the session-scoped collection
// POST: the stable version name to create and the existing snapshot cursor it
// is bound to. The calling device is the session's owning device resolved from
// the path, so no deviceId travels in the body — a stray one is decoded and
// ignored like every other unknown field and can never override the owner.
type sessionVersionRegisterRequest struct {
	Name           string          `json:"name"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// sessionVersionBindRequest is the body of the session-scoped rename (PUT
// .../snapshots/versions/{name}): only the existing snapshot cursor the name is
// moved onto. The name is in the path and the device is the session owner, so
// neither travels here; a stray deviceId is decoded and ignored like any other
// unknown field.
type sessionVersionBindRequest struct {
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// sessionVersionRestoreRequest is the body of the session-scoped by-name
// restore: only the change id the restored state is appended under. The
// calling device is the session owner and the source snapshot is named by the
// path, so neither travels here.
type sessionVersionRestoreRequest struct {
	ChangeID string `json:"changeId"`
}

// sessionVersionDevice resolves the calling device of a session-scoped
// version endpoint from the session path segment. Session existence is the
// session view's first resource verdict and is taken only after the request
// shape is settled: a session that was never created or was deleted is a 404
// JSON error ahead of any document, version or snapshot observation. It
// returns the owning device id for the gated call that follows.
func sessionVersionDevice(w http.ResponseWriter, s *app.App, sessionID string) (string, bool) {
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return "", false
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return "", false
	}
	return deviceID, true
}

// writeSessionVersionGateError maps the shared gate/existence errors of every
// session-scoped version endpoint onto their fixed status codes, returning
// true when err was handled. The order mirrors the fixed verdict order:
// session/device existence (404), document permission (403), version and
// snapshot existence (404), conflict (409). The gate is taken inside the
// delegated transaction, so an unregistered device means the session's owner
// was deregistered (its cascade removed the session too) and is reported as
// the missing session the caller used; none of the failure bodies exposes
// version content.
func writeSessionVersionGateError(w http.ResponseWriter, err error) bool {
	var conflict *events.ErrVersionConflict
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(w, http.StatusNotFound, "session not found")
	case errors.Is(err, store.ErrPermissionDenied):
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
	case errors.Is(err, events.ErrVersionNotFound):
		writeError(w, http.StatusNotFound, "snapshot version not found")
	case errors.Is(err, events.ErrSnapshotNotFound):
		writeError(w, http.StatusNotFound, "snapshot not found")
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":        "snapshot version is already bound to another cursor",
			"conflictName": conflict.Name,
		})
	default:
		return false
	}
	return true
}

// handleSessionPutSnapshotVersion is the session view's counterpart to the
// document-level registration, mounted under the session document's snapshot
// resource:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions
//
// The body names only the version and the existing snapshot cursor; the
// calling device is the session's owning device resolved from the path with
// the same identity rule as the session reads, exports, commits and restores
// — no new credential is introduced. Binding the same name to the same cursor
// again is idempotent with an identical body; binding that name to another
// cursor is a 409 with zero writes. The fixed verdict order is request shape
// (400), session existence (404), document permission (403), then snapshot
// and version existence (404) and the conflict verdict (409). Success is one
// compact JSON line naming the version and the snapshot cursor.
func handleSessionPutSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionVersionRegisterRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name must be a non-empty string")
		return
	}
	// snapshotCursor must be present and a positive integer (no fractions,
	// strings, booleans, null or zero).
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}
	cursor, ok := parsePositiveInt(req.SnapshotCursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	result, err := s.PutSnapshotVersion(documentID, deviceID, req.Name, cursor)
	if err != nil {
		if !writeSessionVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to register snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleSessionListSnapshotVersions is the session-scoped read-only
// collection view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions
//
// The request carries no body or caller parameter — the device is the
// session's owning device. It returns every registration in ascending
// version-name order with the same versions/count body the document-level
// list emits; an unknown document or a document without markers reads as an
// empty array with count 0 rather than an error. The session must exist (404)
// and its device must still be authorized for the document (403); the read
// writes nothing and exposes no snapshot state.
func handleSessionListSnapshotVersions(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	versions, err := s.ListSnapshotVersions(documentID, deviceID)
	if err != nil {
		if !writeSessionVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to list snapshot versions")
		}
		return
	}

	items := make([]snapshotVersionItem, 0, len(versions))
	for _, v := range versions {
		items = append(items, snapshotVersionItem{Name: v.Name, Cursor: v.Cursor})
	}
	writeJSON(w, http.StatusOK, snapshotVersionListResponse{Versions: items, Count: len(items)})
}

// handleSessionGetSnapshotVersion reads, from the session view, the snapshot
// state a version name points at:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The body is byte-for-byte the by-cursor snapshot read
// ({"cursor":N,"state":...}): the state is presented exactly as stored. The
// verdict order is shape (400), session existence (404), permission (403),
// version and snapshot existence (404).
func handleSessionGetSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	cursor, state, err := s.GetSnapshotVersionState(documentID, deviceID, name)
	if err != nil {
		if !writeSessionVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to load snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "state": state})
}

// handleSessionRebindSnapshotVersion moves an existing version name onto
// another existing snapshot from the session view:
//
//	PUT /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The body names only the target snapshot cursor; the device is the session's
// owning device. The verdict order is shape (400), session existence (404),
// permission (403), the version marker (a missing name is 404) and the target
// snapshot (a missing cursor is 404). Rebinding to the cursor already bound is
// an idempotent no-op with the same body; the snapshot itself is never
// modified.
func handleSessionRebindSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	var req sessionVersionBindRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// snapshotCursor must be present and a positive integer (no fractions,
	// strings, booleans, null or zero).
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}
	cursor, good := parsePositiveInt(req.SnapshotCursor)
	if !good {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	result, err := s.RebindSnapshotVersion(documentID, deviceID, name, cursor)
	if err != nil {
		if !writeSessionVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to rename snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleSessionDeleteSnapshotVersion hard-deletes one version marker from the
// session view:
//
//	DELETE /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The request carries no body and names no caller — the device is the
// session's owning device. A missing marker is a 404 JSON error; the snapshot
// itself is untouched and, once the deletion commits, the name is free to bind
// again. Success is one compact JSON line naming the version and the deletion
// marker.
func handleSessionDeleteSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	if err := s.DeleteSnapshotVersion(documentID, deviceID, name); err != nil {
		if !writeSessionVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to delete snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "deleted": true})
}

// handleSessionRestoreSnapshotVersion is the session-scoped by-name restore,
// the session view's counterpart to the document-level by-name restore and a
// sibling of the session cursor restore:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}/restore
//
// The body names only the change id; the device is the session's owning device
// and the source snapshot is resolved from the path. On a hit the named
// snapshot's state is appended exactly like the cursor-based restore in one
// serialized transaction (the document cursor advances by one) and the
// success body is byte-identical to it. Idempotency and conflict follow the
// existing restore rules: an id occupied by an ordinary change, or a source
// mismatch, is a 409 with zero writes. The verdict order is shape (400),
// session existence (404), permission (403), version and snapshot existence
// (404), then the restore conflict (409).
func handleSessionRestoreSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	var req sessionVersionRestoreRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.ChangeID == "" {
		writeError(w, http.StatusBadRequest, "changeId must be a non-empty string")
		return
	}

	deviceID, ok := sessionVersionDevice(w, s, sessionID)
	if !ok {
		return
	}

	result, err := s.RestoreSnapshotVersion(documentID, deviceID, req.ChangeID, name)
	if err != nil {
		var conflict *events.ErrRestoreConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// restore; its cascade removed the session too.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrVersionNotFound):
			writeError(w, http.StatusNotFound, "snapshot version not found")
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":      "change id already exists with a different deviceId, snapshotCursor or source state",
				"conflictId": conflict.ID,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to restore snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}
