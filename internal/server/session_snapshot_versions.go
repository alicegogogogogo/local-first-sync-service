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
// POST: only the stable version name and the existing snapshot cursor travel
// in the request. The calling device is the session's owning device resolved
// from the path, so a stray deviceId is decoded and ignored like every other
// unknown field.
type sessionVersionRegisterRequest struct {
	Name           string          `json:"name"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// sessionVersionBindRequest is the body of the session-scoped rename (PUT
// .../snapshots/versions/{name}): only the existing snapshot cursor the name
// is moved onto. The name itself is in the path and the calling device is the
// session's owning device resolved from the path.
type sessionVersionBindRequest struct {
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// sessionVersionRestoreRequest is the body of the session-scoped by-name
// restore: only the change id the restored state is appended under. The source
// snapshot is named by the path and the calling device is the session's owning
// device resolved from the path.
type sessionVersionRestoreRequest struct {
	ChangeID string `json:"changeId"`
}

// sessionVersionDevice resolves the calling device of a session-scoped version
// endpoint from the session id in the path. A never-created or deleted session
// is a 404 JSON error ahead of any permission, version or snapshot
// observation, exactly like the other session endpoints.
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

// decodeSessionVersionBind enforces the rename endpoint's JSON shape: an
// application/json body carrying a positive-integer snapshotCursor. Every
// violation is a 400 JSON error and the caller returns without touching the
// store.
func decodeSessionVersionBind(w http.ResponseWriter, r *http.Request) (cursor int64, ok bool) {
	var req sessionVersionBindRequest
	if !decodeJSONBody(w, r, &req) {
		return 0, false
	}
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return 0, false
	}
	c, good := parsePositiveInt(req.SnapshotCursor)
	if !good {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return 0, false
	}
	return c, true
}

// writeSessionVersionGateError maps the shared gate/existence errors of a
// session-scoped version endpoint onto their fixed status codes, returning
// true when err was handled. It mirrors writeVersionGateError except that an
// in-transaction ErrDeviceNotFound answers "session not found" (404): the
// session lookup passed and the device was deregistered before the gated
// transaction ran, and that deregistration's cascade removed the session too,
// so the identity the caller used no longer exists. None of the failure bodies
// exposes version content.
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

// handleSessionPutSnapshotVersion is the session-scoped registration entry,
// the session view's counterpart to the document-level collection POST:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions
//
// The body carries only name and snapshotCursor; the calling device is the
// session's owning device resolved from the path with the same identity rule
// as the other session endpoints — no new credential is introduced and a stray
// deviceId in the body is ignored like any other unknown field. The binding,
// idempotency, conflict and success body semantics are byte-identical to the
// document-level registration: the same name bound to the same cursor answers
// the identical one-line body; rebinding it to another cursor is a 409 with
// zero writes. The checks run in the fixed order request shape (400), session
// existence (404), document permission (403) — taken inside the gated
// transaction — then snapshot and version existence (404) and the conflict
// verdict (409).
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
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}
	cursor, ok := parsePositiveInt(req.SnapshotCursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}

	// Request shape is settled; only now does the session lookup run, so a
	// malformed request against an unknown session is still a 400.
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

// handleSessionListSnapshotVersions is the session-scoped read-only collection
// view:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions
//
// It carries no body and no deviceId parameter; the calling device is the
// session's owning device. The body is byte-identical to the document-level
// list — every registration in ascending name order, an empty view as [] with
// count 0 — and the read writes nothing and exposes no snapshot state.
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

// handleSessionGetSnapshotVersion is the session-scoped by-name read:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The body is byte-for-byte the by-cursor snapshot read
// ({"cursor":N,"state":...}), state presented exactly as stored, and is
// identical to the document-level by-name read. The verdict order is shape
// (400), session existence (404), permission (403), version and snapshot
// existence (404).
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

// handleSessionRebindSnapshotVersion is the session-scoped rename:
//
//	PUT /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The body carries only snapshotCursor; the calling device is resolved from
// the session. Rebinding to the cursor already bound is an idempotent no-op
// with the same body; otherwise the named marker must exist (404) and the
// target snapshot must exist (404). The snapshot itself is never modified or
// copied.
func handleSessionRebindSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	cursor, ok := decodeSessionVersionBind(w, r)
	if !ok {
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

// handleSessionDeleteSnapshotVersion is the session-scoped hard delete of one
// version marker:
//
//	DELETE /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}
//
// The request carries no body and no deviceId parameter. A missing marker is a
// 404 JSON error; the snapshot itself is untouched and, once the deletion
// commits, the name is free to bind again.
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

// handleSessionRestoreSnapshotVersion is the session-scoped by-name restore:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}/restore
//
// The body carries only changeId; the source snapshot is resolved from the
// path and the calling device is the session's owning device. On a hit the
// named snapshot's state is appended exactly like the cursor-based restore in
// one serialized transaction (the document cursor advances by one) and the
// success body is byte-identical to it and to the document-level by-name
// restore. Idempotency and conflict follow the existing restore rules: an id
// occupied by an ordinary change, or a restore whose device, snapshot cursor
// or source state differs, is a 409 with zero writes. The verdict order is
// shape (400), session existence (404), permission (403), version and snapshot
// existence (404), then the restore conflict (409).
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
			// restore; its cascade removed the session too, so the identity the
			// caller used no longer exists.
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
