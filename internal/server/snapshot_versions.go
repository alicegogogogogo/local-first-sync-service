package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// snapshotVersionRegisterRequest is the body of the collection POST: the
// calling device, the stable version name to create and the existing snapshot
// cursor it is bound to.
type snapshotVersionRegisterRequest struct {
	DeviceID       string          `json:"deviceId"`
	Name           string          `json:"name"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// snapshotVersionBindRequest is the body of the rename (PUT
// .../snapshots/versions/{name}): the calling device and the existing
// snapshot cursor the name is moved onto. The name itself is in the path.
type snapshotVersionBindRequest struct {
	DeviceID       string          `json:"deviceId"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// snapshotVersionRestoreRequest is the body of the by-name restore: the
// calling device and the change id the restored state is appended under. The
// source snapshot is named by the path rather than carried in the body.
type snapshotVersionRestoreRequest struct {
	DeviceID string `json:"deviceId"`
	ChangeID string `json:"changeId"`
}

// snapshotVersionItem fixes the on-the-wire key order of one list entry to
// name then snapshotCursor.
type snapshotVersionItem struct {
	Name   string `json:"name"`
	Cursor int64  `json:"snapshotCursor"`
}

// snapshotVersionListResponse is the list body with the top-level key order
// fixed to versions then count; Versions is always a non-nil slice so an
// empty view serializes as [].
type snapshotVersionListResponse struct {
	Versions []snapshotVersionItem `json:"versions"`
	Count    int                   `json:"count"`
}

// decodeVersionBindRequest enforces the rename endpoint's JSON shape:
// application/json body with a non-empty deviceId and a positive-integer
// snapshotCursor. Every violation is a 400 JSON error and the caller returns
// without touching the store.
func decodeVersionBindRequest(w http.ResponseWriter, r *http.Request) (deviceID string, cursor int64, ok bool) {
	var req snapshotVersionBindRequest
	if !decodeJSONBody(w, r, &req) {
		return "", 0, false
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return "", 0, false
	}
	// snapshotCursor must be present and a positive integer (no fractions,
	// strings, booleans, null or zero).
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return "", 0, false
	}
	c, good := parsePositiveInt(req.SnapshotCursor)
	if !good {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return "", 0, false
	}
	return req.DeviceID, c, true
}

// versionDeviceFromQuery resolves the calling device of a bodyless or
// read-only version endpoint from the deviceId query parameter. A missing or
// empty parameter is judged exactly like an unregistered device: a 404 JSON
// error ahead of any version or snapshot observation.
func versionDeviceFromQuery(w http.ResponseWriter, r *http.Request) (string, bool) {
	deviceID := r.URL.Query().Get("deviceId")
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return "", false
	}
	return deviceID, true
}

// writeVersionGateError maps the shared gate/existence errors of every version
// endpoint onto their fixed status codes, returning true when err was handled.
// The order mirrors the verdict order: device existence (404), permission
// (403), version and snapshot existence (404), conflict (409); none of the
// failure bodies exposes version content. A bind/rebind conflict carries the
// offending version name in the structured conflictId field alongside the
// error message, the same shape a change-commit conflict uses for its change
// id, instead of embedding it only in the error text.
func writeVersionGateError(w http.ResponseWriter, err error) bool {
	var conflict *events.ErrVersionConflict
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(w, http.StatusNotFound, "device not found")
	case errors.Is(err, store.ErrPermissionDenied):
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
	case errors.Is(err, events.ErrVersionNotFound):
		writeError(w, http.StatusNotFound, "snapshot version not found")
	case errors.Is(err, events.ErrSnapshotNotFound):
		writeError(w, http.StatusNotFound, "snapshot not found")
	case errors.As(err, &conflict):
		writeJSON(w, http.StatusConflict, map[string]string{
			"error":      "snapshot version is already bound to another cursor",
			"conflictId": conflict.Name,
		})
	default:
		return false
	}
	return true
}

// handlePutSnapshotVersion registers an existing snapshot under a stable
// version name:
//
//	POST /v1/documents/{documentID}/snapshots/versions
//
// The name is unique within the document and travels in the JSON body.
// Binding the same name to the same cursor again is idempotent and returns the
// identical body; binding that name to another cursor (or a name another
// cursor occupies) is a 409 with zero writes. The fixed verdict order is
// request shape (400), device existence (404), document permission (403), then
// snapshot and version existence (404) and the conflict verdict (409). Success
// is one compact JSON line naming the version and the snapshot cursor.
func handlePutSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req snapshotVersionRegisterRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
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

	result, err := s.PutSnapshotVersion(documentID, req.DeviceID, req.Name, cursor)
	if err != nil {
		if !writeVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to register snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleListSnapshotVersions is the read-only collection view:
//
//	GET /v1/documents/{documentID}/snapshots/versions?deviceId=D
//
// It returns every registration in ascending version-name order, each entry
// naming only the name and the snapshot cursor; an unknown document (or one
// without markers) reads as an empty array with count 0 rather than an error.
// The calling device must be registered (404) and still authorized (403); the
// read writes nothing and exposes no snapshot state.
func handleListSnapshotVersions(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	deviceID, ok := versionDeviceFromQuery(w, r)
	if !ok {
		return
	}

	versions, err := s.ListSnapshotVersions(documentID, deviceID)
	if err != nil {
		if !writeVersionGateError(w, err) {
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

// handleGetSnapshotVersion reads the snapshot state a version name points at:
//
//	GET /v1/documents/{documentID}/snapshots/versions/{name}?deviceId=D
//
// The body is byte-for-byte the by-cursor snapshot read
// ({"cursor":N,"state":...}): the state is presented exactly as stored. The
// verdict order is shape (400), device existence (404), permission (403),
// version and snapshot existence (404).
func handleGetSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := versionDeviceFromQuery(w, r)
	if !ok {
		return
	}

	cursor, state, err := s.GetSnapshotVersionState(documentID, deviceID, name)
	if err != nil {
		if !writeVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to load snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "state": state})
}

// handleRebindSnapshotVersion moves an existing version name onto another
// existing snapshot:
//
//	PUT /v1/documents/{documentID}/snapshots/versions/{name}
//
// The verdict order is request shape (400), device existence (404), permission
// (403), then the version marker (a missing name is 404) and the target
// snapshot (a missing cursor, an unknown document included, is 404). Rebinding
// to the cursor already bound is an idempotent no-op with the same body. The
// snapshot itself is never modified.
func handleRebindSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, cursor, ok := decodeVersionBindRequest(w, r)
	if !ok {
		return
	}

	result, err := s.RebindSnapshotVersion(documentID, deviceID, name, cursor)
	if err != nil {
		if !writeVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to rename snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleDeleteSnapshotVersion hard-deletes one version marker:
//
//	DELETE /v1/documents/{documentID}/snapshots/versions/{name}?deviceId=D
//
// The request carries no body. A missing marker is a 404 JSON error; the
// snapshot itself is untouched and, once the deletion commits, the name is
// free to bind again. Success is one compact JSON line naming the version and
// the deletion marker.
func handleDeleteSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := versionDeviceFromQuery(w, r)
	if !ok {
		return
	}

	if err := s.DeleteSnapshotVersion(documentID, deviceID, name); err != nil {
		if !writeVersionGateError(w, err) {
			writeError(w, http.StatusInternalServerError, "failed to delete snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"name": name, "deleted": true})
}

// handleRestoreSnapshotVersion restores the snapshot a version name points at
// as one ordinary change:
//
//	POST /v1/documents/{documentID}/snapshots/versions/{name}/restore
//
// The body names the calling device and the change id; the source snapshot is
// resolved from the path. On a hit the snapshot's state is appended exactly
// like the cursor-based restore in one serialized transaction (the document
// cursor advances by one) and the success body is byte-identical to it.
// Idempotency and conflict follow the existing restore rules: an id occupied
// by an ordinary change is a 409 with zero writes. The verdict order is shape
// (400), device existence (404), permission (403), version and snapshot
// existence (404), then the restore conflict (409).
func handleRestoreSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	var req snapshotVersionRestoreRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if req.ChangeID == "" {
		writeError(w, http.StatusBadRequest, "changeId must be a non-empty string")
		return
	}

	result, err := s.RestoreSnapshotVersion(documentID, req.DeviceID, req.ChangeID, name)
	if err != nil {
		var conflict *events.ErrRestoreConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrVersionNotFound):
			writeError(w, http.StatusNotFound, "snapshot version not found")
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "change id already exists with a different deviceId, snapshotCursor or source state: "+req.ChangeID)
		default:
			writeError(w, http.StatusInternalServerError, "failed to restore snapshot version")
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}
