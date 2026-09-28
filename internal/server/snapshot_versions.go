package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// snapshotVersionItem is one element of a version list (and the register/
// rename response). The field order fixes the on-the-wire key order to name
// then cursor; a marker carries only the name and the cursor, never the
// snapshot state.
type snapshotVersionItem struct {
	Name   string `json:"name"`
	Cursor int64  `json:"cursor"`
}

// snapshotVersionListResponse is the list body with the top-level key order
// fixed to versions then count. Versions is always a non-nil slice so an empty
// document serializes as [] rather than null.
type snapshotVersionListResponse struct {
	Versions []snapshotVersionItem `json:"versions"`
	Count    int                   `json:"count"`
}

// snapshotVersionDeleteResponse is the delete body: the removed marker's name
// and the deletion marker, in that fixed key order.
type snapshotVersionDeleteResponse struct {
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

// versionRegisterRequest is the register body: the version name and the
// existing snapshot cursor it points at.
type versionRegisterRequest struct {
	Name   string          `json:"name"`
	Cursor json.RawMessage `json:"cursor"`
}

// versionRenameRequest is the rename body: only the new target snapshot
// cursor travels in the body; the version name is the path item.
type versionRenameRequest struct {
	Cursor json.RawMessage `json:"cursor"`
}

// versionRestoreRequest is the document-level named-restore body. The
// snapshot cursor is absent: the path name resolves it.
type versionRestoreRequest struct {
	DeviceID string `json:"deviceId"`
	ChangeID string `json:"changeId"`
}

// sessionVersionRestoreRequest is the session-scoped named-restore body: only
// the change id travels in the body; the device is the session's owner and the
// snapshot cursor is resolved from the path name.
type sessionVersionRestoreRequest struct {
	ChangeID string `json:"changeId"`
}

// writeSnapshotVersionList renders the marker collection in the shared
// response shape: one compact JSON line with the versions,count key order,
// each item keyed name,cursor in ascending name order, an empty document
// serializing as [] with count 0. It is shared by the document-scoped and
// session-scoped lists so their bodies cannot drift.
func writeSnapshotVersionList(w http.ResponseWriter, versions []events.SnapshotVersion) {
	items := make([]snapshotVersionItem, 0, len(versions))
	for _, v := range versions {
		items = append(items, snapshotVersionItem{Name: v.Name, Cursor: v.Cursor})
	}
	writeJSON(w, http.StatusOK, snapshotVersionListResponse{Versions: items, Count: len(items)})
}

// writeSnapshotVersionState renders a named snapshot read with exactly the
// body the cursor read emits — {"cursor":N,"state":...} — so reading a
// snapshot by name is byte-identical to reading it by cursor.
func writeSnapshotVersionState(w http.ResponseWriter, cursor int64, state json.RawMessage) {
	writeJSON(w, http.StatusOK, map[string]any{"cursor": cursor, "state": state})
}

// handleRegisterSnapshotVersion registers a name over an existing snapshot:
//
//	POST /v1/documents/{documentID}/snapshot-versions
//	{"name": "v1", "cursor": 2}
//
// The target must be an existing snapshot; a well-formed cursor without one is
// a 404. Re-posting the same name against the same cursor is idempotent and
// the body is identical to the first registration (name and cursor, no created
// flag); re-posting it against another cursor is a 409 and writes nothing.
func handleRegisterSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	name, cursor, ok := decodeVersionRegister(w, r)
	if !ok {
		return
	}

	version, _, err := s.RegisterSnapshotVersion(documentID, name, cursor)
	if err != nil {
		var conflict *events.ErrVersionConflict
		switch {
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "snapshot version is already bound to another cursor",
				"name":  conflict.Name,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to register snapshot version")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionItem{Name: version.Name, Cursor: version.Cursor})
}

// handleListSnapshotVersions is the document-level marker collection read:
//
//	GET /v1/documents/{documentID}/snapshot-versions
//
// Every marker is returned in ascending name order with only name and cursor.
// An unknown document or one without markers is a successful empty list with
// count 0, not an error. The read writes nothing and moves no cursor.
func handleListSnapshotVersions(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	versions, err := s.ListSnapshotVersions(documentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list snapshot versions")
		return
	}
	writeSnapshotVersionList(w, versions)
}

// handleGetSnapshotVersion reads a snapshot by its version name:
//
//	GET /v1/documents/{documentID}/snapshot-versions/{name}
//
// The success body is byte-identical to GET .../snapshots/{cursor}:
// {"cursor":N,"state":...}, the state presented exactly as stored. An
// unregistered name (an unknown document included) is a 404.
func handleGetSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	cursor, ok := resolveSnapshotVersion(s, w, documentID, name)
	if !ok {
		return
	}
	state, err := s.GetSnapshot(documentID, cursor)
	if err != nil {
		if errors.Is(err, events.ErrSnapshotNotFound) {
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load snapshot")
		return
	}
	writeSnapshotVersionState(w, cursor, state)
}

// handleRenameSnapshotVersion moves a version name onto another existing
// snapshot:
//
//	POST /v1/documents/{documentID}/snapshot-versions/{name}/rename
//	{"cursor": 3}
//
// An unknown name or a cursor without a snapshot is a 404; rebinding to the
// cursor already bound is idempotent. The snapshots themselves are never
// modified.
func handleRenameSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	cursor, ok := decodeVersionRename(w, r)
	if !ok {
		return
	}

	version, _, err := s.RenameSnapshotVersion(documentID, name, cursor)
	if err != nil {
		switch {
		case errors.Is(err, events.ErrVersionNotFound):
			writeError(w, http.StatusNotFound, "snapshot version not found")
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to rename snapshot version")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionItem{Name: version.Name, Cursor: version.Cursor})
}

// handleDeleteSnapshotVersion removes a marker without touching the snapshot
// it named:
//
//	DELETE /v1/documents/{documentID}/snapshot-versions/{name}
//
// A repeat delete (an unknown name or document included) is a 404 and writes
// nothing; afterward the name is free to register again.
func handleDeleteSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	if err := s.DeleteSnapshotVersion(documentID, name); err != nil {
		if errors.Is(err, events.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, "snapshot version not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete snapshot version")
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionDeleteResponse{Name: name, Deleted: true})
}

// handleRestoreSnapshotVersion restores the snapshot a version name points at
// as one ordinary change:
//
//	POST /v1/documents/{documentID}/snapshot-versions/{name}/restore
//	{"deviceId": "device-1", "changeId": "change-9"}
//
// The name resolves to its snapshot cursor; from there the judgment is exactly
// the cursor restore's: an unknown name is a 404 and an id occupied by an
// ordinary change (or a differing prior restore) is a 409. The document cursor
// advances by one on a created restore.
func handleRestoreSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	var req versionRestoreRequest
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
		writeVersionRestoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// ---- Session-scoped versions: the same surface over the session document
// prefix, with the session's owning device as the caller and the fixed check
// order request shape (400), session existence (404), document permission
// (403), version/snapshot existence (404). ----

// handleSessionRegisterSnapshotVersion is the session-scoped registration over
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions
//
// with the body and success body identical to the document-level registration.
// The permission verdict is taken inside the registration transaction.
func handleSessionRegisterSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	name, cursor, ok := decodeVersionRegister(w, r)
	if !ok {
		return
	}

	// Request shape is settled; only now does the session lookup run.
	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}

	version, _, err := s.RegisterSessionSnapshotVersion(documentID, deviceID, name, cursor)
	if err != nil {
		var conflict *events.ErrVersionConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error": "snapshot version is already bound to another cursor",
				"name":  conflict.Name,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to register snapshot version")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionItem{Name: version.Name, Cursor: version.Cursor})
}

// handleSessionListSnapshotVersions is the session-scoped marker collection
// read at GET /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions.
// The checks and body are the session list's fixed order; the body is
// byte-identical to the document-level list.
func handleSessionListSnapshotVersions(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}
	if !sessionDocumentAuthorized(w, s, documentID, deviceID) {
		return
	}

	versions, err := s.ListSnapshotVersions(documentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list snapshot versions")
		return
	}
	writeSnapshotVersionList(w, versions)
}

// handleSessionGetSnapshotVersion is the session-scoped named read at
// GET /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions/{name}.
// Its success body is byte-identical to the cursor read and the document-level
// named read.
func handleSessionGetSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}
	if !sessionDocumentAuthorized(w, s, documentID, deviceID) {
		return
	}

	cursor, ok := resolveSnapshotVersion(s, w, documentID, name)
	if !ok {
		return
	}
	state, err := s.GetSnapshot(documentID, cursor)
	if err != nil {
		if errors.Is(err, events.ErrSnapshotNotFound) {
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load snapshot")
		return
	}
	writeSnapshotVersionState(w, cursor, state)
}

// handleSessionRenameSnapshotVersion is the session-scoped rename at
// POST /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions/{name}/rename.
func handleSessionRenameSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	cursor, ok := decodeVersionRename(w, r)
	if !ok {
		return
	}

	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}

	version, _, err := s.RenameSessionSnapshotVersion(documentID, deviceID, name, cursor)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrVersionNotFound):
			writeError(w, http.StatusNotFound, "snapshot version not found")
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to rename snapshot version")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionItem{Name: version.Name, Cursor: version.Cursor})
}

// handleSessionDeleteSnapshotVersion is the session-scoped delete at
// DELETE /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions/{name}.
func handleSessionDeleteSnapshotVersion(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty
	name := r.PathValue("name")             // route pattern + guard guarantee non-empty

	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}

	if err := s.DeleteSessionSnapshotVersion(documentID, deviceID, name); err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrVersionNotFound):
			writeError(w, http.StatusNotFound, "snapshot version not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to delete snapshot version")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotVersionDeleteResponse{Name: name, Deleted: true})
}

// handleSessionRestoreSnapshotVersion is the session-scoped named restore at
// POST /v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions/{name}/restore.
// The body carries only changeId; the snapshot cursor is resolved from the
// name and every judgment past it is the existing session restore's.
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

	deviceID, ok := sessionDevice(w, s, sessionID)
	if !ok {
		return
	}

	result, err := s.RestoreSessionSnapshotVersion(documentID, deviceID, req.ChangeID, name)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeVersionRestoreError(w, err)
		}
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// decodeVersionRegister validates the shared register body (name plus a
// positive-integer cursor), writing the 400 and returning false on any shape
// violation. It is shared by the document and session registrations.
func decodeVersionRegister(w http.ResponseWriter, r *http.Request) (string, int64, bool) {
	var req versionRegisterRequest
	if !decodeJSONBody(w, r, &req) {
		return "", 0, false
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name must be a non-empty string")
		return "", 0, false
	}
	// The target snapshot cursor must be present and a positive integer; a
	// well-formed cursor without a snapshot is judged later as a 404.
	if len(req.Cursor) == 0 {
		writeError(w, http.StatusBadRequest, "cursor must be a positive integer")
		return "", 0, false
	}
	cursor, ok := parsePositiveInt(req.Cursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a positive integer")
		return "", 0, false
	}
	return req.Name, cursor, true
}

// decodeVersionRename validates the shared rename body (a positive-integer
// cursor), writing the 400 and returning false on any shape violation.
func decodeVersionRename(w http.ResponseWriter, r *http.Request) (int64, bool) {
	var req versionRenameRequest
	if !decodeJSONBody(w, r, &req) {
		return 0, false
	}
	if len(req.Cursor) == 0 {
		writeError(w, http.StatusBadRequest, "cursor must be a positive integer")
		return 0, false
	}
	cursor, ok := parsePositiveInt(req.Cursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a positive integer")
		return 0, false
	}
	return cursor, true
}

// sessionDevice resolves the session's owning device, mapping a missing
// session to the fixed 404 verdict. It is the session handlers' shared first
// check past request shape.
func sessionDevice(w http.ResponseWriter, s *app.App, sessionID string) (string, bool) {
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

// sessionDocumentAuthorized enforces the session view's document-permission
// verdict (403) for its read-only entries, writing the response and returning
// false on denial.
func sessionDocumentAuthorized(w http.ResponseWriter, s *app.App, documentID, deviceID string) bool {
	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return false
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return false
	}
	return true
}

// resolveSnapshotVersion looks the named marker up and maps a miss to the
// fixed 404 verdict shared by the document and session named reads.
func resolveSnapshotVersion(s *app.App, w http.ResponseWriter, documentID, name string) (int64, bool) {
	version, err := s.GetSnapshotVersion(documentID, name)
	if err != nil {
		if errors.Is(err, events.ErrVersionNotFound) {
			writeError(w, http.StatusNotFound, "snapshot version not found")
			return 0, false
		}
		writeError(w, http.StatusInternalServerError, "failed to load snapshot version")
		return 0, false
	}
	return version.Cursor, true
}

// writeVersionRestoreError maps the shared named-restore failures: an unknown
// version (or its snapshot) is a 404, an occupied change id a 409 carrying the
// conflict id, exactly like the cursor restore.
func writeVersionRestoreError(w http.ResponseWriter, err error) {
	var conflict *events.ErrRestoreConflict
	switch {
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
}

// inSnapshotVersionsNamespace reports whether p lives inside either named-
// version subtree — the document-level
// (/v1/documents/{documentID}/snapshot-versions...) or the session-scoped
// (/v1/sessions/{sessionId}/documents/{documentId}/snapshot-versions...).
// Inside the subtree the version guards own every shape judgment: a version
// name is a free-form identifier and may itself spell "restore", "poll",
// "compact" and the other endpoint keywords, which the older keyword-scanning
// guards would otherwise misread as their own endpoint. The keyword must sit
// in its collection position (past a non-empty document id), so a document
// literally named "snapshot-versions" keeps its ordinary routes.
func inSnapshotVersionsNamespace(p string) bool {
	if rest, ok := strings.CutPrefix(p, "/v1/documents/"); ok {
		segs := strings.Split(rest, "/")
		return len(segs) >= 2 && segs[0] != "" && segs[1] == "snapshot-versions"
	}
	if rest, ok := strings.CutPrefix(p, "/v1/sessions/"); ok {
		segs := strings.Split(rest, "/")
		return len(segs) >= 4 && segs[0] != "" &&
			segs[1] == "documents" && segs[2] != "" && segs[3] == "snapshot-versions"
	}
	return false
}

// malformedSnapshotVersionPath reports whether p targets the document-level
// named-version namespace but is not at one of its exact locations:
//
//	POST/GET /v1/documents/{documentID}/snapshot-versions
//	GET/DELETE /v1/documents/{documentID}/snapshot-versions/{name}
//	POST /v1/documents/{documentID}/snapshot-versions/{name}/rename
//	POST /v1/documents/{documentID}/snapshot-versions/{name}/restore
//
// A trailing slash (an empty name, including one on the collection path),
// extra path segments, an unknown terminal subresource, or a
// "snapshot-versions" segment in any keyword position short of one of those
// shapes is a malformed 400 rather than ServeMux's redirect or plain-text
// 404/405: every version endpoint promises a JSON error and never a redirect.
// Empty segments are already rejected by the guard itself. The keyword is
// matched only past the documentID position, so a document literally named
// "snapshot-versions" keeps its ordinary routes.
func malformedSnapshotVersionPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "snapshot-versions" || i == 0 {
			continue
		}
		collection := len(segs) == 2 && segs[0] != ""
		item := len(segs) == 3 && segs[0] != "" && segs[2] != ""
		subresource := len(segs) == 4 && segs[0] != "" && segs[2] != "" &&
			(segs[3] == "rename" || segs[3] == "restore")
		return !(collection || item || subresource)
	}
	return false
}

// malformedSessionSnapshotVersionsPath reports whether p targets the
// session-scoped named-version namespace but is not at one of its four
// shapes (collection, item, rename, restore) under
// /v1/sessions/{sessionId}/documents/{documentId}/. ServeMux would answer
// anything else with a plain-text 404 (or a redirect for an empty segment);
// the version surface promises a JSON 400 and never a redirect. Empty
// segments are already rejected by the guard itself.
//
// "snapshot-versions" is treated as the collection keyword only in the fourth
// segment, right after the document identifier; a session or document
// identifier literally named "snapshot-versions" occupies an identifier
// position and keeps its ordinary routes. The CRDT namespace has its own
// guard, so a "snapshot-versions" segment under crdt/ is an ordinary
// identifier.
func malformedSessionSnapshotVersionsPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	if len(segs) < 4 || segs[1] != "documents" || segs[3] != "snapshot-versions" {
		return false
	}
	collection := len(segs) == 4 && segs[0] != "" && segs[2] != ""
	item := len(segs) == 5 && segs[0] != "" && segs[2] != "" && segs[4] != ""
	subresource := len(segs) == 6 && segs[0] != "" && segs[2] != "" && segs[4] != "" &&
		(segs[5] == "rename" || segs[5] == "restore")
	return !(collection || item || subresource)
}
