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

// sessionRestoreRequest is the session-scoped restore body: only the change
// id and the snapshot cursor travel in the request. The calling device is the
// session's owning device resolved from the path, so a stray deviceId is
// decoded and ignored like every other unknown field.
type sessionRestoreRequest struct {
	ChangeID       string          `json:"changeId"`
	SnapshotCursor json.RawMessage `json:"snapshotCursor"`
}

// handleSessionRestore is the session-scoped history restore, the session
// view's counterpart to POST /v1/documents/{documentID}/restore, mounted one
// segment below the session document resource:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/restore
//
// The body carries only changeId and snapshotCursor; the calling device is the
// session's owning device resolved from the path with the same identity rule
// as the session reads, exports, commits, replays, merges, polls and
// subscriptions — no new credential is introduced, and a stray deviceId in the
// body is ignored like any other unknown field. On a snapshot hit the snapshot
// state is appended as one ordinary change in a single serialized transaction:
// old rows are untouched and the document cursor advances by one. The success
// body is byte-identical to the document-level restore: id, created, the
// assigned cursor and the source snapshot cursor. The checks run in the fixed
// order request shape (400), session existence (404), document permission
// (403) — the permission verdict is taken inside the restore transaction, so a
// revoked device writes nothing — followed by the snapshot lookup (a miss, an
// unknown document included, is 404) and the idempotency/conflict verdict (an
// id occupied by an ordinary change, or a restore whose device, snapshot
// cursor or source state differs, is 409). Every rejection leaves the log
// untouched.
func handleSessionRestore(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionRestoreRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.ChangeID == "" {
		writeError(w, http.StatusBadRequest, "changeId must be a non-empty string")
		return
	}
	// snapshotCursor must be present and a positive integer (no fractions,
	// strings, booleans, null or zero).
	if len(req.SnapshotCursor) == 0 {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}
	snapshotCursor, ok := parsePositiveInt(req.SnapshotCursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "snapshotCursor must be a positive integer")
		return
	}

	// Request shape is settled; only now does the session lookup run, so a
	// malformed restore against an unknown session is still a 400.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	result, err := s.RestoreSessionSnapshot(documentID, deviceID, req.ChangeID, snapshotCursor)
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
		case errors.Is(err, events.ErrSnapshotNotFound):
			writeError(w, http.StatusNotFound, "snapshot not found")
		case errors.As(err, &conflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":      "change id already exists with a different deviceId, snapshotCursor or source state",
				"conflictId": conflict.ID,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to restore snapshot")
		}
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// malformedSessionRestorePath reports whether p targets the session-scoped
// restore endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/restore): a missing
// "documents" segment, an extra segment, or a "restore" segment in a position
// short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "restore" is treated as the endpoint keyword only in its terminal segment
// position (the fourth segment, index 3); a session or document identifier
// literally named "restore" occupies an identifier position (index 0 or 2) and
// is therefore left to the ordinary session routes like any other id.
func malformedSessionRestorePath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "restore" there is an ordinary identifier, not the
	// restore keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict through
	// malformedSessionSnapshotsPath: a version literally named "restore" and
	// the by-name restore nested one segment below it are not the document
	// restore keyword, whose only valid position is the fourth segment.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "restore" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "restore" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fourth segment must be exactly
		// "restore" with the documents scaffolding around it, and no segment
		// may follow. Any other occurrence is a malformed path.
		if i == 3 && len(segs) == 4 &&
			segs[0] != "" &&
			segs[1] == "documents" &&
			segs[2] != "" {
			return false
		}
		return true
	}
	return false
}
