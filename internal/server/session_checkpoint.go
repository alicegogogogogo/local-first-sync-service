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

// checkpointRequest is the session checkpoint confirmation body: only the
// cursor travels in the request. The calling device is the session's owning
// device resolved from the path, so a stray deviceId is decoded and ignored
// like every other unknown field.
type checkpointRequest struct {
	Cursor json.RawMessage `json:"cursor"`
}

// checkpointConfirmResponse is the confirmation answer: the confirmed cursor
// and whether the recorded position advanced (false on an idempotent
// re-confirmation of the same cursor).
type checkpointConfirmResponse struct {
	Cursor   int64 `json:"cursor"`
	Advanced bool  `json:"advanced"`
}

// checkpointStateResponse is the checkpoint read model: the recorded cursor
// (zero with recorded=false when the pair was never confirmed), the document's
// compaction boundary and its high-water mark.
type checkpointStateResponse struct {
	Cursor    int64 `json:"cursor"`
	Recorded  bool  `json:"recorded"`
	Boundary  int64 `json:"boundary"`
	MaxCursor int64 `json:"maxCursor"`
}

// handleSessionPutCheckpoint confirms the last change cursor a session has
// fully applied locally on one document:
//
//	PUT /v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint
//
// The body is a strict JSON object {"cursor":N} with N a non-negative integer;
// the calling device is the session's owning device, resolved from the path
// with the same identity rule as the session reads, commits, polls and
// subscriptions — no new credential is introduced.
//
// Checks run in the fixed order request shape (400), session existence (404),
// document permission (403) and cursor conflicts (409), and every rejection
// writes nothing. A first confirmation below the compaction boundary is a 409
// carrying the boundary (the client restores a snapshot first); confirming
// exactly the boundary succeeds. A cursor above the document's high-water mark
// is a 409 carrying maxCursor; a cursor below the recorded checkpoint is a 409
// carrying currentCursor. Re-confirming the recorded cursor is idempotent
// (advanced=false). A confirmation is not a change: it allocates no document
// cursor, wakes no long poll and signals no subscriber. An unknown document
// accepts only cursor 0 and the confirmation never makes the document exist.
func handleSessionPutCheckpoint(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req checkpointRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// cursor must be present and a non-negative integer (no fractions,
	// strings, booleans or null).
	if len(req.Cursor) == 0 {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}
	cursor, ok := parseNonNegativeInt(req.Cursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}

	// Request shape is settled; only now does the session lookup run, so a
	// malformed body against an unknown session is still a 400.
	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	advanced, err := s.ConfirmSessionCheckpoint(sessionID, deviceID, documentID, cursor)
	if err != nil {
		var regression *events.ErrCheckpointRegression
		var beyond *events.ErrCheckpointBeyond
		var belowBoundary *events.ErrCheckpointBoundary
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// confirmation; its cascade removed the session too, so the
			// identity the caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &regression):
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":         "checkpoint cursor regresses below the recorded cursor",
				"currentCursor": regression.Current,
			})
		case errors.As(err, &beyond):
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":     "checkpoint cursor is beyond the document's maximum cursor",
				"maxCursor": beyond.Max,
			})
		case errors.As(err, &belowBoundary):
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":    "first checkpoint cursor is below the compaction boundary",
				"boundary": belowBoundary.Boundary,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to confirm checkpoint")
		}
		return
	}

	writeJSON(w, http.StatusOK, checkpointConfirmResponse{Cursor: cursor, Advanced: advanced})
}

// handleSessionGetCheckpoint is the read-only view of a session's confirmed
// checkpoint on one document:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint
//
// It answers the recorded cursor (zero with recorded=false when the pair was
// never confirmed), the document's compaction boundary and its high-water
// mark; an unknown document reads as the all-zero state and the read never
// creates it. Checks run in the fixed order request shape (400), session
// existence (404) and document permission (403); the request carries no body.
// The read writes nothing and never advances the checkpoint.
func handleSessionGetCheckpoint(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return
	}

	state, err := s.ReadSessionCheckpoint(sessionID, documentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read checkpoint")
		return
	}

	writeJSON(w, http.StatusOK, checkpointStateResponse{
		Cursor:    state.Cursor,
		Recorded:  state.Recorded,
		Boundary:  state.Boundary,
		MaxCursor: state.MaxCursor,
	})
}

// malformedSessionCheckpointPath reports whether p targets the session-scoped
// sync checkpoint endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint): a
// missing "documents"/"changes" segment, an extra segment, or a "checkpoint"
// segment in a position short of the registered shape. ServeMux would answer
// those with a plain-text 404 (or a redirect for an empty segment); every
// failure of this endpoint must be a JSON 400 instead. Empty segments are
// already rejected by the guard itself.
//
// "checkpoint" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "checkpoint" occupies an identifier position (index 0 or 2)
// and is therefore left to the ordinary changes routes like any other id.
func malformedSessionCheckpointPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier) has
	// its own guard; "checkpoint" there is an ordinary identifier, not this
	// endpoint's keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict through
	// malformedSessionSnapshotsPath; a version name coinciding with
	// "checkpoint" is an ordinary identifier there, not this keyword.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "checkpoint" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "checkpoint" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly
		// "checkpoint" with the documents/changes scaffolding around it, and
		// no segment may follow. Any other occurrence is a malformed path.
		if i == 4 && len(segs) == 5 &&
			segs[0] != "" &&
			segs[1] == "documents" &&
			segs[2] != "" &&
			segs[3] == "changes" {
			return false
		}
		return true
	}
	return false
}
