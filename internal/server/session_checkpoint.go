package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/checkpoint"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// checkpointRequest is the body of PUT
// .../sessions/{sessionId}/documents/{documentId}/changes/checkpoint: only the
// non-negative integer cursor travels in the request. The calling device is
// the session's owning device resolved from the path, exactly as every other
// session-scoped endpoint does; a stray deviceId is decoded and ignored like
// every other unknown field and can never override the session owner.
type checkpointRequest struct {
	Cursor json.RawMessage `json:"cursor"`
}

// handleSessionPutCheckpoint confirms the session's applied position for one
// document:
//
//	PUT /v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint
//
// with a strict application/json object {"cursor":N} where N is a non-negative
// integer. Writing or advancing the position answers 200 with
// {"cursor":N,"advanced":true}; re-confirming the recorded cursor answers
// {"cursor":N,"advanced":false} and writes nothing. A confirmation creates no
// change, allocates no document cursor, wakes no long poll and notifies no
// subscriber.
//
// Checks run in the fixed order request shape (400) — content type, JSON
// validity, trailing content, a missing or mistyped cursor — then session
// existence (404), document permission (403) and finally the cursor conflict
// (409): a cursor below the recorded one carries currentCursor, one above the
// document high-water mark carries maxCursor, and the first confirmation below
// the compaction boundary (equality allowed) carries boundary so the client
// restores the snapshot first. Every failure writes nothing.
func handleSessionPutCheckpoint(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req checkpointRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// cursor must be present and a JSON integer >= 0: no missing key, null,
	// float, string, boolean or negative value.
	if len(req.Cursor) == 0 {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}
	cursor, ok := parseNonNegativeInt(req.Cursor)
	if !ok {
		writeError(w, http.StatusBadRequest, "cursor must be a non-negative integer")
		return
	}

	result, err := s.PutSessionCheckpoint(sessionID, documentID, cursor)
	if err != nil {
		writeCheckpointError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleSessionGetCheckpoint reads the session's recorded position:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint
//
// answering {"cursor":N,"recorded":true,"boundary":B,"maxCursor":M}. A pair
// that has never been confirmed answers cursor 0 and recorded=false; an
// unknown document answers boundary and maxCursor zero and is not created by
// the read. The read writes nothing, advances no cursor and notifies nobody.
// Checks run in the same fixed order as the PUT minus the body: request
// validity (400, the path/method guards), session existence (404) and document
// permission (403).
func handleSessionGetCheckpoint(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	result, err := s.GetSessionCheckpoint(sessionID, documentID)
	if err != nil {
		writeCheckpointError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// writeCheckpointError maps the checkpoint service's verdicts to the endpoint's
// fixed status codes. A session whose owning device was deregistered between
// the handler and the transaction is indistinguishable from a deleted session
// (the deregistration cascade removed it too), so ErrDeviceNotFound is a 404;
// a revoked device is a 403. The three 409 conflicts each carry the cursor
// value the client needs to reconcile: currentCursor for a regression,
// maxCursor above the high-water mark and boundary below the compaction
// boundary.
func writeCheckpointError(w http.ResponseWriter, err error) {
	var regress *checkpoint.ErrRegress
	var aboveMax *checkpoint.ErrAboveMax
	var belowBoundary *checkpoint.ErrBelowBoundary
	switch {
	case errors.Is(err, store.ErrSessionNotFound), errors.Is(err, store.ErrDeviceNotFound):
		writeError(w, http.StatusNotFound, "session not found")
	case errors.Is(err, store.ErrPermissionDenied):
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
	case errors.As(err, &regress):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":         "checkpoint cursor must not move backward",
			"currentCursor": regress.Current,
		})
	case errors.As(err, &aboveMax):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":     "checkpoint cursor is above the document maximum",
			"maxCursor": aboveMax.Max,
		})
	case errors.As(err, &belowBoundary):
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":    "checkpoint cursor is below the compaction boundary; restore the snapshot first",
			"boundary": belowBoundary.Boundary,
		})
	default:
		writeError(w, http.StatusInternalServerError, "failed to update checkpoint")
	}
}

// malformedSessionCheckpointPath reports whether p targets the session-scoped
// sync checkpoint endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/checkpoint): a
// missing "documents"/"changes" segment, an extra segment, a trailing slash,
// or a "checkpoint" segment in a position short of the registered shape.
// ServeMux would answer those with a redirect or a plain-text 404/405; every
// failure of this endpoint must be a JSON 400 instead and never a redirect.
// Empty segments are already rejected by the guard itself.
//
// "checkpoint" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "checkpoint" occupies an identifier position (index 0 or 2)
// and keeps its ordinary routes.
func malformedSessionCheckpointPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "checkpoint" there is an ordinary identifier.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict; a
	// version name coinciding with "checkpoint" is an ordinary identifier
	// there.
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
		// "checkpoint" with the documents/changes scaffolding around it, and no
		// segment may follow. Any other occurrence is a malformed path.
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
