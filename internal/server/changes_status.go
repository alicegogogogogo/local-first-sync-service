package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// changeStatusResponse is the change-log status summary with the top-level
// key order fixed to onlineCount, boundary, maxCursor then trimmedCount.
type changeStatusResponse struct {
	OnlineCount  int64 `json:"onlineCount"`
	Boundary     int64 `json:"boundary"`
	MaxCursor    int64 `json:"maxCursor"`
	TrimmedCount int64 `json:"trimmedCount"`
}

// handleChangeStatus is the read-only change-log status summary:
//
//	GET /v1/documents/{documentID}/changes/status?deviceId=D
//
// It lets a reconnecting client see at a glance how much of the change log it
// still has to resume. The request is a bodyless GET and the calling device is
// declared by the deviceId query parameter, the same declaration the other
// read-only document entries use — no new authentication is introduced.
//
// Success is one compact JSON line ending in a newline with the top-level
// keys onlineCount, boundary, maxCursor and trimmedCount in that fixed order:
//
//   - onlineCount counts only the changes still held in the online log;
//   - boundary is the greatest cursor carrying a saved snapshot of the
//     document, zero when the document has none;
//   - maxCursor is the greatest cursor among the changes still online, zero
//     when the online log holds no row;
//   - trimmedCount is the total number of change ids compaction has moved out
//     of the online log.
//
// An unknown document is a successful 200 with all four numbers zero: empty
// state is an answer, not an error, and the read creates neither a change nor
// a snapshot. The verdict order is request shape (400) first, then device
// existence (a missing, empty or unregistered deviceId is a 404) and document
// permission (a revoked device is a 403); a 404 or 403 exposes no statistics.
// The summary writes nothing: it creates or deletes no change and no snapshot,
// allocates no cursor and wakes neither a long poll nor a push subscriber.
func handleChangeStatus(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	deviceID := r.URL.Query().Get("deviceId")
	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller, exactly as on the other read-only query-parameter entries.
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	status, err := s.GetChangesStatus(documentID, deviceID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		default:
			writeError(w, http.StatusInternalServerError, "failed to read change status")
		}
		return
	}

	writeJSON(w, http.StatusOK, changeStatusResponse{
		OnlineCount:  status.OnlineCount,
		Boundary:     status.Boundary,
		MaxCursor:    status.MaxCursor,
		TrimmedCount: status.TrimmedCount,
	})
}

// malformedChangeStatusPath reports whether p targets the change-log status
// summary but is not at its exact location
// (/v1/documents/{documentID}/changes/status): a missing "changes" segment, an
// extra segment, a trailing slash (an empty trailing segment), or a "status"
// segment in a position short of the registered shape. ServeMux would answer
// those with a redirect or a plain-text 404/405; every failure of this
// endpoint must be a JSON 400 instead and never a redirect. Empty segments
// are already rejected by the guard itself.
//
// "status" is treated as the endpoint keyword only in its terminal segment
// position (the third segment, index 2); a document identifier literally
// named "status" occupies an identifier position (index 0) and keeps its
// ordinary routes.
func malformedChangeStatusPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guards; "status" there is an ordinary identifier.
	if len(segs) >= 2 && segs[1] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its own shape verdict.
	if len(segs) >= 3 && segs[0] != "" && segs[1] == "snapshots" && segs[2] == "versions" {
		return false
	}
	for i, seg := range segs {
		if seg != "status" {
			continue
		}
		// Identifier position: a document named "status" is an ordinary id.
		if i == 0 {
			continue
		}
		// Endpoint keyword position: the third segment must be exactly
		// "status" with a non-empty documentID and "changes" around it, and no
		// segment may follow. Any other occurrence is a malformed path.
		if i == 2 && len(segs) == 3 &&
			segs[0] != "" &&
			segs[1] == "changes" {
			return false
		}
		return true
	}
	return false
}
