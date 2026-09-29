package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// changeStatusResponse is the success body of GET .../changes/status. The
// struct field order fixes the on-the-wire key order: changeCount, boundary,
// maxCursor then compacted, and the shared JSON writer emits it as one compact
// line with a trailing newline.
type changeStatusResponse struct {
	ChangeCount int64 `json:"changeCount"`
	Boundary    int64 `json:"boundary"`
	MaxCursor   int64 `json:"maxCursor"`
	Compacted   int64 `json:"compacted"`
}

// handleChangeStatus is the read-only change-log resumption summary:
//
//	GET /v1/documents/{documentID}/changes/status?deviceId=D
//
// The calling device is declared by the deviceId query parameter — the same
// declaration the other read-only document entries use; no new authentication
// is introduced and the request carries no body. The answer tells a
// reconnecting client how much catch-up work remains in one line:
//
//   - changeCount counts only the changes still in the online log;
//   - boundary is the greatest cursor with a saved snapshot, zero when the
//     document has none;
//   - maxCursor is the highest cursor among the still-online rows, zero when
//     the online log is empty;
//   - compacted is the total number of ids compaction moved out of the online
//     log.
//
// An unknown document succeeds with all four numbers zero: an empty status is
// not an error and creates neither a change nor a snapshot. A missing, empty
// or unregistered deviceId is a 404 JSON error; a device whose permission for
// the document was revoked gets a 403 JSON error; neither exposes any
// statistics and both write nothing. The status creates no change, allocates
// no cursor and wakes neither a long poll nor a push subscriber.
func handleChangeStatus(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	deviceID := r.URL.Query().Get("deviceId")
	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller; judging it as 404 keeps the existence verdict ahead of any
	// change-content observation, exactly as the other query-param reads do.
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	status, err := s.GetChangeStatus(documentID, deviceID)
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
		ChangeCount: status.OnlineCount,
		Boundary:    status.Boundary,
		MaxCursor:   status.MaxCursor,
		Compacted:   status.Compacted,
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
	// The named-snapshot-version subtree owns its own shape verdict; a version
	// name coinciding with "status" is an ordinary identifier there.
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
