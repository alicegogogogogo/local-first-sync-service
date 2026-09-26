package server

import (
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// changeExportResponse is the export body with the top-level key order fixed
// to changes then count. Changes is always a non-nil slice so an empty
// export serializes as [] rather than null. Each element is an
// events.ListedChange, whose key order is id, deviceId, payload, cursor — the
// same record the paged read emits.
type changeExportResponse struct {
	Changes []events.ListedChange `json:"changes"`
	Count   int                   `json:"count"`
}

// handleExportChanges is the read-only batch export over a document's change
// collection:
//
//	GET /v1/documents/{documentID}/changes?from=0&to=N
//
// from defaults to 0; to absent means no upper bound. Only changes whose
// cursor is in the closed [from, to] interval and still in the online log are
// returned, in ascending cursor order, at most one entry per cursor. An
// unknown document or a range without any online change — including one that
// lies entirely inside the trimmed region — is a successful 200 with an empty
// list and count 0, not an error. Changes compacted out of the online log
// never appear. The handler writes nothing: it creates no change, moves no
// cursor, records no change and notifies the push channels.
func handleExportChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	q := r.URL.Query()

	from := int64(0)
	if raw := q.Get("from"); raw != "" {
		v, ok := parseCursorPath(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "from must be a non-negative integer")
			return
		}
		from = v
	}

	var to *int64
	if raw := q.Get("to"); raw != "" {
		v, ok := parseCursorPath(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "to must be a non-negative integer")
			return
		}
		to = &v
	}
	if to != nil && *to < from {
		writeError(w, http.StatusBadRequest, "to must not be less than from")
		return
	}

	rows, err := s.ExportChanges(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export changes")
		return
	}
	if rows == nil {
		rows = make([]events.ListedChange, 0)
	}
	writeJSON(w, http.StatusOK, changeExportResponse{Changes: rows, Count: len(rows)})
}

// malformedChangeExportPath reports whether p targets the change-log
// collection's export shape but is not the collection's exact location
// (/v1/documents/{documentID}/changes): a trailing slash (an empty trailing
// segment) or extra path segments past the collection. The paged read and
// this export share that exact collection path; its terminal subresources
// (poll, compact, subscribe under the session family) have their own keyword
// guards, so only a genuinely unrecognized suffix reaches this check.
// ServeMux would answer it with a redirect or a plain-text 404/405; the
// change surface promises a JSON error and never a redirect. Empty segments
// are already rejected by the guard itself. The keyword is matched only past
// the documentID position, so a document literally named "changes" keeps its
// ordinary routes.
func malformedChangeExportPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "changes" || i == 0 {
			continue
		}
		// Keyword position reached. The collection is exactly
		// {documentID}/changes; the poll and compact subresources are their own
		// endpoints and keep their own guards. Anything else (a trailing slash,
		// an unrecognized extra segment) is a malformed 400.
		if i == 1 {
			collection := len(segs) == 2 && segs[0] != ""
			knownSubresource := len(segs) >= 3 && (segs[2] == "poll" || segs[2] == "compact")
			return !(collection || knownSubresource)
		}
		return false
	}
	return false
}
