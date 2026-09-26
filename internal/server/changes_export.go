package server

import (
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// changeExportResponse is the export body with the top-level key order fixed
// to changes then count. Changes is always a non-nil slice so an empty export
// serializes as [] rather than null, and each element is the same ListedChange
// shape the paged read emits, so a record at one cursor is byte-identical
// across the two entries.
type changeExportResponse struct {
	Changes []events.ListedChange `json:"changes"`
	Count   int                   `json:"count"`
}

// handleExportChanges is the read-only batch export over a document's online
// change log:
//
//	GET /v1/documents/{documentID}/changes/export?from=0&to=N
//
// from defaults to 0; to absent means no upper bound. Only changes whose
// cursor is in the closed [from, to] interval and still online are returned,
// in ascending cursor order, at most one record per cursor; changes that
// compaction trimmed out of the online log are absent, so an interval inside
// the trimmed range exports empty. An unknown document or a range without any
// change is a successful 200 with an empty list and count 0, not an error. The
// handler writes nothing: it creates no change, moves no cursor and notifies
// neither the long polls nor the push channels.
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

	changes, err := s.ExportChanges(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export changes")
		return
	}

	items := make([]events.ListedChange, 0, len(changes))
	items = append(items, changes...)
	writeJSON(w, http.StatusOK, changeExportResponse{Changes: items, Count: len(items)})
}
