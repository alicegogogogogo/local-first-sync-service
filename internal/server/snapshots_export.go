package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// snapshotExportItem is one element of an export body. The struct field order
// fixes the on-the-wire key order to cursor then state; State is embedded as
// raw JSON so the stored content is emitted verbatim, exactly as the single
// snapshot read emits it.
type snapshotExportItem struct {
	Cursor int64           `json:"cursor"`
	State  json.RawMessage `json:"state"`
}

// snapshotExportResponse is the export body with the top-level key order fixed
// to snapshots then count. Snapshots is always a non-nil slice so an empty
// export serializes as [] rather than null.
type snapshotExportResponse struct {
	Snapshots []snapshotExportItem `json:"snapshots"`
	Count     int                  `json:"count"`
}

// handleExportSnapshots is the read-only batch export over a document's
// snapshot collection:
//
//	GET /v1/documents/{documentID}/snapshots?from=0&to=N
//
// from defaults to 0; to absent means no upper bound. Only snapshots whose
// cursor is in the closed [from, to] interval are returned, in ascending
// cursor order, at most one entry per cursor. An unknown document or a range
// without any snapshot is a successful 200 with an empty list and count 0, not
// an error. The handler writes nothing: it creates no snapshot, moves no
// cursor, records no change and notifies the push channels.
func handleExportSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
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

	snaps, err := s.ExportSnapshots(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export snapshots")
		return
	}

	items := make([]snapshotExportItem, 0, len(snaps))
	for _, snap := range snaps {
		items = append(items, snapshotExportItem{Cursor: snap.Cursor, State: snap.State})
	}
	writeJSON(w, http.StatusOK, snapshotExportResponse{Snapshots: items, Count: len(items)})
}

// malformedSnapshotPath reports whether p targets the snapshot namespace but
// is not at one of its two exact locations:
//
//	GET  /v1/documents/{documentID}/snapshots          (the batch export)
//	GET  /v1/documents/{documentID}/snapshots/{cursor} (the single read)
//
// A trailing slash (an empty cursor segment, including the collection path
// with a trailing slash), extra path segments, or a "snapshots" segment in any
// keyword position short of one of those shapes is a malformed 400 rather than
// ServeMux's redirect or plain-text 404/405: the snapshot endpoints promise a
// JSON error and never a redirect. Empty segments are already rejected by the
// guard itself. The keyword is matched only past the documentID position, so a
// document literally named "snapshots" keeps its ordinary routes.
func malformedSnapshotPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "snapshots" || i == 0 {
			continue
		}
		// Keyword position reached. The collection is exactly
		// {documentID}/snapshots; the single read adds one non-empty cursor
		// segment. Anything else (trailing slash, missing/extra segments) is
		// malformed.
		collection := len(segs) == 2 && segs[0] != ""
		item := len(segs) == 3 && segs[0] != "" && segs[2] != ""
		return !(collection || item)
	}
	return false
}
