package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// exportSnapshotElement is one entry of the export body. Field order is the
// wire order: cursor then state.
type exportSnapshotElement struct {
	Cursor int64           `json:"cursor"`
	State  json.RawMessage `json:"state"`
}

// exportSnapshotBody fixes the top-level wire order: snapshots then count.
type exportSnapshotBody struct {
	Snapshots []exportSnapshotElement `json:"snapshots"`
	Count     int                     `json:"count"`
}

// handleExportSnapshots is the read-only batch export over a document's
// snapshot collection:
//
//	GET /v1/documents/{documentID}/snapshots?from=0&to=N
//
// from defaults to 0 and a missing to leaves the upper end open. Only
// snapshots whose cursor lies in the closed [from, to] interval are returned,
// in ascending cursor order, at most one entry per cursor. An unknown document
// or an empty interval is a successful 200 with an empty list and count 0, not
// an error. The handler never writes: it creates no snapshot and changes no
// cursor, change log or subscription state.
func handleExportSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	from, to, ok := parseSnapshotExportQuery(w, r)
	if !ok {
		return
	}

	snaps, err := s.ExportSnapshots(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export snapshots")
		return
	}

	elements := make([]exportSnapshotElement, len(snaps))
	for i, snap := range snaps {
		elements[i] = exportSnapshotElement{Cursor: snap.Cursor, State: snap.State}
	}
	// A non-nil slice serializes as [] so an empty result is
	// {"snapshots":[],"count":0}, never null.
	raw, err := json.Marshal(exportSnapshotBody{Snapshots: elements, Count: len(elements)})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to encode snapshots")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append(raw, '\n'))
}

// parseSnapshotExportQuery parses the from/to interval parameters, writing the
// documented 400 on an illegal value. from must be a decimal non-negative
// integer and defaults to 0 when absent; to follows the same grammar but, when
// absent, stays nil to mean "no upper bound"; to < from is rejected.
func parseSnapshotExportQuery(w http.ResponseWriter, r *http.Request) (int64, *int64, bool) {
	q := r.URL.Query()

	var from int64
	if raw := q.Get("from"); raw != "" {
		v, ok := parseCursorPath(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "from must be a non-negative integer")
			return 0, nil, false
		}
		from = v
	}

	var to *int64
	if raw := q.Get("to"); raw != "" {
		v, ok := parseCursorPath(raw)
		if !ok {
			writeError(w, http.StatusBadRequest, "to must be a non-negative integer")
			return 0, nil, false
		}
		if v < from {
			writeError(w, http.StatusBadRequest, "to must not be smaller than from")
			return 0, nil, false
		}
		to = &v
	}

	return from, to, true
}

// malformedSnapshotPath reports whether p targets a document's snapshot
// namespace but is at neither of its two exact locations:
//
//	GET/POST /v1/documents/{documentID}/snapshots
//	GET      /v1/documents/{documentID}/snapshots/{cursor}
//
// A trailing slash (an empty terminal segment) or extra segments past the
// cursor is a malformed 400 JSON rather than ServeMux's redirect or
// plain-text 404/405; the collection and the single read never redirect or emit
// HTML. "snapshots" is the keyword only in the segment past the documentID
// (index 1); a document literally named "snapshots" occupies the identifier
// position (index 0) and keeps its ordinary routes. A non-numeric cursor
// segment is left to the single-read handler, which answers 400 itself.
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
		// Keyword position reached: the collection is exactly two segments;
		// the single read adds one non-empty cursor segment.
		switch len(segs) {
		case 2:
			return false
		case 3:
			return segs[2] == ""
		default:
			return true
		}
	}
	return false
}
