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

	writeSnapshotExport(w, snapshotExportItems(snaps))
}

// writeSnapshotExport renders an export result in the shared response shape: a
// single compact JSON line with the snapshots,count key order, an empty export
// serializing as [] with count 0. It is shared by the document-scoped and
// session-scoped exports so their bodies cannot drift apart.
func writeSnapshotExport(w http.ResponseWriter, snaps []snapshotExportItem) {
	if snaps == nil {
		snaps = make([]snapshotExportItem, 0)
	}
	writeJSON(w, http.StatusOK, snapshotExportResponse{Snapshots: snaps, Count: len(snaps)})
}

// snapshotExportItems converts the event service's export rows into the
// on-the-wire items, preserving each stored state verbatim.
func snapshotExportItems(rows []events.ExportedSnapshot) []snapshotExportItem {
	items := make([]snapshotExportItem, 0, len(rows))
	for _, snap := range rows {
		items = append(items, snapshotExportItem{Cursor: snap.Cursor, State: snap.State})
	}
	return items
}

// handleSessionExportSnapshots is the session-scoped view of the snapshot
// batch export, sharing the session document read's collection prefix with the
// resource segment replaced by the snapshot collection:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots?from=0&to=N
//
// The interval semantics and the response body are identical to the
// document-level export; around them sit the session view's checks in their
// fixed order — request shape and parameters (400), session existence (404),
// the session device's document permission (403) — so a rejected export
// exposes no snapshot content and writes nothing. The path offers only this
// export: a request carrying neither from nor to has no other read form and
// is a request-shape 400.
func handleSessionExportSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	q := r.URL.Query()

	// The collection offers only the export, so the from/to marker is part of
	// the request shape; checked, like the interval values, before the
	// resource lookup (404).
	if !exportRequested(q) {
		writeError(w, http.StatusBadRequest, "snapshot export requires a from or to query parameter")
		return
	}

	// Malformed interval parameters are a request-shape error (400) checked
	// before the resource lookup (404), matching the session change export's
	// ordering.
	from, to, ok := parseExportInterval(w, q)
	if !ok {
		return
	}

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

	snaps, err := s.ExportSnapshots(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export snapshots")
		return
	}
	writeSnapshotExport(w, snapshotExportItems(snaps))
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

// malformedSessionSnapshotsPath reports whether p targets the session-scoped
// snapshot collection's shape but is not the collection's exact location:
//
//	/v1/sessions/{sessionId}/documents/{documentId}/snapshots
//
// Extra path segments past the collection are malformed: the interval export
// is this collection's only read shape, so (unlike the change collection) no
// terminal subresource is exempt. ServeMux would answer an unrecognized suffix
// with a plain-text 404; the session snapshot surface promises a JSON 400 and
// never a redirect. Empty segments (doubled slashes, a trailing slash) are
// already rejected by the guard itself.
//
// "snapshots" is treated as the collection keyword only in the fourth segment
// (index 3), right after the document identifier; a session or document
// identifier literally named "snapshots" occupies an identifier position
// (index 0 or 2) and keeps its ordinary routes.
func malformedSessionSnapshotsPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; a "snapshots" segment there is an ordinary segment,
	// not this collection's keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	if len(segs) < 4 || segs[1] != "documents" || segs[3] != "snapshots" {
		return false
	}
	// Keyword position reached. The collection is exactly
	// {sessionId}/documents/{documentId}/snapshots; anything past the keyword
	// (a trailing slash, a missing/extra segment) is a malformed 400.
	collection := len(segs) == 4 && segs[0] != "" && segs[2] != ""
	return !collection
}
