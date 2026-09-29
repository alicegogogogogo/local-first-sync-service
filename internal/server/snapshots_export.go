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

	from, to, ok := parseExportInterval(w, r.URL.Query())
	if !ok {
		return
	}

	snaps, err := s.ExportSnapshots(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export snapshots")
		return
	}
	writeSnapshotExport(w, snaps)
}

// writeSnapshotExport renders an export result in the shared response shape: a
// single compact JSON line with the snapshots,count key order, each item
// keyed cursor,state, an empty export serializing as [] with count 0. It is
// shared by the document-scoped and session-scoped exports so their bodies
// cannot drift apart.
func writeSnapshotExport(w http.ResponseWriter, snaps []events.ExportedSnapshot) {
	items := make([]snapshotExportItem, 0, len(snaps))
	for _, snap := range snaps {
		items = append(items, snapshotExportItem{Cursor: snap.Cursor, State: snap.State})
	}
	writeJSON(w, http.StatusOK, snapshotExportResponse{Snapshots: items, Count: len(items)})
}

// handleSessionExportSnapshots is the session-scoped view of the snapshot
// batch export, over the session document-read prefix with the resource
// segment swapped to the snapshot collection:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/snapshots?from=0&to=N
//
// The interval semantics and the response body are identical to the
// document-level export; the path offers no other read form, so every GET is
// the export (from/to absent means the default interval). Around the export
// sit the session view's checks in their fixed order — request shape and
// parameters (400), session existence (404), the session device's document
// permission (403) — so a rejected export exposes no snapshot content and
// writes nothing.
func handleSessionExportSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Malformed interval parameters are a request-shape error (400) checked
	// before the resource lookup (404), matching the session change export's
	// ordering.
	from, to, ok := parseExportInterval(w, r.URL.Query())
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
	writeSnapshotExport(w, snaps)
}

// malformedSnapshotPath reports whether p targets the snapshot namespace but
// is not at one of its exact locations:
//
//	GET  /v1/documents/{documentID}/snapshots                    (the batch export)
//	GET  /v1/documents/{documentID}/snapshots/{cursor}          (the single read)
//	POST /v1/documents/{documentID}/snapshots/prune             (retention pruning)
//	*    /v1/documents/{documentID}/snapshots/versions          (named versions)
//	*    /v1/documents/{documentID}/snapshots/versions/{name}
//	POST /v1/documents/{documentID}/snapshots/versions/{name}/restore
//
// A trailing slash (an empty cursor or name segment, including the collection
// path with a trailing slash), extra path segments, or a "snapshots" segment
// in any keyword position short of one of those shapes is a malformed 400
// rather than ServeMux's redirect or plain-text 404/405: the snapshot
// endpoints promise a JSON error and never a redirect. Empty segments are
// already rejected by the guard itself. The keyword is matched only past the
// documentID position, so a document literally named "snapshots" keeps its
// ordinary routes.
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
		// segment; the prune adds the literal prune segment; the named-version
		// subtree adds the versions collection, one non-empty name segment, and
		// optionally a trailing restore segment. Anything else (trailing slash,
		// missing/extra segments) is malformed.
		collection := len(segs) == 2 && segs[0] != ""
		cursorItem := len(segs) == 3 && segs[0] != "" && segs[2] != ""
		prune := len(segs) == 3 && segs[0] != "" && segs[2] == "prune"
		versionsCollection := len(segs) == 3 && segs[0] != "" && segs[2] == "versions"
		versionsItem := len(segs) == 4 && segs[0] != "" && segs[2] == "versions" && segs[3] != ""
		versionsRestore := len(segs) == 5 && segs[0] != "" && segs[2] == "versions" && segs[3] != "" && segs[4] == "restore"
		return !(collection || cursorItem || prune || versionsCollection || versionsItem || versionsRestore)
	}
	return false
}

// inSessionSnapshotVersionsSubtree reports whether segs (the path split past
// "/v1/sessions/") sits in the named-snapshot-version subtree: a "snapshots"
// collection segment followed by a "versions" segment. The exact shape verdict
// (collection, item, item/restore) belongs to malformedSessionSnapshotsPath;
// every other session keyword guard yields to it so a version name that
// coincides with a change keyword ("poll", "merge", "restore", "subscribe",
// "compact", "query") keeps its version routes rather than being judged as a
// malformed change path.
func inSessionSnapshotVersionsSubtree(segs []string) bool {
	return len(segs) >= 5 &&
		segs[1] == "documents" &&
		segs[3] == "snapshots" &&
		segs[4] == "versions"
}

// malformedSessionSnapshotsPath reports whether p targets the session-scoped
// snapshot namespace but is not at one of its exact locations:
//
//	GET    /v1/sessions/{sessionId}/documents/{documentId}/snapshots                       (batch export)
//	POST   /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions              (register)
//	GET    /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions              (list)
//	GET    /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}       (read)
//	PUT    /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}       (rename)
//	DELETE /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}       (delete)
//	POST   /v1/sessions/{sessionId}/documents/{documentId}/snapshots/versions/{name}/restore
//
// extra path segments past any of those shapes (the batch export has no
// single-snapshot item, so a trailing cursor segment is malformed too) are a
// JSON 400 rather than ServeMux's plain-text 404 or a redirect. Empty segments
// (doubled slashes, a trailing slash) are already rejected by the guard itself.
//
// "snapshots" is treated as the collection keyword only in the fourth
// segment, right after the document identifier; a session or document
// identifier literally named "snapshots" occupies an identifier position and
// keeps its ordinary routes.
func malformedSessionSnapshotsPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "snapshots" there is an ordinary identifier, not
	// this collection's keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	if len(segs) < 4 || segs[1] != "documents" || segs[3] != "snapshots" {
		return false
	}
	nonEmptyIDs := segs[0] != "" && segs[2] != ""
	// Keyword position reached. The batch export is exactly the collection;
	// the named-version subtree adds the versions collection, one non-empty
	// name segment, and optionally a trailing restore segment. Anything past
	// one of those shapes (a trailing cursor on the export, an extra segment,
	// a trailing keyword other than restore) is a malformed 400.
	collection := len(segs) == 4
	versionsCollection := len(segs) == 5 && segs[4] == "versions"
	versionsItem := len(segs) == 6 && segs[4] == "versions" && segs[5] != ""
	versionsRestore := len(segs) == 7 && segs[4] == "versions" && segs[5] != "" && segs[6] == "restore"
	return !nonEmptyIDs || !(collection || versionsCollection || versionsItem || versionsRestore)
}
