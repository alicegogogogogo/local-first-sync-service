package server

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
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

// exportRequested reports whether the query carries the export's from/to
// parameters. A present key counts even with an empty value (?from= is the
// export with the default bound), matching the document-level collection.
func exportRequested(q url.Values) bool {
	if _, ok := q["from"]; ok {
		return true
	}
	_, ok := q["to"]
	return ok
}

// parseExportInterval parses the shared from/to export parameters, writing
// the documented 400 on an illegal value. from defaults to 0; a nil to means
// the interval has no upper bound; to below from is rejected. It is shared by
// the document-scoped and session-scoped exports so their query semantics
// cannot drift apart.
func parseExportInterval(w http.ResponseWriter, q url.Values) (from int64, to *int64, ok bool) {
	from = 0
	if raw := q.Get("from"); raw != "" {
		v, valid := parseCursorPath(raw)
		if !valid {
			writeError(w, http.StatusBadRequest, "from must be a non-negative integer")
			return 0, nil, false
		}
		from = v
	}

	if raw := q.Get("to"); raw != "" {
		v, valid := parseCursorPath(raw)
		if !valid {
			writeError(w, http.StatusBadRequest, "to must be a non-negative integer")
			return 0, nil, false
		}
		to = &v
	}
	if to != nil && *to < from {
		writeError(w, http.StatusBadRequest, "to must not be less than from")
		return 0, nil, false
	}
	return from, to, true
}

// writeChangeExport renders an export result in the shared response shape: a
// single compact JSON line with the changes,count key order, an empty export
// serializing as [] with count 0.
func writeChangeExport(w http.ResponseWriter, rows []events.ListedChange) {
	if rows == nil {
		rows = make([]events.ListedChange, 0)
	}
	writeJSON(w, http.StatusOK, changeExportResponse{Changes: rows, Count: len(rows)})
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

	from, to, ok := parseExportInterval(w, r.URL.Query())
	if !ok {
		return
	}

	rows, err := s.ExportChanges(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export changes")
		return
	}
	writeChangeExport(w, rows)
}

// handleSessionExportChanges is the session-scoped view of the change-log
// batch export, sharing the session change read's exact collection path:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/changes?from=0&to=N
//
// The interval semantics and the response body are identical to the
// document-level export; around them sit the session view's checks in their
// fixed order — request shape and parameters (400), session existence (404),
// the session device's document permission (403) — so a rejected export
// exposes no change content and writes nothing.
func handleSessionExportChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Malformed interval parameters are a request-shape error (400) checked
	// before the resource lookup (404), matching the paged read's ordering.
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

	rows, err := s.ExportChanges(documentID, from, to)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to export changes")
		return
	}
	writeChangeExport(w, rows)
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
		// endpoints and keep their own guards; the document-level subscription
		// adds one trailing "subscribe" segment and keeps its own endpoint.
		// Anything else (a trailing slash, an unrecognized extra segment) is a
		// malformed 400.
		if i == 1 {
			collection := len(segs) == 2 && segs[0] != ""
			knownSubresource := len(segs) >= 3 &&
				(segs[2] == "poll" || segs[2] == "compact" || segs[2] == "subscribe" || segs[2] == "query" || segs[2] == "status")
			return !(collection || knownSubresource)
		}
		return false
	}
	return false
}

// malformedSessionChangesPath reports whether p targets the session-scoped
// change collection's shape but is not the collection's exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes): extra path
// segments past the collection. The paged read and the interval export share
// that exact collection path; the subscribe subresource has its own endpoint
// and keyword guard, so only a genuinely unrecognized suffix reaches this
// check. ServeMux would answer it with a plain-text 404; the change surface
// promises a JSON 400 and never a redirect. Empty segments (doubled slashes,
// a trailing slash) are already rejected by the guard itself.
//
// "changes" is treated as the collection keyword only in the fourth segment,
// right after the document identifier; a session or document identifier
// literally named "changes" occupies an identifier position and keeps its
// ordinary routes.
func malformedSessionChangesPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "changes" there is an ordinary identifier, not this
	// collection's keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	if len(segs) < 4 || segs[1] != "documents" || segs[3] != "changes" {
		return false
	}
	// Keyword position reached. The collection is exactly
	// {sessionId}/documents/{documentId}/changes; the subscribe subresource
	// adds one trailing "subscribe" segment and keeps its own guard; the long
	// poll adds one trailing "poll" segment and keeps its own guard; the
	// offline replay adds one trailing "replay" segment and keeps its own
	// guard; the single-change merge adds one trailing "merge" segment and
	// keeps its own guard; the change-log compaction adds one trailing
	// "compact" segment and keeps its own guard; the sync checkpoint adds one
	// trailing "checkpoint" segment and keeps its own guard; the batch lookup
	// adds one trailing "query" segment and keeps its own guard. Anything else
	// past the keyword is a malformed 400.
	collection := len(segs) == 4 && segs[0] != "" && segs[2] != ""
	subscribe := len(segs) == 5 && segs[4] == "subscribe"
	poll := len(segs) == 5 && segs[4] == "poll"
	replay := len(segs) == 5 && segs[4] == "replay"
	merge := len(segs) == 5 && segs[4] == "merge"
	compact := len(segs) == 5 && segs[4] == "compact"
	checkpoint := len(segs) == 5 && segs[4] == "checkpoint"
	query := len(segs) == 5 && segs[4] == "query"
	return !(collection || subscribe || poll || replay || merge || compact || checkpoint || query)
}

// malformedSessionCompactPath reports whether p targets the session-scoped
// change-log compaction endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/compact): a missing
// "documents"/"changes" segment, an extra segment, or a "compact" segment in a
// position short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "compact" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "compact" occupies an identifier position (index 0 or 2) and
// is therefore left to the ordinary changes routes like any other id.
func malformedSessionCompactPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "compact" there is the CRDT compaction keyword, not
	// the change-log one.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict through
	// malformedSessionSnapshotsPath; a version name coinciding with "compact"
	// is an ordinary identifier there, not this keyword.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "compact" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "compact" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly
		// "compact" with the documents/changes scaffolding around it, and no
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

// malformedSessionPollPath reports whether p targets the session-scoped
// long-poll endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/poll): a missing
// "documents"/"changes" segment, an extra segment, or a "poll" segment in a
// position short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "poll" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "poll" occupies an identifier position (index 0 or 2) and is
// therefore left to the ordinary changes routes like any other id.
func malformedSessionPollPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "poll" there is an ordinary identifier, not the
	// changes-poll keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict; a
	// version name coinciding with "poll" is an ordinary identifier there.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "poll" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "poll" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly "poll"
		// with the documents/changes scaffolding around it, and no segment may
		// follow. Any other occurrence is a malformed path.
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

// malformedSessionMergePath reports whether p targets the session-scoped
// single-change merge endpoint but is not at its exact location
// (/v1/sessions/{sessionId}/documents/{documentId}/changes/merge): a missing
// "documents"/"changes" segment, an extra segment, or a "merge" segment in a
// position short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "merge" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "merge" occupies an identifier position (index 0 or 2) and is
// therefore left to the ordinary changes routes like any other id.
func malformedSessionMergePath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "merge" there is an ordinary identifier, not the
	// changes-merge keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict; a
	// version name coinciding with "merge" is an ordinary identifier there.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "merge" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "merge" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly "merge"
		// with the documents/changes scaffolding around it, and no segment may
		// follow. Any other occurrence is a malformed path.
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

// (/v1/sessions/{sessionId}/documents/{documentId}/changes/replay): a missing
// "documents"/"changes" segment, an extra segment, or a "replay" segment in a
// position short of the registered shape. ServeMux would answer those with a
// plain-text 404 (or a redirect for an empty segment); every failure of this
// endpoint must be a JSON 400 instead. Empty segments are already rejected by
// the guard itself.
//
// "replay" is treated as the endpoint keyword only in its terminal segment
// position (the fifth segment, index 4); a session or document identifier
// literally named "replay" occupies an identifier position (index 0 or 2) and
// is therefore left to the ordinary changes routes like any other id.
func malformedSessionReplayPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/sessions/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	// The CRDT namespace ("crdt" immediately past the document identifier)
	// has its own guard; "replay" there is an ordinary identifier, not the
	// changes-replay keyword.
	if len(segs) >= 4 && segs[1] == "documents" && segs[3] == "crdt" {
		return false
	}
	// The named-snapshot-version subtree owns its whole shape verdict; a
	// version name coinciding with "replay" is an ordinary identifier there.
	if inSessionSnapshotVersionsSubtree(segs) {
		return false
	}
	for i, seg := range segs {
		if seg != "replay" {
			continue
		}
		// Identifier positions: sessionId (0) and documentId (2). A value of
		// "replay" there is an ordinary identifier, not the endpoint word.
		if i == 0 || i == 2 {
			continue
		}
		// Endpoint keyword position: the fifth segment must be exactly
		// "replay" with the documents/changes scaffolding around it, and no
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
