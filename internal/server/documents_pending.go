package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// pendingChangeEntry is one item of a device's pending-change statistics. It
// carries the document id, the number of that device's changes still in the
// online log and the largest cursor among them, in that fixed key order.
type pendingChangeEntry struct {
	DocumentID  string `json:"documentId"`
	ChangeCount int64  `json:"changeCount"`
	MaxCursor   int64  `json:"maxCursor"`
}

// pendingChangesResponse renders the statistics body with the documents array
// before the count, the fixed top-level key order a map cannot guarantee.
type pendingChangesResponse struct {
	Documents []pendingChangeEntry `json:"documents"`
	Count     int                  `json:"count"`
}

// handleListPendingChanges is the read-only pending-change statistics over a
// device's document collection, one pending segment below the document
// listing:
//
//	GET /v1/devices/{deviceId}/documents/pending
//
// It lets a reconnecting client see at a glance how many online changes each
// of its documents still has to resume. The path device id is the caller's
// only identity — no new authentication is introduced — and the request is a
// bodyless GET. The statistics use the document listing's exact scope: each
// document the device's sessions once wrote changes to and whose changes are
// still in the online log appears at most once as one
// {"documentId","changeCount","maxCursor"} item, sorted by document id in
// ascending lexicographic order. changeCount counts only the device's
// changes still in the online log and maxCursor is the largest cursor among
// them; retained compaction summaries never count. limit/offset page that
// fixed order with the document listing's exact rules (limit 1..1000 default
// 100, offset non-negative default 0), so pages with no intervening change
// neither repeat nor skip an id and an offset past the end answers an empty
// array with a zero count.
//
// The body is one compact JSON line plus a trailing newline with the
// documents array and the count in that fixed order, count being this page's
// length. An illegal pagination value is a 400 JSON error checked before the
// device lookup, exactly as on the document listing; an unregistered or
// deregistered device is a 404 JSON error carrying no statistics content. The
// read writes nothing: it neither creates nor deletes documents or changes,
// nor consumes a change cursor or a push subscription. A document that was
// deleted outright, or whose changes were all compacted out of the online
// log, no longer appears, and deregistering the device makes its statistics
// answer 404 like a never-registered id.
func handleListPendingChanges(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	stats, err := s.ListDevicePendingChanges(deviceID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list pending changes")
		}
		return
	}

	entries := make([]pendingChangeEntry, 0, len(stats))
	for _, stat := range stats {
		entries = append(entries, pendingChangeEntry{
			DocumentID:  stat.DocumentID,
			ChangeCount: stat.Count,
			MaxCursor:   stat.MaxCursor,
		})
	}
	writeJSON(w, http.StatusOK, pendingChangesResponse{Documents: entries, Count: len(entries)})
}
