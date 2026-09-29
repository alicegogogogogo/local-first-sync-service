package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// documentPendingEntry is one item of a device's pending-change statistics. It
// carries the document id, the count of changes still in the online log and
// the largest cursor among them, in that fixed key order.
type documentPendingEntry struct {
	DocumentID  string `json:"documentId"`
	ChangeCount int64  `json:"changeCount"`
	MaxCursor   int64  `json:"maxCursor"`
}

// documentPendingResponse renders the statistics body with the array before
// the count, a fixed top-level key order a map cannot guarantee.
type documentPendingResponse struct {
	Pending []documentPendingEntry `json:"pending"`
	Count   int                    `json:"count"`
}

// handleListPending is the read-only pending-change statistics over a device's
// document collection:
//
//	GET /v1/devices/{deviceId}/documents/pending
//
// It lets a reconnecting client see at a glance how many online changes it
// still has outstanding per document. The path device id is the caller's only
// identity — no new authentication is introduced — and the request is a
// bodyless GET. Every document that currently carries an online change the
// device originated appears at most once as one
// {"documentId":...,"changeCount":...,"maxCursor":...} item: the count and the
// maximum cursor count only changes still retained in the online log, so a
// document deleted outright or fully compacted out no longer appears. Items
// are sorted by document id in ascending lexicographic order; limit/offset
// page that fixed order with the document listing's exact rules (limit
// 1..1000 default 100, offset non-negative default 0), so pages with no
// intervening change neither repeat nor skip an id and an offset past the end
// answers an empty array with a zero count.
//
// The body is one compact JSON line plus a trailing newline with the pending
// array and the count in that fixed order, count being this page's length. An
// illegal pagination value is a 400 JSON error checked before the device
// lookup, exactly as on the document listing; an unregistered or deregistered
// device is a 404 JSON error carrying no statistics content. The read writes
// nothing: it neither creates nor deletes documents or changes, nor consumes a
// change cursor or a push subscription.
func handleListPending(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	stats, err := s.ListDevicePending(deviceID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list pending changes")
		}
		return
	}

	entries := make([]documentPendingEntry, 0, len(stats))
	for _, row := range stats {
		entries = append(entries, documentPendingEntry{
			DocumentID:  row.DocumentID,
			ChangeCount: row.ChangeCount,
			MaxCursor:   row.MaxCursor,
		})
	}
	writeJSON(w, http.StatusOK, documentPendingResponse{Pending: entries, Count: len(entries)})
}
