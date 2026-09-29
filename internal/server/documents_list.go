package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// documentListEntry is one item of a device's document listing. It carries the
// document id alone and no other field.
type documentListEntry struct {
	DocumentID string `json:"documentId"`
}

// documentListResponse renders the list body with the array before the
// count, a fixed top-level key order a map cannot guarantee.
type documentListResponse struct {
	Documents []documentListEntry `json:"documents"`
	Count     int                 `json:"count"`
}

// handleListDocuments is the read-only listing over a device's document
// collection:
//
//	GET /v1/devices/{deviceId}/documents
//
// It lets a reconnecting client see at a glance which of its documents still
// have changes to resume. The path device id is the caller's only identity —
// no new authentication is introduced — and the request is a bodyless GET.
// Each document the device's sessions once wrote changes to appears at most
// once as one {"documentId":...} item, sorted by document id in ascending
// lexicographic order; limit/offset page that fixed order with the attachment
// listing's exact rules (limit 1..1000 default 100, offset non-negative
// default 0), so pages with no intervening change neither repeat nor skip an
// id and an offset past the end answers an empty array with a zero count.
//
// The body is one compact JSON line plus a trailing newline with the
// documents array and the count in that fixed order, count being this page's
// length. An illegal pagination value is a 400 JSON error checked before the
// device lookup, exactly as on the attachment and session listings; an
// unregistered or deregistered device is a 404 JSON error carrying no listing
// content. The read writes nothing: it neither creates nor deletes documents
// or changes, nor consumes a change cursor or a push subscription. A document
// that was deleted outright, or whose changes were all compacted out of the
// online log, no longer appears, and deregistering the device makes its
// listing answer 404 like a never-registered id.
func handleListDocuments(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	ids, err := s.ListDeviceDocuments(deviceID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list documents")
		}
		return
	}

	entries := make([]documentListEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, documentListEntry{DocumentID: id})
	}
	writeJSON(w, http.StatusOK, documentListResponse{Documents: entries, Count: len(entries)})
}
