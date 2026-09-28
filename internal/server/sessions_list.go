package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionListEntry is one item of a device's session listing. It carries the
// session id alone and no other field.
type sessionListEntry struct {
	SessionID string `json:"sessionId"`
}

// sessionListResponse renders the list body with the array before the count,
// a fixed top-level key order a map cannot guarantee.
type sessionListResponse struct {
	Sessions []sessionListEntry `json:"sessions"`
	Count    int                `json:"count"`
}

// handleListSessions is the read-only listing over a device's session
// collection:
//
//	GET /v1/devices/{deviceId}/sessions
//
// It lets a client browse every live session it owns after reconnecting. The
// path device id is the caller's only identity — no new authentication is
// introduced — and the request is a bodyless GET. Each live session appears at
// most once as one {"sessionId":...} item, sorted by session id in ascending
// lexicographic order; limit/offset page that fixed order with the attachment
// listing's exact rules (limit 1..1000 default 100, offset non-negative
// default 0), so pages with no intervening change neither repeat nor skip an
// id and an offset past the end answers an empty array with a zero count.
//
// The body is one compact JSON line plus a trailing newline with the sessions
// array and the count in that fixed order, count being this page's length.
// An illegal pagination value is a 400 JSON error checked before the device
// lookup, exactly as on the attachment listing; an unregistered or
// deregistered device is a 404 JSON error carrying no listing content. The
// read writes nothing: it neither creates nor deletes sessions, nor consumes a
// change cursor or a push subscription; deleted sessions are absent and
// deregistering the device removes every session it owns from the listing.
func handleListSessions(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	ids, err := s.ListSessions(deviceID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list sessions")
		}
		return
	}

	entries := make([]sessionListEntry, 0, len(ids))
	for _, id := range ids {
		entries = append(entries, sessionListEntry{SessionID: id})
	}
	writeJSON(w, http.StatusOK, sessionListResponse{Sessions: entries, Count: len(entries)})
}
