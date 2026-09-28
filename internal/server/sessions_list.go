package server

import (
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionListEntry is one row of the device session list. The listing promises
// exactly one field per item, the session id, so the struct carries it alone.
type sessionListEntry struct {
	SessionID string `json:"sessionId"`
}

// sessionListResponse renders the list body with the array before the count, a
// fixed top-level key order a map cannot guarantee. Sessions is always a
// non-nil slice so an empty listing serializes as [] rather than null.
type sessionListResponse struct {
	Sessions []sessionListEntry `json:"sessions"`
	Count    int                `json:"count"`
}

// handleListDeviceSessions is the read-only session listing mounted on the
// device's session collection path:
//
//	GET /v1/devices/{deviceId}/sessions
//
// It answers every session currently owned by the device — nothing else — as
// one compact JSON line plus a trailing newline. Each item gives only the
// session id; ids are ascending in lexicographic (dictionary) order, each
// appears at most once, and limit/offset page over that fixed order, so
// successive pages neither repeat nor skip an id. A page past the end is an
// empty array with count 0, still a 200.
//
// limit follows the attachment listing: an integer in 1..1000, default 100;
// offset is a non-negative integer, default 0. Any illegal value is a 400 JSON
// error decided before the device existence check, so a malformed query against
// a ghost device is still a 400. An unregistered device is a 404 JSON error
// that returns no listing content.
//
// The read is strictly read-only: it creates and deletes no session, moves no
// change cursor and opens no push subscription, so a repeat request and the
// same request after a process restart return a byte-for-byte identical body
// and the read changes no state. Deleted sessions never appear; deregistering
// the device removes its sessions from the listing in the same cascade.
func handleListDeviceSessions(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	// Request shape (the pagination parameters) precedes the device existence
	// verdict: a malformed limit/offset is a 400 even for an unregistered
	// device.
	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	ids, err := s.ListDeviceSessions(deviceID, limit, offset)
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
