package server

import (
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// subscriptionEntry is one row of the device subscription list. The JSON key
// order is the contract's fixed order: subscription id, document id, channel
// kind, handshake starting cursor, establishment time.
type subscriptionEntry struct {
	SubscriptionID string `json:"subscriptionId"`
	DocumentID     string `json:"documentId"`
	Kind           string `json:"kind"`
	Cursor         int64  `json:"cursor"`
	EstablishedAt  int64  `json:"establishedAt"`
}

// subscriptionListResponse renders the list body with the array before the
// count, a fixed top-level key order a map cannot guarantee.
type subscriptionListResponse struct {
	Subscriptions []subscriptionEntry `json:"subscriptions"`
	Count         int                 `json:"count"`
}

// subscriptionCancelResponse is the one-line DELETE success body.
type subscriptionCancelResponse struct {
	SubscriptionID string `json:"subscriptionId"`
	Deleted        bool   `json:"deleted"`
}

// handleListDeviceSubscriptions is the read-only subscription management
// entry:
//
//	GET /v1/devices/{deviceId}/subscriptions
//
// The path device id is the caller's only identity; no new authentication is
// introduced. It answers every live push connection owned by the device —
// both the document-level subscriptions the device declared itself and the
// subscriptions opened through sessions that belong to it, across both the
// change channel and the CRDT state channel — as one compact JSON line plus a
// trailing newline, ordered by connection establishment (oldest first). Each
// row names the subscription id, document id, channel kind ("changes" or
// "state"), the starting cursor requested at the handshake (0 for a state
// subscription) and the establishment time as Unix milliseconds.
//
// The view is memory only: entries vanish the moment the client disconnects
// or the process restarts, and listing writes nothing and consumes no change
// cursor. A device with no live subscription still answers 200 with an empty
// array and a zero count. An unregistered calling device answers 404 JSON; a
// malformed path or wrong method answers 400 JSON before any device lookup.
func handleListDeviceSubscriptions(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	exists, err := s.DeviceExists(deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up device")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	infos := s.ListPushSubscriptions(deviceID)
	entries := make([]subscriptionEntry, 0, len(infos))
	for _, info := range infos {
		entries = append(entries, subscriptionEntry{
			SubscriptionID: info.ID,
			DocumentID:     info.DocumentID,
			Kind:           string(info.Kind),
			Cursor:         info.Cursor,
			EstablishedAt:  info.EstablishedAt,
		})
	}
	writeJSON(w, http.StatusOK, subscriptionListResponse{Subscriptions: entries, Count: len(entries)})
}

// handleCancelDeviceSubscription is the active-recycling management entry:
//
//	DELETE /v1/devices/{deviceId}/subscriptions/{subscriptionId}
//
// It ends exactly the one named live connection owned by the path device: the
// success body is one JSON line naming the subscription id and the deletion
// marker, and the target connection then closes with code 4410 and stops
// pushing. Other subscribers of the same document are untouched. The checks
// run in the fixed order request shape (400), device existence (404),
// subscription ownership (404): an unknown subscription, one that already
// ended, and one owned by another device are the same 404 JSON error that
// leaks no subscription content. A repeated cancel answers 404 again and
// changes nothing; a failure writes nothing, never redirects and never emits
// HTML.
func handleCancelDeviceSubscription(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")             // route pattern + guard guarantee non-empty
	subscriptionID := r.PathValue("subscriptionId") // route pattern + guard guarantee non-empty

	exists, err := s.DeviceExists(deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up device")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	if !s.CancelPushSubscription(deviceID, subscriptionID) {
		// No detail about why: unknown id, already disconnected and another
		// device's subscription are indistinguishable.
		writeError(w, http.StatusNotFound, "subscription not found")
		return
	}

	writeJSON(w, http.StatusOK, subscriptionCancelResponse{SubscriptionID: subscriptionID, Deleted: true})
}
