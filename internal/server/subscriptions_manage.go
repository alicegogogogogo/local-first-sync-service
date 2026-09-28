package server

import (
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// subscriptionListEntry is one row of a device's live-subscription view. The
// struct field order fixes the on-the-wire key order: subscriptionId,
// documentId, channel, cursor, establishedAt.
type subscriptionListEntry struct {
	SubscriptionID string `json:"subscriptionId"`
	DocumentID     string `json:"documentId"`
	Channel        string `json:"channel"`
	Cursor         int64  `json:"cursor"`
	EstablishedAt  int64  `json:"establishedAt"`
}

// subscriptionListResponse is the list body with the top-level key order
// fixed to subscriptions then count. Subscriptions is always a non-nil slice,
// so a device with no live connection serializes as [] rather than null; an
// empty view is a normal 200, not an error.
type subscriptionListResponse struct {
	Subscriptions []subscriptionListEntry `json:"subscriptions"`
	Count         int                     `json:"count"`
}

// subscriptionCancelResponse is the cancellation body; the key order is
// subscriptionId then deleted.
type subscriptionCancelResponse struct {
	SubscriptionID string `json:"subscriptionId"`
	Deleted        bool   `json:"deleted"`
}

// handleListSubscriptions is the read-only live-subscription view mounted
// under the device resource:
//
//	GET /v1/devices/{deviceId}/subscriptions
//
// The path device id is the caller's only identity and no new authentication
// is introduced. It answers one compact single-line JSON object (plus a
// trailing newline) listing every currently live push connection the device
// owns — both the document-level subscriptions it declared and the
// subscriptions opened through sessions it owns, across the change-log channel
// and the CRDT-state channel.
//
// Each entry gives the server-allocated subscriptionId, the documentId, the
// channel kind ("changes" or "state"), the starting cursor requested in the
// handshake (0 for a state subscription) and the establishment time as a Unix
// millisecond integer, in that fixed key order. Entries are ascending by
// connection establishment and count equals the array length. The view is
// purely in-memory: a client disconnect or a process restart removes an entry
// immediately, nothing is persisted and no change cursor is consumed. A device
// without any live subscription still answers 200 with an empty array and
// count 0.
//
// The checks run in the fixed order request shape (400 for an empty
// identifier, a missing or extra path segment, or a method mismatch — no
// redirect, no HTML), then device existence (404 for an unregistered caller,
// exposing no subscription content). The read writes nothing.
func handleListSubscriptions(s *app.App, w http.ResponseWriter, r *http.Request) {
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

	live := s.ListPushSubscriptions(deviceID)
	entries := make([]subscriptionListEntry, 0, len(live))
	for _, sub := range live {
		entries = append(entries, subscriptionListEntry{
			SubscriptionID: sub.ID,
			DocumentID:     sub.DocumentID,
			Channel:        sub.Channel,
			Cursor:         sub.Cursor,
			EstablishedAt:  sub.EstablishedAt.UnixMilli(),
		})
	}
	writeJSON(w, http.StatusOK, subscriptionListResponse{Subscriptions: entries, Count: len(entries)})
}

// handleCancelSubscription actively ends one of the path device's live push
// connections:
//
//	DELETE /v1/devices/{deviceId}/subscriptions/{subscriptionId}
//
// Success answers one compact single-line JSON object (plus a trailing
// newline) of {"subscriptionId","deleted":true}; the target connection is then
// ended with close code 4410 and pushes stop, while every other subscriber of
// the same document is untouched. The checks run in the fixed order request
// shape (400), device existence (404) and subscription ownership (404): a
// subscription that does not exist, has already ended, belongs to another
// device, or is canceled a second time is a 404 JSON error indistinguishable
// from a missing one, leaking no subscription content. No failure writes
// anything; failures are never redirected and never render HTML. A permission
// revoke still ends its connections with 4403 and the termination signal with
// 1001 — this entry only ever produces 4410 on the one target connection.
func handleCancelSubscription(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")             // route pattern + guard guarantee non-empty
	subscriptionID := r.PathValue("subscriptionId") // route pattern + guard guarantee non-empty

	// Device existence precedes the subscription lookup, so an unregistered
	// caller learns nothing about any subscription id.
	exists, err := s.DeviceExists(deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up device")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	// A missing id, an already-ended connection and another device's
	// connection share the same 404 verdict.
	if !s.CancelPushSubscription(deviceID, subscriptionID) {
		writeError(w, http.StatusNotFound, "subscription not found")
		return
	}

	writeJSON(w, http.StatusOK, subscriptionCancelResponse{SubscriptionID: subscriptionID, Deleted: true})
}
