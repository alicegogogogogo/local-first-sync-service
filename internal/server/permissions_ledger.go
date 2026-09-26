package server

import (
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// permissionLedgerEntry is one row of a document's permission ledger. The
// struct field order fixes the on-the-wire key order to deviceId then
// authorized, matching the write endpoint's verdict fields.
type permissionLedgerEntry struct {
	DeviceID   string `json:"deviceId"`
	Authorized bool   `json:"authorized"`
}

// permissionLedgerResponse is the ledger body with the top-level key order
// fixed to permissions then count. Permissions is always a non-nil slice so an
// empty ledger serializes as [] rather than null.
type permissionLedgerResponse struct {
	Permissions []permissionLedgerEntry `json:"permissions"`
	Count       int                     `json:"count"`
}

// handleListPermissions is the read-only ledger query mounted on the existing
// document-permission path:
//
//	GET /v1/documents/{documentID}/permissions?limit=N&offset=M
//
// It answers one entry per currently registered device — deviceId plus the
// boolean authorized — in ascending lexicographic device-id order, paged with
// the attachment listing's exact limit (1..1000, default 100) and offset
// (non-negative, default 0) semantics. A device with no ledger row starts
// authorized, so it lists authorized=true; a revoked device lists false; each
// device appears at most once and a deregistered device appears never. An
// unknown document succeeds exactly like an empty one: an empty array and
// count 0 are a normal 200, not an error.
//
// The read is strictly read-only: it creates no permission row, changes no
// authorization state, moves no change cursor and notifies the subscription
// registries, so a revoked session still reads 403 and a live subscription
// still ends with 4403 exactly as if the query never happened. Pagination
// parameters are validated before anything else.
func handleListPermissions(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	entries, err := s.ListDocumentPermissions(documentID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list permissions")
		return
	}

	items := make([]permissionLedgerEntry, 0, len(entries))
	for _, entry := range entries {
		items = append(items, permissionLedgerEntry{
			DeviceID:   entry.DeviceID,
			Authorized: entry.Authorized,
		})
	}
	writeJSON(w, http.StatusOK, permissionLedgerResponse{Permissions: items, Count: len(items)})
}

// malformedPermissionPath reports whether p targets the document-permission
// resource but is not at its exact location
// (/v1/documents/{documentID}/permissions): an empty documentID segment, a
// missing documentID segment (the bare /v1/documents/permissions), a trailing
// slash (an empty extra segment), or any segment past the resource is a
// malformed 400 rather than ServeMux's redirect or plain-text 404/405 — the
// permission surface promises a JSON error and never a redirect, and an extra
// segment past the path answers 400, not 404. Empty segments are already
// rejected by the guard itself. The keyword in the documentID position is only
// treated as the endpoint word when nothing follows it; a document literally
// named "permissions" keeps its ordinary changes/merge/snapshot routes.
func malformedPermissionPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "permissions" {
			continue
		}
		if i == 0 {
			// The keyword sits where the document id belongs. Bare, or with a
			// trailing slash, the documentID segment is missing; with a real
			// suffix it is an ordinary document named "permissions".
			return len(segs) == 1 || (len(segs) == 2 && segs[1] == "")
		}
		// Keyword position reached. The resource is exactly
		// {documentID}/permissions; a trailing slash or any further segment is
		// malformed.
		valid := len(segs) == 2 && segs[0] != ""
		return !valid
	}
	return false
}
