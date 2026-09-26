package server

import (
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// permissionLedgerItem is one element of a permission ledger body. The struct
// field order fixes the on-the-wire key order to deviceId then authorized.
type permissionLedgerItem struct {
	DeviceID   string `json:"deviceId"`
	Authorized bool   `json:"authorized"`
}

// permissionLedgerResponse is the ledger body with the top-level key order
// fixed to permissions then count. Permissions is always a non-nil slice so
// an empty ledger serializes as [] rather than null.
type permissionLedgerResponse struct {
	Permissions []permissionLedgerItem `json:"permissions"`
	Count       int                    `json:"count"`
}

// handleListPermissions is the read-only ledger query over a document's
// permission collection:
//
//	GET /v1/documents/{documentID}/permissions?limit=N&offset=M
//
// Every currently registered device appears exactly once, ordered by device
// id ascending, with its present authorization: devices start authorized, a
// committed revoke reports false and a grant reports true again. A
// deregistered device never appears; an unknown document is a successful
// report of every registered device as authorized, never an error.
// Pagination shares the attachment listing's limit/offset parsing (limit
// 1..1000, default 100; offset non-negative, default 0), and the page is a
// slice of the same lexicographic order, so paging never duplicates or drops
// an entry. The handler writes nothing: it neither grants nor revokes, moves
// no cursor, ends no session and notifies the push channels; revokes still
// drive the existing 403/4403 verdicts through the write path alone.
func handleListPermissions(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	entries, err := s.ListDocumentPermissions(documentID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list document permissions")
		return
	}

	items := make([]permissionLedgerItem, 0, len(entries))
	for _, entry := range entries {
		items = append(items, permissionLedgerItem{
			DeviceID:   entry.DeviceID,
			Authorized: entry.Authorized,
		})
	}
	writeJSON(w, http.StatusOK, permissionLedgerResponse{Permissions: items, Count: len(items)})
}

// malformedPermissionPath reports whether p targets the permission
// collection but is not at its exact location
// (/v1/documents/{documentID}/permissions): a trailing slash, extra segments,
// doubled slashes inside the tail, or a "permissions" segment short of the
// registered shape. ServeMux would answer those with a 301/307 redirect or a
// plain-text 404/405; the permission endpoints promise a JSON error and never
// a redirect, so every such path is a malformed 400. The keyword is matched
// only past the documentID position, so a document literally named
// "permissions" keeps its ordinary routes.
func malformedPermissionPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg != "permissions" || i == 0 {
			continue
		}
		// Keyword position reached. The collection is exactly
		// {documentID}/permissions; anything else (trailing slash, a missing
		// or extra segment, an empty adjacent segment) is malformed.
		return !(len(segs) == 2 && segs[0] != "")
	}
	return false
}
