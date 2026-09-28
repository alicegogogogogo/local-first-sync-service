package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// handleListCRDTOps is the document-level read-only ordered listing over a
// document's CRDT operation collection:
//
//	GET /v1/documents/{documentID}/crdt/list?deviceId=D&limit=N&offset=M
//
// It is the browse counterpart of the POST .../crdt/query batch lookup:
// instead of naming ids the caller pages over the operations still in the
// online tables, ordered by ascending operation id. The calling device is
// declared by the deviceId query parameter with the same identity rule as the
// other read-only document-level CRDT entry (the state subscription); a
// missing or empty deviceId is an unregistered caller. Pagination uses the
// attachment listing's exact parameters: limit an integer in 1..1000
// (default 100), offset a non-negative integer (default 0), and any illegal
// value is a 400 judged before the device or the document is looked up.
//
// Every item carries its operation id, its source device, the document's
// type and the type-specific comparison content exactly as it was saved at
// commit time — a counter's contribution, a gset's element set, a register's
// version and value, an orset's action and element. An id compaction trimmed
// appears in no page; compacted reports the total number trimmed. The body
// is one compact JSON line ending in a newline with the top-level keys ops,
// count, compacted, and count equals the page length, so non-overlapping
// pages visit every online operation exactly once.
//
// The verdict order is request shape (400, including pagination and the
// exact path/verb) first, then device existence (404), document permission
// (403) and, last, the document's CRDT existence (404 for a document with no
// committed operation, the same verdict the state read gives). A 404 or 403
// exposes no CRDT content. The listing writes no operation, allocates no
// cursor and wakes neither a long poll nor a push subscriber. Success is
// byte-stable for a repeat request and across a process restart.
func handleListCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	// Pagination shape is settled before the device existence verdict, so a
	// malformed page request against an unregistered device is still a 400.
	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	// A missing or empty deviceId is indistinguishable from an unregistered
	// one, exactly as on the other read-only query-parameter CRDT entry: 404
	// JSON, before any CRDT content is observed.
	deviceID := r.URL.Query().Get("deviceId")
	if deviceID == "" {
		writeError(w, http.StatusNotFound, "device not found")
		return
	}

	page, err := s.ListCRDTOps(documentID, deviceID, limit, offset)
	if err != nil {
		writeCRDTLookupError(w, err)
		return
	}
	writeCRDTListResponse(w, page)
}

// handleSessionListCRDTOps is the session view's counterpart to the
// document-level read-only CRDT operation listing:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/list?limit=N&offset=M
//
// No device identity travels in the request: the calling device is the
// session's owning device resolved from the path with the same identity rule
// as the other session CRDT entries, and a stray deviceId query parameter is
// ignored and can never override the session owner. Pagination, item shape,
// ordering and the response body are byte-for-byte the document-level
// listing's. Checks run in the fixed order request shape (400, including
// pagination) first, then session existence (404), document permission
// (403) and the document's CRDT existence (404). A 404 or 403 exposes no
// CRDT content; the listing is read-only, byte-stable on repeat and across a
// restart, and wakes nobody.
func handleSessionListCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Pagination shape precedes the session lookup, so a malformed page
	// request against an unknown session is still a 400.
	limit, offset, ok := parseAttachmentListQuery(w, r)
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

	// The listing is the same gated read the document-level entry uses: the
	// registration/permission verdict is retaken inside its serialized
	// transaction, so a session whose device was deregistered between the two
	// lookups answers 404 and a revoked device answers 403 before any CRDT
	// content is observed.
	page, err := s.ListCRDTOps(documentID, deviceID, limit, offset)
	if err != nil {
		if errors.Is(err, store.ErrDeviceNotFound) {
			// The session's device was deregistered between the lookup and the
			// read; its cascade removed the session too, so the identity the
			// caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeCRDTLookupError(w, err)
		return
	}
	writeCRDTListResponse(w, page)
}

// crdtListResponse fixes the on-the-wire top-level key order to ops then
// count then compacted; count always equals the number of ops entries, which
// is the page length. Items are pre-rendered so each carries its
// type-specific fixed key order.
type crdtListResponse struct {
	Ops       []json.RawMessage `json:"ops"`
	Count     int               `json:"count"`
	Compacted int64             `json:"compacted"`
}

// writeCRDTListResponse renders one listing page the single way both list
// surfaces emit it: compact single-line JSON, top-level keys ops, count,
// compacted, one trailing newline.
func writeCRDTListResponse(w http.ResponseWriter, page crdt.OpsPage) {
	items := make([]json.RawMessage, len(page.Ops))
	for i, op := range page.Ops {
		items[i] = marshalCRDTListItem(op)
	}
	writeJSON(w, http.StatusOK, crdtListResponse{Ops: items, Count: len(items), Compacted: page.Compacted})
}

// marshalCRDTListItem renders one online operation with the fixed key order
// the listing promises:
//
//   - counter: {"id","deviceId","type","value"} with the stored contribution;
//   - gset: {"id","deviceId","type","elements"} with the stored element set;
//   - register: {"id","deviceId","type","version","value"} with the stored
//     version and the value verbatim as it was saved;
//   - orset: {"id","deviceId","type","action","element"}.
//
// The content is embedded as native JSON rather than an escaped blob, exactly
// as the state, snapshot and by-id lookup renders do, and the content field
// names are the by-id lookup's hit-item names.
func marshalCRDTListItem(op crdt.OpListItem) json.RawMessage {
	var buf strings.Builder
	idRaw, _ := json.Marshal(op.ID)
	buf.WriteString(`{"id":`)
	_, _ = buf.Write(idRaw)
	deviceRaw, _ := json.Marshal(op.DeviceID)
	buf.WriteString(`,"deviceId":`)
	_, _ = buf.Write(deviceRaw)
	typeRaw, _ := json.Marshal(op.Type)
	buf.WriteString(`,"type":`)
	_, _ = buf.Write(typeRaw)

	switch op.Type {
	case crdt.TypeCounter:
		buf.WriteString(`,"value":`)
		_, _ = buf.Write(op.Value)
	case crdt.TypeGSet:
		buf.WriteString(`,"elements":`)
		_, _ = buf.Write(op.Elements)
	case crdt.TypeRegister:
		buf.WriteString(`,"version":`)
		buf.WriteString(strconv.FormatInt(op.Version, 10))
		buf.WriteString(`,"value":`)
		_, _ = buf.Write(op.Value)
	case crdt.TypeORSet:
		actionRaw, _ := json.Marshal(op.Action)
		elementRaw, _ := json.Marshal(op.Element)
		buf.WriteString(`,"action":`)
		_, _ = buf.Write(actionRaw)
		buf.WriteString(`,"element":`)
		_, _ = buf.Write(elementRaw)
	}
	buf.WriteString("}")
	return json.RawMessage(buf.String())
}
