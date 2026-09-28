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

// handleListCRDTOps is the document-level read-only browse entry over a
// document's online CRDT operation collection:
//
//	GET /v1/documents/{documentID}/crdt/list?deviceId=D&limit=N&offset=M
//
// It is GET with no request body and the calling device is declared by the
// deviceId query parameter, the same declaration the other read-only entries
// use. limit (1..1000, default 100) and offset (non-negative, default 0) page
// the online operations in their stable (ascending operation-id) order; an
// illegal value is a 400 JSON error checked before any other verdict. The
// page then runs the fixed verdict order — device existence (an unregistered
// or unnamed device is a 404), document permission (a revoked device is a
// 403) and CRDT state existence (a document with no committed operation is
// the same 404 the state read gives); a 404 or 403 exposes no CRDT content.
//
// Success is one compact JSON line ending in a newline with the top-level
// keys ops, count, compacted. Each item names its operation id, source
// device, document type and the type-specific comparison content exactly as
// saved at commit time, with the query hit's field names. Ids compaction
// trimmed never enter the page and their content is never restored; compacted
// reports their total count. The listing writes nothing, allocates no change
// cursor, wakes no long poll and pushes to no subscriber, so a repeat request
// or the same request after a restart renders byte-for-byte the same body.
func handleListCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	// Pagination shape is settled before any existence verdict, so an illegal
	// limit/offset against an unregistered device or an unknown document is
	// still a 400.
	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	deviceID := r.URL.Query().Get("deviceId")
	// A missing or empty deviceId is indistinguishable from an unregistered
	// caller; judging it here keeps the 404 ahead of any CRDT-content
	// observation, exactly as the document deletion entry does.
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
// It is GET with no request body. The calling device is the session's owning
// device resolved from the path with the same identity rule as the other
// session CRDT entries; a deviceId carried by mistake in the query string is
// just another unknown query parameter and can never override the session
// owner. limit/offset pagination, the stable order, the page body and the
// compacted count are byte-for-byte the document-level listing's. Checks run
// in the fixed order request shape (400, the pagination verdict first),
// session existence (404), document permission (403) and CRDT state
// existence (404); a 404 or 403 exposes no CRDT content and the read wakes
// nobody.
func handleSessionListCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	// Pagination shape precedes the session existence verdict, so a malformed
	// listing against an unknown session is still a 400.
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

	// The listing is the same gated read the document-level browse uses: the
	// registration/permission verdict is retaken inside its transaction, so a
	// session whose device was deregistered between the two lookups answers
	// 404 and a revoked device answers 403 before any CRDT content is observed.
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

// crdtListResponse fixes the on-the-wire top-level key order to ops, count and
// compacted; count is always the page length and therefore equals the array
// length. Ops is always a non-nil slice so an exhausted page serializes as []
// rather than null.
type crdtListResponse struct {
	Ops       []json.RawMessage `json:"ops"`
	Count     int               `json:"count"`
	Compacted int64             `json:"compacted"`
}

// writeCRDTListResponse renders the page the single way both list surfaces
// emit it: compact single-line JSON, top-level keys ops, count, compacted,
// one trailing newline.
func writeCRDTListResponse(w http.ResponseWriter, page crdt.OpList) {
	items := make([]json.RawMessage, len(page.Ops))
	for i, op := range page.Ops {
		items[i] = marshalCRDTListItem(op)
	}
	writeJSON(w, http.StatusOK, crdtListResponse{
		Ops:       items,
		Count:     len(items),
		Compacted: page.Compacted,
	})
}

// marshalCRDTListItem renders one listed operation with the fixed key order
// id, deviceId, type followed by the type-specific comparison content:
//
//   - counter: {"id","deviceId","type","value"} with the stored contribution;
//   - gset: {"id","deviceId","type","elements"} with the stored element set;
//   - register: {"id","deviceId","type","version","value"} with the stored
//     version and the value verbatim as it was saved;
//   - orset: {"id","deviceId","type","action","element"}.
//
// Every listed item is an online operation, so it carries no status field;
// the content field names are the query hit's. The content is embedded as
// native JSON rather than an escaped blob, exactly as the lookup renderer
// does.
func marshalCRDTListItem(l crdt.OpLookup) json.RawMessage {
	var buf strings.Builder
	idRaw, _ := json.Marshal(l.ID)
	deviceRaw, _ := json.Marshal(l.DeviceID)
	typeRaw, _ := json.Marshal(l.Type)
	buf.WriteString(`{"id":`)
	_, _ = buf.Write(idRaw)
	buf.WriteString(`,"deviceId":`)
	_, _ = buf.Write(deviceRaw)
	buf.WriteString(`,"type":`)
	_, _ = buf.Write(typeRaw)

	switch l.Type {
	case crdt.TypeCounter:
		buf.WriteString(`,"value":`)
		_, _ = buf.Write(l.Value)
	case crdt.TypeGSet:
		buf.WriteString(`,"elements":`)
		_, _ = buf.Write(l.Elements)
	case crdt.TypeRegister:
		buf.WriteString(`,"version":`)
		buf.WriteString(strconv.FormatInt(l.Version, 10))
		buf.WriteString(`,"value":`)
		_, _ = buf.Write(l.Value)
	case crdt.TypeORSet:
		actionRaw, _ := json.Marshal(l.Action)
		elementRaw, _ := json.Marshal(l.Element)
		buf.WriteString(`,"action":`)
		_, _ = buf.Write(actionRaw)
		buf.WriteString(`,"element":`)
		_, _ = buf.Write(elementRaw)
	}
	buf.WriteString("}")
	return json.RawMessage(buf.String())
}
