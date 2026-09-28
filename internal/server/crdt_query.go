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

// crdtLookupRequest is the body of POST .../crdt/query: the calling device and
// the non-empty, in-batch-unique set of operation ids to answer. The device
// identity uses the same declaration the document-level CRDT submission uses —
// no new authentication is introduced.
type crdtLookupRequest struct {
	DeviceID string   `json:"deviceId"`
	IDs      []string `json:"ids"`
}

// sessionCRDTLookupRequest is the body of the session-scoped
// POST .../crdt/query: only the ids travel in the request. The calling device
// is the session's owning device, so a stray deviceId is decoded and ignored
// like every other unknown field and can never override the session owner.
type sessionCRDTLookupRequest struct {
	IDs []string `json:"ids"`
}

// handleQueryCRDTOps is the read-only batch lookup over a document's CRDT
// operation collection:
//
//	POST /v1/documents/{documentID}/crdt/query
//
// The strict application/json body names the calling device and a non-empty
// array of non-empty, in-batch-unique operation ids; answers arrive in the
// exact order the ids were given. Each id is answered independently:
//
//   - "found": the operation is still in the online tables, so the answer
//     carries its originating device and the type-specific comparison content
//     exactly as it was saved at commit time — a counter's contribution value,
//     a gset's element set, a register's version and value, an orset's action
//     and element;
//   - "compacted": compaction trimmed the row, so the answer reports only the
//     trimmed status from its retained identity — no device or content is
//     restored and the identity never participates in another read;
//   - "missing": the id has never belonged to the document, so the answer
//     carries no content. A missing id is a normal answer, not an error.
//
// The verdict order is request shape (400) first — content type, JSON
// validity, trailing content, a missing or empty ids array, an empty or
// mistyped element and an in-batch duplicate — then device existence (404),
// document permission (403) and, last, the document's CRDT existence (404 for
// a document with no committed operation, the same verdict the state read
// gives). A 404 or 403 exposes no CRDT content. The query writes no
// operation, allocates no cursor and wakes neither a long poll nor a push
// subscriber. Success is one compact JSON line ending in a newline with
// results then count.
func handleQueryCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtLookupRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// Shape checks all precede the device existence verdict, so a malformed
	// query against an unregistered device is still a 400.
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if !validateCRDTLookupIDs(w, req.IDs) {
		return
	}

	lookups, err := s.GetCRDTOpsByIDs(documentID, req.DeviceID, req.IDs)
	if err != nil {
		writeCRDTLookupError(w, err)
		return
	}
	writeCRDTLookupResponse(w, lookups)
}

// handleSessionQueryCRDTOps is the session view's counterpart to the
// document-level read-only CRDT batch lookup:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/crdt/query
//
// The strict application/json body names only a non-empty array of non-empty,
// in-batch-unique operation ids; the calling device is the session's owning
// device resolved from the path with the same identity rule as the other
// session CRDT entries. Answers and the response body are byte-for-byte the
// document-level lookup's. Checks run in the fixed order request shape (400)
// first, then session existence (404), document permission (403) and the
// document's CRDT existence (404). A 404 or 403 exposes no CRDT content; the
// query is read-only and wakes nobody.
func handleSessionQueryCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionCRDTLookupRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// Request shape is settled before the session lookup, so a malformed query
	// against an unknown session is still a 400.
	if !validateCRDTLookupIDs(w, req.IDs) {
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

	// The lookup is the same gated read the document-level batch query uses:
	// the registration/permission verdict is retaken inside its serialized
	// transaction, so a session whose device was deregistered between the two
	// lookups answers 404 and a revoked device answers 403 before any CRDT
	// content is observed.
	lookups, err := s.GetCRDTOpsByIDs(documentID, deviceID, req.IDs)
	if err != nil {
		if errors.Is(err, store.ErrDeviceNotFound) {
			// The session's device was deregistered between the lookup and the
			// query; its cascade removed the session too, so the identity the
			// caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeCRDTLookupError(w, err)
		return
	}
	writeCRDTLookupResponse(w, lookups)
}

// validateCRDTLookupIDs enforces the shared ids contract of both CRDT query
// entries: a non-empty array of non-empty strings with no in-batch duplicate.
// A non-string element was already rejected while decoding the JSON. Any
// violation writes a 400 JSON error and reports false.
func validateCRDTLookupIDs(w http.ResponseWriter, ids []string) bool {
	if len(ids) == 0 {
		writeError(w, http.StatusBadRequest, "ids must be a non-empty array of non-empty strings")
		return false
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			writeError(w, http.StatusBadRequest, "each id must be a non-empty string")
			return false
		}
		if _, dup := seen[id]; dup {
			writeError(w, http.StatusBadRequest, "duplicate crdt op id within batch: "+id)
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

// writeCRDTLookupError maps a GetCRDTOpsByIDs failure to its fixed JSON
// verdict: an unregistered device is a 404, a revoked permission a 403, and a
// document with no committed CRDT operation the same 404 the state read
// gives; anything else is a 500. The session handler intercepts the
// unregistered-device verdict itself so that path answers "session not
// found".
func writeCRDTLookupError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrDeviceNotFound):
		writeError(w, http.StatusNotFound, "device not found")
	case errors.Is(err, store.ErrPermissionDenied):
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
	case errors.Is(err, crdt.ErrNotFound):
		writeError(w, http.StatusNotFound, "crdt state not found")
	default:
		writeError(w, http.StatusInternalServerError, "failed to look up crdt ops")
	}
}

// crdtLookupResponse fixes the on-the-wire top-level key order to results then
// count; count always equals the number of result entries, which equals the
// number of ids asked for. Items are pre-rendered so each carries its
// type-specific fixed key order.
type crdtLookupResponse struct {
	Results []json.RawMessage `json:"results"`
	Count   int               `json:"count"`
}

// writeCRDTLookupResponse renders the batch answer the single way both query
// surfaces emit it: compact single-line JSON, top-level keys results then
// count, one trailing newline.
func writeCRDTLookupResponse(w http.ResponseWriter, lookups []crdt.OpLookup) {
	items := make([]json.RawMessage, len(lookups))
	for i, l := range lookups {
		items[i] = marshalCRDTLookupItem(l)
	}
	writeJSON(w, http.StatusOK, crdtLookupResponse{Results: items, Count: len(items)})
}

// marshalCRDTLookupItem renders one lookup answer with a fixed key order that
// depends on the answer:
//
//   - missing/compacted: {"id","status"} only — a compacted id carries no
//     device and no content;
//   - counter: {"id","status","deviceId","value"} with the stored contribution;
//   - gset: {"id","status","deviceId","elements"} with the stored element set;
//   - register: {"id","status","deviceId","version","value"} with the stored
//     version and the value verbatim as it was saved;
//   - orset: {"id","status","deviceId","action","element"}.
//
// The content is embedded as native JSON rather than an escaped blob, exactly
// as the state and snapshot renders do.
func marshalCRDTLookupItem(l crdt.OpLookup) json.RawMessage {
	var buf strings.Builder
	idRaw, _ := json.Marshal(l.ID)
	buf.WriteString(`{"id":`)
	_, _ = buf.Write(idRaw)
	buf.WriteString(`,"status":`)
	statusRaw, _ := json.Marshal(l.Status)
	_, _ = buf.Write(statusRaw)

	if l.Status == crdt.LookupFound {
		deviceRaw, _ := json.Marshal(l.DeviceID)
		buf.WriteString(`,"deviceId":`)
		_, _ = buf.Write(deviceRaw)
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
	}
	buf.WriteString("}")
	return json.RawMessage(buf.String())
}
