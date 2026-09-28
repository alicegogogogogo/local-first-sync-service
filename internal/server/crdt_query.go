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

// crdtOpLookupRequest is the body of POST .../crdt/query: the calling device
// and the non-empty, in-batch-unique set of operation ids to answer. The
// device identity uses the same declaration the document-level CRDT
// submission uses — no new authentication is introduced.
type crdtOpLookupRequest struct {
	DeviceID string   `json:"deviceId"`
	IDs      []string `json:"ids"`
}

// sessionCRDTOpLookupRequest is the body of the session-scoped POST
// .../crdt/query: only the non-empty, in-batch-unique set of operation ids
// travels in the request. The calling device is the session's owning device
// resolved from the path, so a stray deviceId is decoded and ignored like
// every other unknown field and can never override the session owner.
type sessionCRDTOpLookupRequest struct {
	IDs []string `json:"ids"`
}

// crdtOpLookupItem fixes the on-the-wire key order of one answer to id,
// status, then the found-only content fields. Missing and compacted answers
// omit every content field: a compacted id carries nothing but id and status
// — its retained summary is only consulted to report the trimmed verdict, so
// no content is restored — and a missing id carries the same bare shape.
// Found items are rendered per type by marshalCRDTLookupItem, which keeps the
// type-specific key order fixed; this struct only renders the bare shapes.
type crdtOpLookupItem struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

// crdtOpLookupResponse is the query body with the top-level key order fixed
// to results then count; count always equals the number of result entries,
// which equals the number of ids asked for.
type crdtOpLookupResponse struct {
	Results []json.RawMessage `json:"results"`
	Count   int               `json:"count"`
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
//   - "found": the operation is still stored online, so the answer carries
//     its originating device and that type's comparison content exactly as it
//     was saved at submission — a counter its contribution, a gset its element
//     set, a register its version and value, an orset its action and element;
//   - "compacted": CRDT compaction trimmed the operation's merge row, so the
//     answer reports the status alone from the retained identity summary — no
//     content is restored and the summary never participates in another read;
//   - "missing": the id has never belonged to the document, so the answer
//     carries no content. A missing id is a normal answer, not an error.
//
// The verdict order is request shape (400) first — content type, JSON
// validity, trailing content, a missing or empty ids array, an empty or
// mistyped element and an in-batch duplicate — then device existence (404),
// then document permission (403), then the CRDT state existence the state
// read uses (404 for a document with no committed operation). A 404 or 403
// exposes no CRDT content. The query writes nothing, allocates no change
// cursor, wakes neither a long poll nor a push subscriber and registers no
// subscription. Success is one compact JSON line ending in a newline with
// results then count.
func handleQueryCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtOpLookupRequest
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
	writeCRDTLookupResults(w, lookups)
}

// handleSessionQueryCRDTOps is the session view's counterpart to the
// document-level read-only CRDT batch lookup:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/crdt/query
//
// The strict application/json body names a non-empty array of non-empty,
// in-batch-unique operation ids; answers arrive in the exact order the ids
// were given and use the exact item and response shapes the document-level
// lookup emits. Each id is answered independently with the same found,
// compacted and missing semantics. Checks run in the fixed order request
// shape (400) first, then session existence (404), then the session device's
// document permission (403, retaken inside the lookup transaction), then the
// CRDT state existence (404 for a document with no committed operation). A
// 404 or 403 exposes no CRDT content. The query writes nothing, allocates no
// change cursor and wakes neither a long poll nor a push subscriber. Success
// is one compact JSON line ending in a newline with results then count.
func handleSessionQueryCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionCRDTOpLookupRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// The session body carries no device field. The device is resolved from
	// the session after the shape verdict, so a malformed batch against an
	// unknown session is still a 400.
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
		// A deregistered session device answers with the session verdict,
		// exactly as the other session-scoped CRDT entries do.
		if errors.Is(err, store.ErrDeviceNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeCRDTLookupError(w, err)
		return
	}
	writeCRDTLookupResults(w, lookups)
}

// validateCRDTLookupIDs enforces the shared ids contract of both CRDT batch
// lookups: a non-empty array of non-empty strings without an in-batch
// duplicate. Any violation writes a 400 and reports false. The check runs
// before the device/session existence verdict in both handlers.
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

// writeCRDTLookupError maps a lookup transaction error to the same status
// verdicts the CRDT state reads use: an unregistered device 404, a revoked
// permission 403, a document with no committed CRDT operation 404 — three
// independent failures, none exposing content.
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

// writeCRDTLookupResults renders the lookup body the single way both surfaces
// emit it: compact single-line JSON with the top-level keys in results,count
// order and one trailing newline. Each item's key order is fixed by
// marshalCRDTLookupItem rather than the encoder's map ordering.
func writeCRDTLookupResults(w http.ResponseWriter, lookups []crdt.OpLookup) {
	items := make([]json.RawMessage, len(lookups))
	for i, l := range lookups {
		items[i] = marshalCRDTLookupItem(l)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(crdtOpLookupResponse{Results: items, Count: len(items)})
}

// marshalCRDTLookupItem renders one lookup answer as compact JSON with fixed
// key order. Missing and compacted answers are {"id":...,"status":...}; a
// found answer keeps id,status,deviceId first and then the type-specific
// fields in a fixed order: counter value, gset elements, register
// version,value and orset action,element. Content is embedded verbatim — a
// counter's saved contribution, a register's saved value (any JSON, null
// included) and a gset's saved element array — so the answer presents exactly
// what submission stored rather than a re-derived value.
func marshalCRDTLookupItem(l crdt.OpLookup) json.RawMessage {
	if l.Status != crdt.OpLookupFound {
		raw, _ := json.Marshal(crdtOpLookupItem{ID: l.ID, Status: l.Status})
		return raw
	}

	var buf strings.Builder
	buf.WriteString(`{"id":`)
	id, _ := json.Marshal(l.ID)
	buf.Write(id)
	buf.WriteString(`,"status":"found","deviceId":`)
	device, _ := json.Marshal(l.DeviceID)
	buf.Write(device)
	switch l.Type {
	case crdt.TypeCounter:
		buf.WriteString(`,"value":`)
		buf.Write(l.Value)
	case crdt.TypeGSet:
		buf.WriteString(`,"elements":`)
		elements, _ := json.Marshal(l.Elements)
		buf.Write(elements)
	case crdt.TypeRegister:
		buf.WriteString(`,"version":`)
		buf.WriteString(strconv.FormatInt(l.Version, 10))
		buf.WriteString(`,"value":`)
		buf.Write(l.Value)
	case crdt.TypeORSet:
		buf.WriteString(`,"action":`)
		action, _ := json.Marshal(l.Action)
		buf.Write(action)
		buf.WriteString(`,"element":`)
		element, _ := json.Marshal(l.Element)
		buf.Write(element)
	}
	buf.WriteString("}")
	return json.RawMessage(buf.String())
}
