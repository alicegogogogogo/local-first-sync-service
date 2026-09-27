package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// crdtOpIn is one element of a CRDT submission. Exactly one content shape is
// meaningful depending on the declared document type: value for a counter,
// elements for a grow-only set, value plus version for a register, action
// plus element for an observed-remove set.
type crdtOpIn struct {
	ID       string          `json:"id"`
	Value    json.RawMessage `json:"value"`
	Elements []string        `json:"elements"`
	Version  json.RawMessage `json:"version"`
	Action   string          `json:"action"`
	Element  string          `json:"element"`
}

// crdtOpsRequest is the body of POST .../crdt/ops.
type crdtOpsRequest struct {
	DeviceID string     `json:"deviceId"`
	Type     string     `json:"type"`
	Ops      []crdtOpIn `json:"ops"`
}

// sessionCRDTOpsRequest is the body of the session-scoped POST .../crdt/ops:
// the document-level shape minus the device id, which the session supplies.
type sessionCRDTOpsRequest struct {
	Type string     `json:"type"`
	Ops  []crdtOpIn `json:"ops"`
}

// handleCRDTOps accepts a batch of CRDT operations for a document.
//
// The batch declares the document's type; the first accepted batch fixes it
// to counter, gset, register or orset and every later batch must declare the
// same type. Two batches racing to declare different types serialize in one
// transaction: exactly one takes effect and the other is a 409 that writes
// nothing.
//
// The strict body contract matches the other JSON endpoints: application/json,
// one JSON value, no trailing content, a non-empty deviceId, a valid type, a
// non-empty ops array of elements with non-empty ids, no in-batch duplicate
// ids, and type-correct content (counter: an integer value; gset: an elements
// array; register: any JSON value — null included — plus a non-negative
// integer version; orset: action "add" or "remove" plus a non-empty string
// element). Any violation is a 400 with zero writes.
func handleCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtOpsRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	ops, ok := decodeCRDTOps(w, req.Type, req.Ops)
	if !ok {
		return
	}
	for i := range ops {
		ops[i].DeviceID = req.DeviceID
	}

	results, err := s.SubmitCRDTOps(documentID, req.Type, ops)
	if err != nil {
		var conflict *crdt.ErrConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, conflict.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to commit crdt ops")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// handleSessionCRDTOps is the session-scoped CRDT batch commit:
//
//	POST /v1/sessions/{sessionId}/documents/{documentId}/crdt/ops
//
// The request body carries only the declared type and the ops array — the
// calling device is the session's owning device, resolved with the same
// identity rule as the session reads and subscriptions; no new credential is
// introduced and no device id travels in the body. Type fixation, the four
// merge rules, per-op idempotency and conflict judgments, the success body
// (results in request order, each naming its id and whether it was created)
// and the state-change push are exactly the document-level submission's: both
// handlers funnel into the same serialized commit. The checks run in the
// fixed order request shape (400), session existence (404), document
// permission (403, taken inside the commit transaction) and only then the
// state-dependent judgments (409). Every rejection — a malformed body, an
// empty or duplicate-id batch, a missing or deleted session, a revoked
// permission, a type conflict, a regressed contribution or version — writes
// nothing and notifies nobody.
func handleSessionCRDTOps(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	var req sessionCRDTOpsRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// Request shape is settled before the session lookup, so a malformed batch
	// against an unknown session is still a 400. The device is stamped onto
	// the ops only after the session resolves.
	ops, ok := decodeCRDTOps(w, req.Type, req.Ops)
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
	for i := range ops {
		ops[i].DeviceID = deviceID
	}

	results, err := s.SubmitCRDTOps(documentID, req.Type, ops)
	if err != nil {
		var conflict *crdt.ErrConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			// The session's device was deregistered between the lookup and the
			// commit; its cascade removed the session too, so the identity the
			// caller used no longer exists.
			writeError(w, http.StatusNotFound, "session not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, conflict.Error())
		default:
			writeError(w, http.StatusInternalServerError, "failed to commit crdt ops")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// decodeCRDTOps validates the shared type/ops contract of a CRDT submission
// and builds the service-level operations (without a device stamp; the caller
// supplies the device identity). Any violation writes a 400 and reports
// false: an unknown type, an empty ops array, an element with a missing or
// empty id, an in-batch duplicate id, or type-incorrect content (counter: an
// integer value >= 0; gset: a non-empty elements array of non-empty strings;
// register: any JSON value — null included — plus a non-negative integer
// version; orset: action "add" or "remove" plus a non-empty string element).
func decodeCRDTOps(w http.ResponseWriter, declaredType string, in []crdtOpIn) ([]crdt.Op, bool) {
	if declaredType != crdt.TypeCounter && declaredType != crdt.TypeGSet && declaredType != crdt.TypeRegister && declaredType != crdt.TypeORSet {
		writeError(w, http.StatusBadRequest, `type must be "counter", "gset", "register" or "orset"`)
		return nil, false
	}
	if len(in) == 0 {
		writeError(w, http.StatusBadRequest, "ops must be a non-empty array")
		return nil, false
	}

	ops := make([]crdt.Op, len(in))
	seen := make(map[string]struct{}, len(in))
	for i, op := range in {
		if op.ID == "" {
			writeError(w, http.StatusBadRequest, "each op must have a non-empty string id")
			return nil, false
		}
		if _, dup := seen[op.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate op id within batch: "+op.ID)
			return nil, false
		}
		seen[op.ID] = struct{}{}

		crdtOp := crdt.Op{ID: op.ID}
		switch declaredType {
		case crdt.TypeCounter:
			v, ok := parseNonNegativeInt(op.Value)
			if !ok {
				writeError(w, http.StatusBadRequest, "each counter op must carry an integer value >= 0")
				return nil, false
			}
			// Re-marshal the canonical integer so idempotency compares the
			// decoded value rather than the client's literal formatting.
			raw, _ := json.Marshal(v)
			crdtOp.Value = raw
		case crdt.TypeGSet:
			if len(op.Elements) == 0 {
				writeError(w, http.StatusBadRequest, "each gset op must add at least one element")
				return nil, false
			}
			for _, element := range op.Elements {
				if element == "" {
					writeError(w, http.StatusBadRequest, "gset elements must be non-empty strings")
					return nil, false
				}
			}
			crdtOp.Elements = op.Elements
		case crdt.TypeRegister:
			// The value may be any JSON, null included; only an absent field
			// (a nil RawMessage — an explicit null decodes to the bytes
			// "null") is a missing field. The value is stored verbatim.
			if op.Value == nil {
				writeError(w, http.StatusBadRequest, "each register op must carry a value")
				return nil, false
			}
			v, ok := parseNonNegativeInt(op.Version)
			if !ok {
				writeError(w, http.StatusBadRequest, "each register op must carry an integer version >= 0")
				return nil, false
			}
			crdtOp.Value = op.Value
			crdtOp.Version = v
		case crdt.TypeORSet:
			if op.Action != crdt.ORSetAdd && op.Action != crdt.ORSetRemove {
				writeError(w, http.StatusBadRequest, `each orset op must carry action "add" or "remove"`)
				return nil, false
			}
			if op.Element == "" {
				writeError(w, http.StatusBadRequest, "each orset op must carry a non-empty string element")
				return nil, false
			}
			crdtOp.Action = op.Action
			crdtOp.Element = op.Element
		}
		ops[i] = crdtOp
	}
	return ops, true
}

// handleCRDTState returns the document's current type and merged result. A
// document with no committed operations has no state yet and answers 404.
func handleCRDTState(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	state, err := s.GetCRDTState(documentID)
	if err != nil {
		if errors.Is(err, crdt.ErrNotFound) {
			writeError(w, http.StatusNotFound, "crdt state not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load crdt state")
		return
	}

	writeCRDTState(w, state)
}

// handleSessionCRDTState is the session-scoped view of the document-level
// state read:
//
//	GET /v1/sessions/{sessionId}/documents/{documentId}/crdt/state
//
// The read identity is entirely the existing session (no new auth): the
// session's owning device is the credential and the validation order matches
// the other session-scoped reads exactly — request shape (400, enforced by the
// route and path guard before this handler), then session existence
// (404 JSON for a session that never existed or was deleted), then the
// session device's permission for the document (403 JSON with no state
// content), and finally the document's CRDT state. A document without any
// committed CRDT operation is the same 404 JSON as the document-level read, so
// a deleted session, an unknown document-state and a revoked permission remain
// three independent answers.
//
// On success the body is byte-for-byte the document-level state read's body
// (writeCRDTState): compact single-line JSON, keys type then value, one
// trailing newline. The endpoint is read-only: it renders the state derived by
// the store without advancing a cursor, writing a change, registering a
// subscription or affecting CRDT type fixation, idempotency or persistence; a
// restart changes neither the answer nor the status codes.
func handleSessionCRDTState(s *app.App, w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")   // route pattern + guard guarantee non-empty
	documentID := r.PathValue("documentId") // route pattern + guard guarantee non-empty

	deviceID, err := s.SessionDevice(sessionID)
	if err != nil {
		if errors.Is(err, store.ErrSessionNotFound) {
			writeError(w, http.StatusNotFound, "session not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to look up session")
		return
	}

	authorized, err := s.DocumentAuthorized(documentID, deviceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to look up permission")
		return
	}
	if !authorized {
		writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		return
	}

	state, err := s.GetCRDTState(documentID)
	if err != nil {
		if errors.Is(err, crdt.ErrNotFound) {
			writeError(w, http.StatusNotFound, "crdt state not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load crdt state")
		return
	}

	writeCRDTState(w, state)
}

// writeCRDTState renders a CRDT state exactly as every state reader must:
// compact single-line JSON with the keys in type, value order, terminated by a
// newline (the json.Encoder output), shared by the document-level read, the
// session-scoped read and — via marshalCRDTState — the subscription push
// frames, so the three can be compared byte-for-byte.
func writeCRDTState(w http.ResponseWriter, state crdt.State) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(marshalCRDTState(state))
}

// marshalCRDTState encodes a CRDT state the single way every surface emits it:
// compact single-line JSON with the keys in type, value order and one trailing
// newline. The value is embedded as native JSON (a number, an array, or a
// register's arbitrary JSON value) rather than an escaped blob.
func marshalCRDTState(state crdt.State) []byte {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	_ = enc.Encode(map[string]any{
		"type":  state.Type,
		"value": json.RawMessage(state.Value),
	})
	return []byte(buf.String())
}

// malformedCRDTPath reports whether p targets the CRDT namespace but is not at
// one of the five exact endpoints:
//
//	POST     /v1/documents/{documentID}/crdt/ops
//	GET      /v1/documents/{documentID}/crdt/state
//	POST     /v1/documents/{documentID}/crdt/compact
//	GET      /v1/documents/{documentID}/crdt/snapshot
//	GET      /v1/documents/{documentID}/crdt/state/subscribe
//
// A missing/empty document id, a trailing slash, extra path segments, or a
// "crdt" segment in any position short of that shape is a malformed 400 rather
// than ServeMux's redirect or plain-text 404/405, so the new endpoints never
// redirect or emit HTML. The keyword is matched only past the documentID
// position, so a document literally named "crdt" keeps its ordinary routes.
func malformedCRDTPath(p string) bool {
	rest, ok := strings.CutPrefix(p, "/v1/documents/")
	if !ok {
		return false
	}
	segs := strings.Split(rest, "/")
	for i, seg := range segs {
		if seg == "crdt" && i > 0 {
			if segs[0] == "" {
				return true
			}
			// The CRDT state subscription adds one trailing "subscribe"
			// segment past the state read; nothing else may follow state.
			if len(segs) == 4 && segs[2] == "state" && segs[3] == "subscribe" {
				return false
			}
			if len(segs) != 3 {
				return true
			}
			switch segs[2] {
			case "ops", "state", "compact", "snapshot":
				return false
			default:
				return true
			}
		}
	}
	return false
}
