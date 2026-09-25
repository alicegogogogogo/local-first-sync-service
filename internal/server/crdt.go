package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// crdtOpIn is one element of a CRDT submission. Exactly one content field is
// meaningful depending on the declared document type: value for a counter,
// elements for a grow-only set.
type crdtOpIn struct {
	ID       string          `json:"id"`
	Value    json.RawMessage `json:"value"`
	Elements []string        `json:"elements"`
}

// crdtOpsRequest is the body of POST .../crdt/ops.
type crdtOpsRequest struct {
	DeviceID string     `json:"deviceId"`
	Type     string     `json:"type"`
	Ops      []crdtOpIn `json:"ops"`
}

// handleCRDTOps accepts a batch of CRDT operations for a document.
//
// The batch declares the document's type; the first accepted batch fixes it to
// counter or gset and every later batch must declare the same type. Two
// batches racing to declare different types serialize in one transaction:
// exactly one takes effect and the other is a 409 that writes nothing.
//
// The strict body contract matches the other JSON endpoints: application/json,
// one JSON value, no trailing content, a non-empty deviceId, a valid type, a
// non-empty ops array of elements with non-empty ids, no in-batch duplicate
// ids, and type-correct content (counter: an integer value; gset: an elements
// array). Any violation is a 400 with zero writes.
func handleCRDTOps(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtOpsRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	if req.Type != store.CRDTTypeCounter && req.Type != store.CRDTTypeGSet {
		writeError(w, http.StatusBadRequest, `type must be "counter" or "gset"`)
		return
	}
	if len(req.Ops) == 0 {
		writeError(w, http.StatusBadRequest, "ops must be a non-empty array")
		return
	}

	ops := make([]store.CRDTOp, len(req.Ops))
	seen := make(map[string]struct{}, len(req.Ops))
	for i, op := range req.Ops {
		if op.ID == "" {
			writeError(w, http.StatusBadRequest, "each op must have a non-empty string id")
			return
		}
		if _, dup := seen[op.ID]; dup {
			writeError(w, http.StatusBadRequest, "duplicate op id within batch: "+op.ID)
			return
		}
		seen[op.ID] = struct{}{}

		crdtOp := store.CRDTOp{ID: op.ID, DeviceID: req.DeviceID}
		switch req.Type {
		case store.CRDTTypeCounter:
			v, ok := parseNonNegativeInt(op.Value)
			if !ok {
				writeError(w, http.StatusBadRequest, "each counter op must carry an integer value >= 0")
				return
			}
			// Re-marshal the canonical integer so idempotency compares the
			// decoded value rather than the client's literal formatting.
			raw, _ := json.Marshal(v)
			crdtOp.Value = raw
		case store.CRDTTypeGSet:
			if len(op.Elements) == 0 {
				writeError(w, http.StatusBadRequest, "each gset op must add at least one element")
				return
			}
			for _, element := range op.Elements {
				if element == "" {
					writeError(w, http.StatusBadRequest, "gset elements must be non-empty strings")
					return
				}
			}
			crdtOp.Elements = op.Elements
		}
		ops[i] = crdtOp
	}

	results, err := s.SubmitCRDTOps(documentID, req.Type, ops)
	if err != nil {
		var conflict *store.ErrCRDTConflict
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

// handleCRDTState returns the document's current type and merged result. A
// document with no committed operations has no state yet and answers 404.
func handleCRDTState(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	state, err := s.GetCRDTState(documentID)
	if err != nil {
		if errors.Is(err, store.ErrCRDTNotFound) {
			writeError(w, http.StatusNotFound, "crdt state not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load crdt state")
		return
	}

	// Marshal through a concrete shape so the value stays a native JSON number
	// or array rather than an escaped blob.
	writeJSON(w, http.StatusOK, map[string]any{
		"type":  state.Type,
		"value": json.RawMessage(state.Value),
	})
}

// malformedCRDTPath reports whether p targets the CRDT namespace but is not at
// one of the two exact endpoints:
//
//	POST/GET /v1/documents/{documentID}/crdt/ops
//	GET      /v1/documents/{documentID}/crdt/state
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
			if len(segs) != 3 || segs[0] == "" {
				return true
			}
			switch segs[2] {
			case "ops", "state":
				return false
			default:
				return true
			}
		}
	}
	return false
}
