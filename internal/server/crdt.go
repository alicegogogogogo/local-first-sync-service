package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

type crdtOpIn struct {
	ID    string          `json:"id"`
	Value json.RawMessage `json:"value"`
}

type crdtSubmitRequest struct {
	DeviceID string     `json:"deviceId"`
	Type     string     `json:"type"`
	Ops      []crdtOpIn `json:"ops"`
}

// handleSubmitCRDT is the commit entry of the CRDT state layer:
//
//	POST /v1/documents/{documentID}/crdt/ops
//
// The body declares the document's CRDT type ("counter" or "gset"), fixed by
// the first accepted batch; a later batch declaring the other type is a 409
// with no write, so two concurrent declarations serialize and exactly one
// wins. The same registration/permission gate as replay applies first: an
// unregistered device is 404 and a revoked one 403, neither exposing state.
//
// Counter ops carry the device's cumulative contribution as a non-negative
// integer; a value behind the device's accepted maximum is a 409 and changes
// nothing. G-set ops carry a string element added to the document union.
// Every op has a stable id: an identical re-post (same device and value) is
// idempotent; an existing id with a different device or value is a 409 and
// the whole batch stays untouched.
func handleSubmitCRDT(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtSubmitRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	switch req.Type {
	case store.CRDTTypeCounter, store.CRDTTypeGSet:
	default:
		writeError(w, http.StatusBadRequest, `type must be "counter" or "gset"`)
		return
	}
	if req.Ops == nil {
		writeError(w, http.StatusBadRequest, "ops must be a non-empty array")
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

		switch req.Type {
		case store.CRDTTypeCounter:
			v, ok := parseNonNegativeInt(op.Value)
			if !ok {
				writeError(w, http.StatusBadRequest, "each counter op must carry a non-negative integer value")
				return
			}
			ops[i] = store.CRDTOp{ID: op.ID, Value: strconv.FormatInt(v, 10)}
		case store.CRDTTypeGSet:
			var member any
			if len(op.Value) == 0 || json.Unmarshal(op.Value, &member) != nil {
				writeError(w, http.StatusBadRequest, "each gset op must carry a string value")
				return
			}
			m, ok := member.(string)
			if !ok {
				// Rejects null, numbers, booleans, objects and arrays; only a
				// JSON string is a set element.
				writeError(w, http.StatusBadRequest, "each gset op must carry a string value")
				return
			}
			ops[i] = store.CRDTOp{ID: op.ID, Value: m}
		}
	}

	results, err := s.SubmitCRDT(documentID, req.DeviceID, req.Type, ops)
	if err != nil {
		var opConflict *store.ErrCRDTOpConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, store.ErrCRDTTypeConflict):
			writeError(w, http.StatusConflict, "document's crdt type is already fixed and cannot be changed")
		case errors.Is(err, store.ErrCRDTValueRejected):
			writeError(w, http.StatusConflict, "counter contribution must be monotonically non-decreasing for each device")
		case errors.As(err, &opConflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"error":      "op id already exists with a different deviceId or value",
				"conflictId": opConflict.ID,
			})
		default:
			writeError(w, http.StatusInternalServerError, "failed to commit crdt ops")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

// handleGetCRDTState is the read entry of the CRDT state layer:
//
//	GET /v1/documents/{documentID}/crdt/state
//
// It returns the document's fixed type and its current merged result: the sum
// of per-device maximum contributions for a counter, or the ascending element
// union for a grow-only set. A document that has never accepted a CRDT op is
// a 404 JSON error. The read is independent of the change log: it neither
// consumes nor advances a document cursor.
func handleGetCRDTState(s *store.Store, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	state, err := s.GetCRDTState(documentID)
	if err != nil {
		if errors.Is(err, store.ErrCRDTNotFound) {
			writeError(w, http.StatusNotFound, "crdt state not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to read crdt state")
		return
	}

	var value any
	switch state.Type {
	case store.CRDTTypeCounter:
		value = state.Counter
	case store.CRDTTypeGSet:
		value = state.Members
	}
	writeJSON(w, http.StatusOK, map[string]any{"type": state.Type, "value": value})
}
