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

// crdtCompactRequest is the body of POST .../crdt/compact.
type crdtCompactRequest struct {
	DeviceID string `json:"deviceId"`
}

// handleCRDTCompact trims the document's stored CRDT operations and
// tombstones down to the ones still participating in the merge.
//
// The request contract matches the CRDT submission endpoint: strict
// application/json, exactly one JSON value, no trailing content and a
// non-empty deviceId — any violation is a 400 JSON error with zero writes.
// The device gate then applies in the submission's order: an unregistered
// device is a 404, a device whose permission for the document was revoked is
// a 403, and a document with no committed CRDT operation is a 404; none of
// them writes anything or leaks state content.
//
// On success the answer is 200 with the post-compaction snapshot, rendered
// byte-for-byte like a snapshot read immediately afterward (writeCRDTSnapshot).
// Compaction is idempotent — a repeat trims nothing and returns the same
// body — and touches only the CRDT state layer: it allocates no change
// cursor, writes no change record and, because the merged value never moves,
// pushes no notification.
func handleCRDTCompact(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req crdtCompactRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}

	snapshot, err := s.CompactCRDT(documentID, req.DeviceID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, crdt.ErrNotFound):
			writeError(w, http.StatusNotFound, "crdt state not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to compact crdt state")
		}
		return
	}

	writeCRDTSnapshot(w, snapshot)
}

// handleCRDTSnapshot returns the document's merged CRDT state together with
// the number of stored operations and tombstones the merge is derived from.
// The read carries no device identity and adds no authentication: like the
// document-level state read it is open, and a document with no committed CRDT
// operation answers the same 404 JSON as the state read. The read is pure —
// no cursor, no change record, no subscription.
func handleCRDTSnapshot(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	snapshot, err := s.GetCRDTSnapshot(documentID)
	if err != nil {
		if errors.Is(err, crdt.ErrNotFound) {
			writeError(w, http.StatusNotFound, "crdt state not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load crdt snapshot")
		return
	}

	writeCRDTSnapshot(w, snapshot)
}

// writeCRDTSnapshot renders a CRDT snapshot exactly as both snapshot surfaces
// must emit it: compact single-line JSON with the keys in type, value,
// operations, tombstones order, terminated by a newline, so the compaction
// response and the snapshot read can be compared byte-for-byte.
func writeCRDTSnapshot(w http.ResponseWriter, snapshot crdt.Snapshot) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(marshalCRDTSnapshot(snapshot))
}

// marshalCRDTSnapshot encodes a CRDT snapshot the single way both surfaces
// emit it: compact single-line JSON with the keys in type, value, operations,
// tombstones order and one trailing newline. The merged value is embedded as
// native JSON (a number, an array, or a register's arbitrary JSON value)
// rather than an escaped blob, and the two counts are non-negative integers.
// The key order is fixed by construction — not by the encoder's map ordering
// — so the bytes are stable across every reader.
func marshalCRDTSnapshot(snapshot crdt.Snapshot) []byte {
	var buf strings.Builder
	typeRaw, _ := json.Marshal(snapshot.Type)
	buf.WriteString(`{"type":`)
	_, _ = buf.Write(typeRaw)
	buf.WriteString(`,"value":`)
	_, _ = buf.Write(snapshot.Value)
	buf.WriteString(`,"operations":`)
	buf.WriteString(strconv.FormatInt(snapshot.Operations, 10))
	buf.WriteString(`,"tombstones":`)
	buf.WriteString(strconv.FormatInt(snapshot.Tombstones, 10))
	buf.WriteString("}\n")
	return []byte(buf.String())
}
