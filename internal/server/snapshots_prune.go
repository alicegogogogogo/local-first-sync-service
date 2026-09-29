package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// snapshotPruneRequest is the body of the snapshot retention prune: the
// calling device, using the same device field as the document-level write
// entries, and the number of most recent snapshots to keep.
type snapshotPruneRequest struct {
	DeviceID string          `json:"deviceId"`
	Keep     json.RawMessage `json:"keep"`
}

// snapshotPruneResponse is the success body of the prune. The struct field
// order fixes the key order (maxCursor, then removed), and the shared JSON
// writer emits it as one compact line with a trailing newline.
type snapshotPruneResponse struct {
	MaxCursor int64 `json:"maxCursor"`
	Removed   int64 `json:"removed"`
}

// handlePruneSnapshots is the snapshot retention entry:
//
//	POST /v1/documents/{documentID}/snapshots/prune
//
// It keeps the keep most recent snapshots of the document (the greatest keep
// snapshot cursors) and hard-deletes every older one in a single serialized
// transaction. The body carries the calling device with the same device
// contract as the document-level write entries and a positive-integer keep in
// 1..1000.
//
// The fixed verdict order is request shape (400), device existence (404),
// document permission (403), then parameter legality (400): a bad content
// type, malformed JSON, trailing content or an empty/wrong-typed deviceId is a
// 400 before the device is looked up; an unregistered device is then a 404 and
// a revoked device a 403, regardless of the keep value; only after the gate
// does an invalid keep become a 400. Every failure writes nothing and exposes
// no snapshot content.
//
// Success answers 200 with one compact JSON line naming the greatest cursor
// among the snapshots still stored (zero when none remain) and the number of
// snapshots this call deleted. A document without snapshots — an unknown one
// included — succeeds with both numbers zero and creates nothing; no more
// snapshots than keep deletes nothing. The prune is idempotent, allocates no
// cursor, writes no change record and leaves the compaction boundary and the
// change cursor space untouched.
func handlePruneSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req snapshotPruneRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	// The device field belongs to the request shape: it is validated before the
	// device existence check, exactly as on the other document-level writes.
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}

	result, err := s.PruneSnapshots(documentID, req.DeviceID, req.Keep)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.Is(err, store.ErrPermissionDenied):
			writeError(w, http.StatusForbidden, "device permission for this document has been revoked")
		case errors.Is(err, events.ErrPruneKeepInvalid):
			writeError(w, http.StatusBadRequest, "keep must be an integer between 1 and 1000")
		default:
			writeError(w, http.StatusInternalServerError, "failed to prune snapshots")
		}
		return
	}

	writeJSON(w, http.StatusOK, snapshotPruneResponse{MaxCursor: result.MaxCursor, Removed: result.Removed})
}
