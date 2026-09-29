package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// pruneSnapshotsRequest is the body of POST .../snapshots/prune: the same
// device field every document-level write entry carries, plus the retention
// count. Keep is captured as raw JSON so the service can enforce its legality
// after the registration/permission gate in the prune transaction — a missing
// keep against an unknown device must still report the 404 first.
type pruneSnapshotsRequest struct {
	DeviceID string          `json:"deviceId"`
	Keep     json.RawMessage `json:"keep"`
}

// pruneSnapshotsResponse is the success body of POST .../snapshots/prune. The
// struct field order fixes the key order (maxCursor, then deleted), and the
// shared JSON writer emits it as one compact line with a trailing newline.
type pruneSnapshotsResponse struct {
	MaxCursor int64 `json:"maxCursor"`
	Deleted   int64 `json:"deleted"`
}

// handlePruneSnapshots is the document snapshot retention entry, mounted one
// prune segment below the snapshot collection:
//
//	POST /v1/documents/{documentID}/snapshots/prune
//
// It keeps the most recent keep snapshots (highest cursors) and hard-deletes
// every older one. The strict request contract matches every document-level
// write entry: application/json, exactly one JSON value, no trailing content
// and a non-empty deviceId — any violation is a 400 JSON error with zero
// writes. The fixed verdict order then runs in the prune transaction: an
// unregistered device is a 404, a revoked one a 403, and only after both is
// keep validated as an integer in 1..1000 (a missing, fractional, string,
// boolean, null, zero or negative value is a 400 with zero writes), so the
// gate failure always wins for a malformed argument.
//
// On success the answer is 200 with one compact JSON line naming the greatest
// snapshot cursor still stored (zero when none remain) and the number of
// snapshots this call removed. A snapshot-less or unknown document succeeds
// with both zero and creates nothing; keeping at least as many snapshots as
// exist removes nothing. The prune is idempotent, allocates no cursor, moves
// no compaction boundary, leaves restore provenance and version markers and
// wakes no waiter or subscriber.
func handlePruneSnapshots(s *app.App, w http.ResponseWriter, r *http.Request) {
	documentID := r.PathValue("documentID") // route pattern + guard guarantee non-empty

	var req pruneSnapshotsRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
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
		case errors.Is(err, events.ErrInvalidKeep):
			writeError(w, http.StatusBadRequest, "keep must be an integer between 1 and 1000")
		default:
			writeError(w, http.StatusInternalServerError, "failed to prune snapshots")
		}
		return
	}

	writeJSON(w, http.StatusOK, pruneSnapshotsResponse{MaxCursor: result.MaxCursor, Deleted: result.Deleted})
}
