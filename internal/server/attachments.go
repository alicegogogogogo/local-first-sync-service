package server

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// maxChunkBytes caps a single inbound chunk body.
const maxChunkBytes = 64 << 20

type attachmentRequest struct {
	ID         string          `json:"attachmentId"`
	TotalBytes json.RawMessage `json:"totalBytes"`
	ChunkSize  json.RawMessage `json:"chunkSize"`
	SHA256     string          `json:"sha256"`
}

// attachmentListEntry is one item of the attachment listing response.
type attachmentListEntry struct {
	ID             string  `json:"attachmentId"`
	TotalBytes     int64   `json:"totalBytes"`
	ChunkSize      int64   `json:"chunkSize"`
	SHA256         string  `json:"sha256"`
	Complete       bool    `json:"complete"`
	Owned          bool    `json:"owned"`
	ReceivedChunks []int64 `json:"receivedChunks"`
}

// handleListAttachments is the read-only listing over the attachment
// collection: every upload the device in the path created plus every upload
// another device created and granted it read access to, each at most once,
// ordered by creation ascending. limit (1..1000, default 100) and offset
// (non-negative, default 0) page the result; an illegal value is a 400 JSON
// error checked before the device lookup. An unregistered device is a 404
// JSON error with no listing. The handler writes nothing: no attachment,
// chunk or access state changes.
func handleListAttachments(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	items, err := s.ListAttachments(deviceID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list attachments")
		}
		return
	}

	entries := make([]attachmentListEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, attachmentListEntry{
			ID:             item.ID,
			TotalBytes:     item.TotalBytes,
			ChunkSize:      item.ChunkSize,
			SHA256:         item.SHA256,
			Complete:       item.Complete,
			Owned:          item.Owned,
			ReceivedChunks: item.ReceivedChunks,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"attachments": entries})
}

// parseAttachmentListQuery parses the limit/offset pagination parameters of
// the attachment listing, writing the documented 400 on an illegal value.
func parseAttachmentListQuery(w http.ResponseWriter, r *http.Request) (limit, offset int64, ok bool) {
	q := r.URL.Query()

	limit = defaultListLimit
	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 1 || v > maxListLimit {
			writeError(w, http.StatusBadRequest, "limit must be an integer between 1 and 1000")
			return 0, 0, false
		}
		limit = v
	}

	offset = 0
	if raw := q.Get("offset"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return 0, 0, false
		}
		offset = v
	}

	return limit, offset, true
}

// handleCreateAttachment registers a resumable chunked upload for the device
// in the path. The body is strict JSON carrying the stable attachment id, the
// declared total size and chunk size (positive integers) and the lowercase
// hex SHA-256 of the final content. Any malformed input is a 400 JSON error
// and writes nothing; an unregistered device is a 404; an id already taken —
// by another device, or by this device with different metadata — is a 409
// that leaves the original record untouched.
func handleCreateAttachment(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	var req attachmentRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "attachmentId must be a non-empty string")
		return
	}
	totalBytes, ok := parsePositiveInt(req.TotalBytes)
	if !ok {
		writeError(w, http.StatusBadRequest, "totalBytes must be a positive integer")
		return
	}
	chunkSize, ok := parsePositiveInt(req.ChunkSize)
	if !ok {
		writeError(w, http.StatusBadRequest, "chunkSize must be a positive integer")
		return
	}
	if !isLowerHexSHA256(req.SHA256) {
		writeError(w, http.StatusBadRequest, "sha256 must be 64 lowercase hexadecimal characters")
		return
	}

	created, err := s.CreateAttachment(deviceID, store.Attachment{
		ID:         req.ID,
		TotalBytes: totalBytes,
		ChunkSize:  chunkSize,
		SHA256:     req.SHA256,
	})
	if err != nil {
		var conflict *store.ErrAttachmentConflict
		switch {
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "attachmentId already exists with a different owner or metadata: "+conflict.ID)
		default:
			writeError(w, http.StatusInternalServerError, "failed to create attachment")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"attachmentId": req.ID, "created": created})
}

// handlePutChunk stores one binary chunk of an upload. The request must be
// application/octet-stream; the index is zero-based and chunks may arrive in
// any order. An unknown attachment is a 404 and a non-creator device a 403; a
// wrong content type, an out-of-range index, a short non-final chunk or a
// final chunk beyond the declared total is a 400 — none of these write
// anything. Re-submitting identical bytes is idempotent; different bytes for
// the same index are a 409 and the first content is kept. Once the upload is
// finished it is sealed: every further chunk write, even a byte-identical
// one, is a 409 and the stored content is unchanged.
func handlePutChunk(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/octet-stream" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/octet-stream")
		return
	}

	index, ok := parseCursorPath(r.PathValue("index"))
	if !ok {
		writeError(w, http.StatusBadRequest, "chunk index must be a non-negative integer")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxChunkBytes+1)
	data, err := io.ReadAll(r.Body)
	if err != nil || int64(len(data)) > maxChunkBytes {
		writeError(w, http.StatusBadRequest, "chunk body is too large")
		return
	}

	created, err := s.PutChunk(deviceID, attachmentID, index, data)
	if err != nil {
		var invalid *store.ErrChunkInvalid
		var conflict *store.ErrChunkConflict
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		case errors.Is(err, store.ErrAttachmentSealed):
			writeError(w, http.StatusConflict, "attachment is complete and no longer accepts chunks")
		case errors.As(err, &invalid):
			writeError(w, http.StatusBadRequest, invalid.Reason)
		case errors.As(err, &conflict):
			writeError(w, http.StatusConflict, "chunk already exists with different content")
		default:
			writeError(w, http.StatusInternalServerError, "failed to store chunk")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"index": index, "created": created})
}

// handleCompleteAttachment seals an upload. Only the creating device may
// finish it (others get a 403; an unknown attachment a 404). Missing chunks
// or a total-length mismatch are a 409 and leave the upload resumable; a
// digest mismatch is a 422 and leaves it incomplete. When another finished
// attachment already holds the same digest and size, the content is reused
// without copying bytes (reused=true); the same digest at a different size is
// a 409. A successful or repeated finish returns the same recorded result.
func handleCompleteAttachment(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	result, err := s.CompleteAttachment(deviceID, attachmentID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		case errors.Is(err, store.ErrAttachmentIncomplete):
			writeError(w, http.StatusConflict, "chunks are missing or do not add up to the declared total")
		case errors.Is(err, store.ErrAttachmentDigestMismatch):
			writeError(w, http.StatusUnprocessableEntity, "assembled content does not match the declared sha256")
		case errors.Is(err, store.ErrAttachmentDigestConflict):
			writeError(w, http.StatusConflict, "a completed content with the same digest but a different size exists")
		default:
			writeError(w, http.StatusInternalServerError, "failed to complete attachment")
		}
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// handleGetAttachment returns the reader's view of an upload: the original
// metadata, the sorted indices received so far and the completion status. The
// creator and any device granted access through the access endpoint see the
// same body; any other device gets a 403, an unknown attachment a 404.
func handleGetAttachment(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	a, indices, err := s.GetAttachment(deviceID, attachmentID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		default:
			writeError(w, http.StatusInternalServerError, "failed to load attachment")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"attachmentId":   a.ID,
		"totalBytes":     a.TotalBytes,
		"chunkSize":      a.ChunkSize,
		"sha256":         a.SHA256,
		"complete":       a.Complete,
		"receivedChunks": indices,
	})
}

// handleGetChunk streams one stored chunk back to the creator or a granted
// reader. An empty identifier is rejected by the path guard; an illegal index
// is a 400; an unknown attachment or a chunk that never arrived is a 404; any
// other device is a 403.
func handleGetChunk(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	index, ok := parseCursorPath(r.PathValue("index"))
	if !ok {
		writeError(w, http.StatusBadRequest, "chunk index must be a non-negative integer")
		return
	}

	data, err := s.GetAttachmentChunk(deviceID, attachmentID, index)
	if err != nil {
		var invalid *store.ErrChunkInvalid
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrChunkNotFound):
			writeError(w, http.StatusNotFound, "chunk not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		case errors.As(err, &invalid):
			writeError(w, http.StatusBadRequest, invalid.Reason)
		default:
			writeError(w, http.StatusInternalServerError, "failed to load chunk")
		}
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleGetAttachmentContent returns the whole content of a sealed upload in
// one read: the stored chunks concatenated in ascending index order, byte for
// byte what the per-chunk reads return one piece at a time. The path device
// id is the caller identity — there is no other authentication — and the
// creator and any granted device see the same bytes. The judgments run in a
// fixed order: the path shape first (an empty identifier, a missing or extra
// segment and any non-GET method are a 400 JSON error enforced by the routing
// layer, all with zero writes), then an unknown or deleted attachment is a
// 404 JSON error, a caller that is neither the creator nor a granted reader
// is a 403 JSON error that leaks not a single byte, and an upload that is not
// sealed yet is a 409 JSON error that leaves the upload resumable. The
// content is assembled inside one serialized transaction, so a delete racing
// the read yields either the complete bytes or a 404, never half of the
// content. The endpoint is read-only: it writes nothing, allocates no cursor
// and notifies no subscriber, and repeated reads return the same bytes.
func handleGetAttachmentContent(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	data, err := s.GetAttachmentContent(deviceID, attachmentID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		case errors.Is(err, store.ErrAttachmentNotSealed):
			writeError(w, http.StatusConflict, "attachment is not sealed yet")
		default:
			writeError(w, http.StatusInternalServerError, "failed to load attachment content")
		}
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleListAttachmentAccess is the read-only roster over an attachment's
// access subresource: the device ids the attachment is currently open to for
// reading — every device the creator granted through the access endpoint and
// that has not been revoked — each at most once, ordered by the time their
// latest grant took effect ascending. The creator itself is not part of the
// roster (its reads come from ownership, so a self grant/revoke neither adds
// nor removes an entry). Only the attachment's creator may read it — the
// device in the path is the caller identity and there is no other
// authentication: an unknown or deleted attachment is a 404 and a non-creator
// caller a 403, judged independently. Pagination shares the attachment
// listing's limit/offset parsing, so an illegal value is a 400 checked before
// the attachment lookup. The handler writes nothing: no access row, chunk or
// attachment state changes.
func handleListAttachmentAccess(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	limit, offset, ok := parseAttachmentListQuery(w, r)
	if !ok {
		return
	}

	devices, err := s.ListAttachmentAccess(deviceID, attachmentID, limit, offset)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		default:
			writeError(w, http.StatusInternalServerError, "failed to list attachment access")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"devices": devices})
}

// handleSetAttachmentAccess grants or revokes a registered device's read
// access to one attachment. Every (attachment, device) pair starts
// unauthorized, so the first grant is the first write; repeating the current
// state is idempotent (changed=false). Only the creator may change access: a
// non-creator caller gets a 403 and an unknown attachment a 404, neither
// touching any access state. A rejected request (bad content type, malformed
// JSON, trailing content, empty or mistyped fields, unknown action) is a 400
// JSON error and writes nothing; an unregistered target device is a 404 JSON
// error. A granted device reads the same metadata and chunk bytes as the
// creator; a revoke returns its reads to 403 at once and never deletes
// stored chunks or sealed content.
func handleSetAttachmentAccess(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	var req permissionRequest
	if !decodeJSONBody(w, r, &req) {
		return
	}
	if req.DeviceID == "" {
		writeError(w, http.StatusBadRequest, "deviceId must be a non-empty string")
		return
	}
	var authorized bool
	switch req.Action {
	case "grant":
		authorized = true
	case "revoke":
		authorized = false
	default:
		writeError(w, http.StatusBadRequest, `action must be "grant" or "revoke"`)
		return
	}

	changed, err := s.SetAttachmentAccess(deviceID, attachmentID, req.DeviceID, authorized)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		case errors.Is(err, store.ErrDeviceNotFound):
			writeError(w, http.StatusNotFound, "device not found")
		default:
			writeError(w, http.StatusInternalServerError, "failed to update attachment access")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deviceId":   req.DeviceID,
		"authorized": authorized,
		"changed":    changed,
	})
}

// handleDeleteAttachment removes an upload outright. Only the creating device
// may delete: any other device gets a 403 and an unknown (or already deleted)
// id a 404, neither writing anything. A successful delete answers the
// attachment id and a deletion marker, after which the record, its chunks,
// its completion state and every access grant are gone: every later read,
// chunk write, finish or access change against the id is a 404, and
// re-creating the id starts a brand-new upload. Digest-addressed content is
// reclaimed only when no finished attachment references it any longer.
func handleDeleteAttachment(s *app.App, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	if err := s.DeleteAttachment(deviceID, attachmentID); err != nil {
		switch {
		case errors.Is(err, store.ErrAttachmentNotFound):
			writeError(w, http.StatusNotFound, "attachment not found")
		case errors.Is(err, store.ErrAttachmentForbidden):
			writeError(w, http.StatusForbidden, "attachment belongs to another device")
		default:
			writeError(w, http.StatusInternalServerError, "failed to delete attachment")
		}
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"attachmentId": attachmentID, "deleted": true})
}

// isLowerHexSHA256 reports whether s is exactly 64 lowercase hex characters.
func isLowerHexSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
