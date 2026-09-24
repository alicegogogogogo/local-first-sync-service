package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// maxChunkBytes caps a single inbound binary chunk.
const maxChunkBytes = 64 << 20

// octetStream is the required Content-Type of a binary chunk body.
const octetStream = "application/octet-stream"

type attachmentCreateRequest struct {
	AttachmentID string          `json:"attachmentId"`
	Size         json.RawMessage `json:"size"`
	ChunkSize    json.RawMessage `json:"chunkSize"`
	SHA256       string          `json:"sha256"`
}

// registerAttachmentRoutes mounts the resumable attachment surface under the
// per-device path. The device id in the path identifies the creator; no other
// authentication is involved. All failures answer JSON.
func registerAttachmentRoutes(mux *http.ServeMux, s *store.Store) {
	mux.HandleFunc("POST /v1/devices/{deviceId}/attachments", func(w http.ResponseWriter, r *http.Request) {
		handleCreateAttachment(s, w, r)
	})
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}", func(w http.ResponseWriter, r *http.Request) {
		handleGetAttachment(s, w, r)
	})
	mux.HandleFunc("PUT /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}", func(w http.ResponseWriter, r *http.Request) {
		handlePutChunk(s, w, r)
	})
	mux.HandleFunc("GET /v1/devices/{deviceId}/attachments/{attachmentId}/chunks/{index}", func(w http.ResponseWriter, r *http.Request) {
		handleGetChunk(s, w, r)
	})
	mux.HandleFunc("POST /v1/devices/{deviceId}/attachments/{attachmentId}/complete", func(w http.ResponseWriter, r *http.Request) {
		handleCompleteAttachment(s, w, r)
	})
}

// handleCreateAttachment accepts the declared metadata of a new upload as JSON.
// Missing/mistyped/non-positive fields or a malformed digest are a 400 JSON
// error with zero writes; an unregistered device is a 404. Re-posting the same
// device + identical metadata is idempotent (created=false); an id owned by
// another device is a 409 and leaves the original record untouched.
func handleCreateAttachment(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId") // route pattern + guard guarantee non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/json")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBatchBytes)
	var req attachmentCreateRequest
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: unexpected trailing content")
		return
	}

	if req.AttachmentID == "" {
		writeError(w, http.StatusBadRequest, "attachmentId must be a non-empty string")
		return
	}
	size, ok := parsePositiveJSONInt(req.Size)
	if !ok {
		writeError(w, http.StatusBadRequest, "size must be a positive integer")
		return
	}
	chunkSize, ok := parsePositiveJSONInt(req.ChunkSize)
	if !ok {
		writeError(w, http.StatusBadRequest, "chunkSize must be a positive integer")
		return
	}
	if !isLowerHexSHA256(req.SHA256) {
		writeError(w, http.StatusBadRequest, "sha256 must be 64 lowercase hexadecimal characters")
		return
	}

	created, err := s.CreateAttachment(deviceID, req.AttachmentID, size, chunkSize, req.SHA256)
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

	writeJSON(w, http.StatusOK, map[string]any{
		"attachmentId": req.AttachmentID,
		"size":         size,
		"chunkSize":    chunkSize,
		"sha256":       req.SHA256,
		"created":      created,
	})
}

// handlePutChunk stores one binary chunk. The body must be
// application/octet-stream and the path index a decimal non-negative integer;
// either violation is a 400 before any lookup. The store then enforces
// existence (404), ownership (403), range/length (400) and same-index byte
// identity (409, first bytes retained); an identical re-post is a 200 no-op.
func handlePutChunk(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")         // route pattern + guard guarantee non-empty
	attachmentID := r.PathValue("attachmentId") // route pattern + guard guarantee non-empty

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != octetStream {
		writeError(w, http.StatusBadRequest, "Content-Type must be application/octet-stream")
		return
	}
	index, ok := parseCursorPath(r.PathValue("index"))
	if !ok {
		writeError(w, http.StatusBadRequest, "chunk index must be a non-negative integer")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxChunkBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "chunk body too large or unreadable: "+err.Error())
		return
	}

	if err := s.PutChunk(deviceID, attachmentID, index, data); err != nil {
		writeAttachmentError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"attachmentId": attachmentID,
		"index":        index,
		"bytes":        len(data),
	})
}

// handleCompleteAttachment seals a finished upload. Only the creator may call
// it (403 otherwise); missing chunks or a wrong total length leave the upload
// resumable (409), a digest mismatch leaves it unsealed (422), and a digest
// collision with a different sealed size is a 409. A successful seal reports
// id, size, digest, completed=true and whether content was reused; repeating
// it returns the same result.
func handleCompleteAttachment(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")
	attachmentID := r.PathValue("attachmentId")

	result, err := s.CompleteAttachment(deviceID, attachmentID)
	if err != nil {
		writeAttachmentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// handleGetAttachment returns the creator's view of an attachment: received
// chunk indices, completion state and the original declared metadata. A
// non-creator gets 403; an unknown id gets 404.
func handleGetAttachment(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")
	attachmentID := r.PathValue("attachmentId")

	meta, err := s.GetAttachmentMeta(deviceID, attachmentID)
	if err != nil {
		writeAttachmentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, meta)
}

// handleGetChunk streams one stored chunk back as raw bytes. A non-numeric
// index is a 400 before lookup; an unknown attachment or a not-yet-received
// chunk is a 404, and a non-creator is a 403.
func handleGetChunk(s *store.Store, w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("deviceId")
	attachmentID := r.PathValue("attachmentId")

	index, ok := parseCursorPath(r.PathValue("index"))
	if !ok {
		writeError(w, http.StatusBadRequest, "chunk index must be a non-negative integer")
		return
	}

	data, err := s.GetAttachmentChunk(deviceID, attachmentID, index)
	if err != nil {
		writeAttachmentError(w, err)
		return
	}
	w.Header().Set("Content-Type", octetStream)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// writeAttachmentError maps the store's attachment errors onto the documented
// HTTP status codes. Every response is a JSON error.
func writeAttachmentError(w http.ResponseWriter, err error) {
	var chunkConflict *store.ErrChunkConflict
	switch {
	case errors.Is(err, store.ErrAttachmentNotFound):
		writeError(w, http.StatusNotFound, "attachment or chunk not found")
	case errors.Is(err, store.ErrAttachmentForbidden):
		writeError(w, http.StatusForbidden, "attachment belongs to another device")
	case errors.Is(err, store.ErrAttachmentBadChunk):
		writeError(w, http.StatusBadRequest, "chunk index is out of range or body length does not match")
	case errors.As(err, &chunkConflict):
		writeError(w, http.StatusConflict, "chunk already exists with different bytes")
	case errors.Is(err, store.ErrAttachmentIncomplete):
		writeError(w, http.StatusConflict, "attachment is missing chunks or the assembled length does not match the declared size")
	case errors.Is(err, store.ErrAttachmentDigest):
		writeError(w, http.StatusUnprocessableEntity, "assembled content does not match the declared SHA-256 digest")
	case errors.Is(err, store.ErrAttachmentContentConflict):
		writeError(w, http.StatusConflict, "digest matches existing sealed content with a different size")
	default:
		writeError(w, http.StatusInternalServerError, "attachment operation failed")
	}
}

// parsePositiveJSONInt reports whether raw is a present JSON integer >= 1 and
// returns its value. Zero, floats, strings, booleans, null and negatives fail.
func parsePositiveJSONInt(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	return parsePositiveInt(raw)
}

// isLowerHexSHA256 reports whether digest is exactly 64 lowercase hex digits.
func isLowerHexSHA256(digest string) bool {
	if len(digest) != hex.EncodedLen(sha256Size) {
		return false
	}
	for _, c := range digest {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// sha256Size is SHA-256's output length in bytes.
const sha256Size = 32
