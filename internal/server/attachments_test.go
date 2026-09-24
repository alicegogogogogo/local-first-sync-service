package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// createAttachment posts a create request and returns the recorder.
func createAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string, total, chunk int64, digest string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/devices/"+deviceID+"/attachments", map[string]any{
		"attachmentId": attachmentID,
		"totalBytes":   total,
		"chunkSize":    chunk,
		"sha256":       digest,
	})
}

// mustCreateAttachment registers a fresh attachment whose digest matches
// content, failing the test on any error.
func mustCreateAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string, content []byte, chunkSize int64) {
	t.Helper()
	w, body := createAttachment(t, h, deviceID, attachmentID, int64(len(content)), chunkSize, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create attachment = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

func putChunk(t *testing.T, h http.Handler, deviceID, attachmentID string, index int64, data []byte) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut,
		fmt.Sprintf("/v1/devices/%s/attachments/%s/chunks/%d", deviceID, attachmentID, index),
		strings.NewReader(string(data)))
	r.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &body)
	}
	return w, body
}

func completeAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost,
		"/v1/devices/"+deviceID+"/attachments/"+attachmentID+"/complete", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &body)
	}
	return w, body
}

func TestCreateAttachmentHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	digest := sha256Hex([]byte("hello world"))
	w, body := createAttachment(t, h, "dev-1", "att-1", 11, 4, digest)
	if w.Code != http.StatusOK || body["created"] != true || body["attachmentId"] != "att-1" {
		t.Fatalf("create = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Identical retry by the same device is idempotent.
	w, body = createAttachment(t, h, "dev-1", "att-1", 11, 4, digest)
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("retry = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

func TestCreateAttachmentRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	digest := sha256Hex([]byte("x"))

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"attachmentId":"a","totalBytes":1,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":1,"sha256":"` + digest + `"}x`},
		{"missing attachmentId", "application/json", `{"totalBytes":1,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"empty attachmentId", "application/json", `{"attachmentId":"","totalBytes":1,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"numeric attachmentId", "application/json", `{"attachmentId":1,"totalBytes":1,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"missing totalBytes", "application/json", `{"attachmentId":"a","chunkSize":1,"sha256":"` + digest + `"}`},
		{"zero totalBytes", "application/json", `{"attachmentId":"a","totalBytes":0,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"negative totalBytes", "application/json", `{"attachmentId":"a","totalBytes":-3,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"fractional totalBytes", "application/json", `{"attachmentId":"a","totalBytes":1.5,"chunkSize":1,"sha256":"` + digest + `"}`},
		{"string totalBytes", "application/json", `{"attachmentId":"a","totalBytes":"3","chunkSize":1,"sha256":"` + digest + `"}`},
		{"missing chunkSize", "application/json", `{"attachmentId":"a","totalBytes":1,"sha256":"` + digest + `"}`},
		{"zero chunkSize", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":0,"sha256":"` + digest + `"}`},
		{"missing sha256", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":1}`},
		{"short sha256", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":1,"sha256":"abc"}`},
		{"uppercase sha256", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":1,"sha256":"` + strings.ToUpper(digest) + `"}`},
		{"non-hex sha256", "application/json", `{"attachmentId":"a","totalBytes":1,"chunkSize":1,"sha256":"` + strings.Repeat("g", 64) + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, "/v1/devices/dev-1/attachments", tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 body=%s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: none of the rejected creates left anything behind.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/a", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("GET after rejected creates = %d, want 404", w.Code)
	}
}

func TestCreateAttachmentDeviceAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	digest := sha256Hex([]byte("data"))

	// Unregistered device: 404.
	w, _ := createAttachment(t, h, "ghost", "att-1", 4, 2, digest)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}

	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)

	// Same id, other device, identical metadata: 409, original unchanged.
	w, _ = createAttachment(t, h, "dev-2", "att-1", 4, 2, digest)
	if w.Code != http.StatusConflict {
		t.Fatalf("cross-device create = %d, want 409", w.Code)
	}
	assertJSONError(t, w)

	// Same device, different metadata: 409.
	w, _ = createAttachment(t, h, "dev-1", "att-1", 8, 2, digest)
	if w.Code != http.StatusConflict {
		t.Fatalf("different metadata = %d, want 409", w.Code)
	}

	// The original record is intact.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != http.StatusOK || body["totalBytes"] != float64(4) || body["chunkSize"] != float64(2) {
		t.Fatalf("original record changed: %d %v", w.Code, body)
	}
}

func TestChunkUploadHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// Out-of-order uploads are accepted.
	w, body := putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk 1 = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk 0 = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Re-submitting identical bytes is idempotent.
	w, body = putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("idempotent chunk = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Different bytes at the same index: 409, first content kept.
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("HELL"))
	if w.Code != http.StatusConflict {
		t.Fatalf("conflicting chunk = %d, want 409", w.Code)
	}
	assertJSONError(t, w)
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/0", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("chunk 0 after conflict = %d %q, want first content", w.Code, w.Body.String())
	}

	// Wrong content type.
	r = httptest.NewRequest(http.MethodPut, "/v1/devices/dev-1/attachments/att-1/chunks/2", strings.NewReader("rld"))
	r.Header.Set("Content-Type", "application/json")
	w = serveRecorder(h, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("json content type = %d, want 400", w.Code)
	}
	assertJSONError(t, w)

	// Index out of range.
	w, _ = putChunk(t, h, "dev-1", "att-1", 3, []byte("xxxx"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("index 3 = %d, want 400", w.Code)
	}

	// Non-final chunk with the wrong length.
	w, _ = putChunk(t, h, "dev-1", "att-1", 1, []byte("short"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("short non-final chunk = %d, want 400", w.Code)
	}

	// Final chunk beyond the declared total (4 bytes would reach 12 > 11).
	w, _ = putChunk(t, h, "dev-1", "att-1", 2, []byte("rld!"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized final chunk = %d, want 400", w.Code)
	}

	// Unknown attachment: 404. Other device: 403.
	w, _ = putChunk(t, h, "dev-1", "nope", 0, []byte("hell"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment = %d, want 404", w.Code)
	}
	w, _ = putChunk(t, h, "dev-2", "att-1", 2, []byte("rld"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator chunk = %d, want 403", w.Code)
	}

	// The failed writes above stored nothing: only chunks 0 and 1 exist.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	got := fmt.Sprint(meta["receivedChunks"])
	if got != "[0 1]" {
		t.Fatalf("receivedChunks = %v, want [0 1]", meta["receivedChunks"])
	}
}

func TestCompleteAttachmentHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// Non-creator cannot complete.
	w, _ := completeAttachment(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator complete = %d, want 403", w.Code)
	}
	// Unknown attachment.
	w, _ = completeAttachment(t, h, "dev-1", "nope")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown complete = %d, want 404", w.Code)
	}

	// Missing chunks: 409, upload stays resumable.
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	w, _ = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusConflict {
		t.Fatalf("incomplete = %d, want 409", w.Code)
	}
	assertJSONError(t, w)

	// Resume and finish.
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK {
		t.Fatalf("complete = %d body=%s", w.Code, w.Body.String())
	}
	if body["attachmentId"] != "att-1" || body["complete"] != true ||
		body["size"] != float64(11) || body["sha256"] != sha256Hex(content) || body["reused"] != false {
		t.Fatalf("complete body = %v", body)
	}

	// Repeating the finish returns the same result.
	w, body2 := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || fmt.Sprint(body) != fmt.Sprint(body2) {
		t.Fatalf("repeat complete = %d %v, want %v", w.Code, body2, body)
	}

	// Metadata now reports completion.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	if meta["complete"] != true {
		t.Fatalf("meta complete = %v", meta["complete"])
	}
}

func TestCompleteAttachmentDigestMismatch(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Declare a digest that does not match the content that will arrive.
	w, body := createAttachment(t, h, "dev-1", "att-1", 3, 3, sha256Hex([]byte("xyz")))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create = %d %v", w.Code, body)
	}
	putChunk(t, h, "dev-1", "att-1", 0, []byte("abc"))

	w, _ = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("digest mismatch = %d, want 422", w.Code)
	}
	assertJSONError(t, w)

	// The upload stays incomplete.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	if meta["complete"] != false {
		t.Fatalf("complete after digest mismatch = %v", meta["complete"])
	}
}

func TestCompleteAttachmentLengthMismatch(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Final chunk shorter than the declared total: accepted at upload time
	// (it does not exceed the declared range) but the finish is a 409 and the
	// upload stays resumable.
	w, body := createAttachment(t, h, "dev-1", "att-1", 6, 4, sha256Hex([]byte("abcdef")))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create = %d %v", w.Code, body)
	}
	putChunk(t, h, "dev-1", "att-1", 0, []byte("abcd"))
	w, _ = putChunk(t, h, "dev-1", "att-1", 1, []byte("e")) // 1 byte, declared remainder is 2
	if w.Code != http.StatusOK {
		t.Fatalf("short final chunk = %d, want 200", w.Code)
	}
	w, _ = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusConflict {
		t.Fatalf("length mismatch = %d, want 409", w.Code)
	}
}

func TestAttachmentContentReuse(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("shared content")

	mustCreateAttachment(t, h, "dev-1", "att-1", content, 8)
	putChunk(t, h, "dev-1", "att-1", 0, content[:8])
	putChunk(t, h, "dev-1", "att-1", 1, content[8:])
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["reused"] != false {
		t.Fatalf("first complete = %d %v", w.Code, body)
	}

	// A second attachment (another device) with the same digest and size
	// reuses the stored content instead of copying bytes.
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 8)
	putChunk(t, h, "dev-2", "att-2", 0, content[:8])
	putChunk(t, h, "dev-2", "att-2", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("second complete = %d %v, want reused=true", w.Code, body)
	}
	// The reuse marker is stable across repeats.
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("repeat complete = %d %v, want reused=true", w.Code, body)
	}
}

func TestGetAttachmentAndChunkHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	// Metadata: original fields, received indices, completion status.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("get meta = %d body=%s", w.Code, w.Body.String())
	}
	var meta map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &meta)
	if meta["attachmentId"] != "att-1" || meta["totalBytes"] != float64(11) ||
		meta["chunkSize"] != float64(4) || meta["sha256"] != sha256Hex(content) ||
		meta["complete"] != false || fmt.Sprint(meta["receivedChunks"]) != "[0 2]" {
		t.Fatalf("meta = %v", meta)
	}

	// Chunk bytes round-trip.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/2", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusOK || w.Body.String() != "rld" ||
		w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("get chunk = %d %q %q", w.Code, w.Body.String(), w.Header().Get("Content-Type"))
	}

	// Missing chunk: 404. Unknown attachment: 404. Non-creator: 403.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/1", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusNotFound {
		t.Fatalf("missing chunk = %d, want 404", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/nope/chunks/0", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment chunk = %d, want 404", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/0", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusForbidden {
		t.Fatalf("non-creator chunk read = %d, want 403", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-2/attachments/att-1", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusForbidden {
		t.Fatalf("non-creator meta read = %d, want 403", w.Code)
	}

	// Illegal index and empty identifiers: 400 JSON.
	for _, path := range []string{
		"/v1/devices/dev-1/attachments/att-1/chunks/-1",
		"/v1/devices/dev-1/attachments/att-1/chunks/x",
		"/v1/devices/dev-1/attachments//chunks/0",
		"/v1/devices/dev-1/attachments/att-1/chunks/",
	} {
		r = httptest.NewRequest(http.MethodGet, path, nil)
		if w = serveRecorder(h, r); w.Code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d, want 400", path, w.Code)
		}
	}
}

func TestGetChunkOutOfRangeIs400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world") // 11 bytes / chunk 4 -> 3 chunks (0..2)
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// A numeric index beyond the declared range is a 400 JSON error for a known
	// attachment, even though that chunk row simply does not exist.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/3", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("out-of-range known = %d, want 400 body=%s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// Same index on an unknown attachment stays 404, and on a non-creator 403.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/nope/chunks/3", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusNotFound {
		t.Fatalf("out-of-range unknown = %d, want 404", w.Code)
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/3", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusForbidden {
		t.Fatalf("out-of-range non-creator = %d, want 403", w.Code)
	}

	// An in-range-but-not-yet-received index is still a 404.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/0", nil)
	if w = serveRecorder(h, r); w.Code != http.StatusNotFound {
		t.Fatalf("missing in-range chunk = %d, want 404", w.Code)
	}
}

func TestAttachmentsSurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *store.Store) {
		t.Helper()
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	_ = s.Close()

	// Reopen: state, idempotency and dedup decisions are unchanged.
	h, s = open(t)
	defer func() { _ = s.Close() }()

	// Create retry is still idempotent.
	w, body := createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("create after restart = %d %v", w.Code, body)
	}
	// Chunk retry is still idempotent, conflict still detected.
	w, body = putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("chunk retry after restart = %d %v", w.Code, body)
	}
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("HELL"))
	if w.Code != http.StatusConflict {
		t.Fatalf("chunk conflict after restart = %d, want 409", w.Code)
	}
	// The upload resumes where it stopped and completes.
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true || body["reused"] != false {
		t.Fatalf("complete after restart = %d %v", w.Code, body)
	}
	_ = s.Close()

	// Completion and its result survive a second restart.
	h, s = open(t)
	defer func() { _ = s.Close() }()
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true || body["size"] != float64(11) {
		t.Fatalf("repeat complete after restart = %d %v", w.Code, body)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/2", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusOK || w.Body.String() != "rld" {
		t.Fatalf("chunk read after restart = %d %q", w.Code, w.Body.String())
	}
}

func TestAttachmentDedupSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *store.Store) {
		t.Helper()
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	content := []byte("shared content")

	// First process: finish two identical-content attachments so the second
	// reuses the first's stored bytes.
	h, s := open(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	for _, tc := range []struct {
		device, id string
	}{
		{"dev-1", "att-1"},
		{"dev-2", "att-2"},
	} {
		mustCreateAttachment(t, h, tc.device, tc.id, content, 8)
		putChunk(t, h, tc.device, tc.id, 0, content[:8])
		putChunk(t, h, tc.device, tc.id, 1, content[8:])
		w, body := completeAttachment(t, h, tc.device, tc.id)
		if w.Code != http.StatusOK {
			t.Fatalf("complete %s = %d body=%s", tc.id, w.Code, w.Body.String())
		}
		if body["reused"] != (tc.id == "att-2") {
			t.Fatalf("complete %s reused=%v", tc.id, body["reused"])
		}
	}
	_ = s.Close()

	// After restart the dedup decision and the stored result are unchanged:
	// repeating the second finish still reports reused=true, and a third
	// identical attachment also reuses without re-copying bytes.
	h, s = open(t)
	defer func() { _ = s.Close() }()

	w, body := completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true || body["size"] != float64(len(content)) {
		t.Fatalf("reused result after restart = %d %v", w.Code, body)
	}

	mustCreateAttachment(t, h, "dev-1", "att-3", content, 8)
	putChunk(t, h, "dev-1", "att-3", 0, content[:8])
	putChunk(t, h, "dev-1", "att-3", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-1", "att-3")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("third attachment after restart reused = %d %v, want reused=true", w.Code, body)
	}
}

// TestSealedAttachmentRejectsChunksHTTP seals an upload and then verifies that
// every further chunk write — identical bytes, different bytes, an index that
// was never uploaded — is a 409 JSON error that changes nothing, and that the
// rejection and the original bytes survive a restart.
func TestSealedAttachmentRejectsChunksHTTP(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *store.Store) {
		t.Helper()
		s, err := store.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}

	assertSealed := func(t *testing.T, h http.Handler) {
		t.Helper()
		// A byte-identical resubmission is no longer idempotent: 409 JSON.
		w, _ := putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
		if w.Code != http.StatusConflict {
			t.Fatalf("identical chunk after seal = %d, want 409 body=%s", w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		// Different bytes at the same index: still 409, first content kept.
		w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("HELL"))
		if w.Code != http.StatusConflict {
			t.Fatalf("conflicting chunk after seal = %d, want 409", w.Code)
		}
		assertJSONError(t, w)
		// An index that was never uploaded is rejected as well.
		w, _ = putChunk(t, h, "dev-1", "att-1", 3, []byte("xxxx"))
		if w.Code != http.StatusConflict {
			t.Fatalf("new index after seal = %d, want 409", w.Code)
		}
		// The stored content and metadata are unchanged.
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/0", nil)
		w = serveRecorder(h, r)
		if w.Code != http.StatusOK || w.Body.String() != "hell" {
			t.Fatalf("chunk 0 after rejected writes = %d %q", w.Code, w.Body.String())
		}
		r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
		w = serveRecorder(h, r)
		var meta map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &meta)
		if meta["complete"] != true || fmt.Sprint(meta["receivedChunks"]) != "[0 1 2]" {
			t.Fatalf("meta after rejected writes = %v", meta)
		}
	}
	assertSealed(t, h)
	_ = s.Close()

	// After a restart the seal still holds and the original bytes are intact.
	h, s = open(t)
	defer func() { _ = s.Close() }()
	assertSealed(t, h)
	// The recorded finish result is unchanged across the restart.
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true || body["reused"] != false ||
		body["size"] != float64(11) || body["sha256"] != sha256Hex(content) {
		t.Fatalf("repeat complete after restart = %d %v", w.Code, body)
	}
}
