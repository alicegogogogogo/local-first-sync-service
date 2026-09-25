package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// The whole attachment path over the public HTTP surface: register a device,
// create the upload, push chunks out of order, retry them after a reconnect,
// seal, observe content dedup across devices and the seal's immutability.
func TestAttachmentLifecycleEndToEndContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]

	// Create: first attempt creates, an identical retry is idempotent.
	w, body := createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("create retry = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Chunks arrive out of order; each first store reports created=true.
	for _, c := range []struct {
		index int64
		data  string
	}{
		{2, "rld"},
		{0, "hell"},
		{1, "o wo"},
	} {
		w, body = putChunk(t, h, "dev-1", "att-1", c.index, []byte(c.data))
		if w.Code != http.StatusOK || body["created"] != true || body["index"] != float64(c.index) {
			t.Fatalf("chunk %d = %d %v body=%s", c.index, w.Code, body, w.Body.String())
		}
	}
	// A reconnect retries the last chunk: idempotent, created=false.
	w, body = putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("reconnect retry = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Seal: the recorded result carries the declared size and digest.
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true || body["reused"] != false ||
		body["size"] != float64(11) || body["sha256"] != sha256Hex(content) {
		t.Fatalf("complete = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// A second device uploading the same content reuses the stored bytes.
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 4)
	putChunk(t, h, "dev-2", "att-2", 0, []byte("hell"))
	putChunk(t, h, "dev-2", "att-2", 1, []byte("o wo"))
	putChunk(t, h, "dev-2", "att-2", 2, []byte("rld"))
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true || body["size"] != float64(11) {
		t.Fatalf("dedup complete = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// The seal is immutable: even a byte-identical chunk write is a 409.
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w.Code != http.StatusConflict {
		t.Fatalf("write after seal = %d, want 409", w.Code)
	}
	assertJSONError(t, w)
}

// Access and request-shape matrix for the attachment read/write endpoints:
// unknown ids are 404, non-creators are 403, illegal indices and empty
// identifiers are 400 — every one a JSON error that leaks no content.
func TestAttachmentAccessAndShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	// Unknown attachment metadata read: 404 JSON, no content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/nope", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("get unknown meta = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Non-creator metadata read: 403 JSON, no metadata leaked.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-2/attachments/att-1", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("get non-creator meta = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "hello") || strings.Contains(w.Body.String(), sha256Hex(content)) {
		t.Fatalf("403 body leaks content: %s", w.Body.String())
	}

	// Illegal chunk indices on the write path: 400 JSON.
	for _, index := range []string{"-1", "x", "1.5"} {
		r = httptest.NewRequest(http.MethodPut,
			"/v1/devices/dev-1/attachments/att-1/chunks/"+index, strings.NewReader("hell"))
		r.Header.Set("Content-Type", "application/octet-stream")
		w = serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("put chunk index %q = %d, want 400", index, w.Code)
		}
		assertJSONError(t, w)
	}

	// Empty identifiers and trailing slashes on the chunk paths: 400 JSON.
	for _, path := range []string{
		"/v1/devices//attachments/att-1/chunks/0",
		"/v1/devices/dev-1/attachments//chunks/0",
		"/v1/devices/dev-1/attachments/att-1/chunks/",
		"/v1/devices/dev-1/attachments/",
	} {
		r = httptest.NewRequest(http.MethodPut, path, strings.NewReader("hell"))
		r.Header.Set("Content-Type", "application/octet-stream")
		w = serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("PUT %s = %d, want 400", path, w.Code)
		}
		assertJSONError(t, w)
	}

	// None of the rejections stored anything: only chunk 0 was ever received.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	if !strings.Contains(w.Body.String(), `"receivedChunks":[0]`) {
		t.Fatalf("receivedChunks after rejections = %s", w.Body.String())
	}
}

// Finishing an upload that has no chunks at all is a 409 and the upload stays
// resumable: the chunks can still arrive afterwards and the finish succeeds.
func TestAttachmentCompleteWithNoChunksIs409AndResumable(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 2)

	w, _ := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusConflict {
		t.Fatalf("complete with no chunks = %d, want 409", w.Code)
	}
	assertJSONError(t, w)

	// The failed finish sealed nothing: chunks are still accepted.
	w, body := putChunk(t, h, "dev-1", "att-1", 0, []byte("da"))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk after 409 = %d %v body=%s", w.Code, body, w.Body.String())
	}
	putChunk(t, h, "dev-1", "att-1", 1, []byte("ta"))
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true || body["size"] != float64(4) {
		t.Fatalf("complete after resume = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

// A digest mismatch is a 422 that leaves the upload incomplete, and that
// judgment survives a restart: the same finish is still a 422, the metadata
// still reports incomplete and the stored chunks are still readable.
func TestAttachmentDigestMismatchPersistsAcrossRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	// The declared digest belongs to other content than what will arrive.
	w, body := createAttachment(t, h, "dev-1", "att-1", 4, 2, sha256Hex([]byte("xyz!")))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create = %d %v", w.Code, body)
	}
	putChunk(t, h, "dev-1", "att-1", 0, []byte("da"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("ta"))

	assertMismatch := func(t *testing.T, h http.Handler) {
		t.Helper()
		w, _ := completeAttachment(t, h, "dev-1", "att-1")
		if w.Code != http.StatusUnprocessableEntity {
			t.Fatalf("digest mismatch = %d, want 422", w.Code)
		}
		assertJSONError(t, w)
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
		w = serveRecorder(h, r)
		if !strings.Contains(w.Body.String(), `"complete":false`) ||
			!strings.Contains(w.Body.String(), `"receivedChunks":[0,1]`) {
			t.Fatalf("meta after 422 = %s", w.Body.String())
		}
		r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/1", nil)
		w = serveRecorder(h, r)
		if w.Code != http.StatusOK || w.Body.String() != "ta" {
			t.Fatalf("chunk after 422 = %d %q", w.Code, w.Body.String())
		}
	}
	assertMismatch(t, h)
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	assertMismatch(t, h)
}

// The same digest at a different size must never be silently reused: an
// upload that declares the digest of an already-finished attachment but a
// different size can never assemble matching bytes, so its finish is rejected
// (the assembled content does not hash to the declared digest: 422) and the
// upload stays unsealed instead of being completed against foreign bytes.
func TestAttachmentSameDigestDifferentSizeNeverReused(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["reused"] != false {
		t.Fatalf("first complete = %d %v", w.Code, body)
	}

	// A second upload declares the same digest but a different size. Its
	// finish must not be answered with the stored content: the assembled
	// bytes cannot match the declared digest, so the finish is a 422 and the
	// upload stays resumable rather than being sealed against foreign bytes.
	w, body = createAttachment(t, h, "dev-1", "att-2", 6, 6, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create different-size = %d %v", w.Code, body)
	}
	putChunk(t, h, "dev-1", "att-2", 0, []byte("hello "))
	w, _ = completeAttachment(t, h, "dev-1", "att-2")
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("same digest different size = %d, want 422 (never a silent reuse)", w.Code)
	}
	assertJSONError(t, w)
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-2", nil)
	w = serveRecorder(h, r)
	if !strings.Contains(w.Body.String(), `"complete":false`) {
		t.Fatalf("different-size upload sealed: %s", w.Body.String())
	}
}

// After a restart the create/chunk idempotency and the 403/404 access
// judgments are unchanged.
func TestAttachmentAccessJudgmentsSurviveRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	// The create retry is still idempotent; a metadata-different create by the
	// same device and any create by another device are still 409.
	w, body := createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("create retry after restart = %d %v", w.Code, body)
	}
	w, _ = createAttachment(t, h, "dev-1", "att-1", 12, 4, sha256Hex(content))
	if w.Code != http.StatusConflict {
		t.Fatalf("different metadata after restart = %d, want 409", w.Code)
	}
	w, _ = createAttachment(t, h, "dev-2", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusConflict {
		t.Fatalf("cross-device create after restart = %d, want 409", w.Code)
	}
	// Non-creator access is still 403, unknown ids still 404.
	w, _ = putChunk(t, h, "dev-2", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator chunk after restart = %d, want 403", w.Code)
	}
	w, _ = completeAttachment(t, h, "dev-1", "nope")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown complete after restart = %d, want 404", w.Code)
	}
	// The interrupted upload resumes and seals.
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete after restart = %d %v", w.Code, body)
	}
	// Chunks read back in order reassemble the original content.
	var got strings.Builder
	for i := int64(0); i < 3; i++ {
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/devices/dev-1/attachments/att-1/chunks/%d", i), nil)
		w = serveRecorder(h, r)
		if w.Code != http.StatusOK {
			t.Fatalf("chunk %d after restart = %d", i, w.Code)
		}
		got.WriteString(w.Body.String())
	}
	if got.String() != string(content) {
		t.Fatalf("reassembled content = %q, want %q", got.String(), content)
	}
}
