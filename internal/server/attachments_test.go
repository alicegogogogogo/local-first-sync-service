package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func httpSHAHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// createAttachmentHTTP posts a valid create request and fails the test on any
// non-2xx outcome.
func createAttachmentHTTP(t *testing.T, h http.Handler, deviceID, attachmentID string, size, chunkSize int64, digest string) {
	t.Helper()
	w, body := postJSON(t, h, "/v1/devices/"+deviceID+"/attachments", map[string]any{
		"attachmentId": attachmentID,
		"size":         size,
		"chunkSize":    chunkSize,
		"sha256":       digest,
	})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create attachment %s/%s = %d %s", deviceID, attachmentID, w.Code, w.Body.String())
	}
}

func putChunkHTTP(t *testing.T, h http.Handler, deviceID, attachmentID string, index int64, contentType string, data []byte) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut,
		"/v1/devices/"+deviceID+"/attachments/"+attachmentID+"/chunks/"+itoa(index),
		bytes.NewReader(data))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// uploadAllChunksHTTP posts every chunk of content with the given layout in
// order.
func uploadAllChunksHTTP(t *testing.T, h http.Handler, deviceID, attachmentID string, content []byte, chunkSize int64) {
	t.Helper()
	for off, idx := int64(0), int64(0); off < int64(len(content)); off, idx = off+chunkSize, idx+1 {
		end := off + chunkSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		w := putChunkHTTP(t, h, deviceID, attachmentID, idx, "application/octet-stream", content[off:end])
		if w.Code != http.StatusOK {
			t.Fatalf("chunk %d = %d %s", idx, w.Code, w.Body.String())
		}
	}
}

func TestCreateAttachmentHTTPValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	const url = "/v1/devices/dev-1/attachments"
	digest := httpSHAHex([]byte("hello"))

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"missing content type", "", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"json suffix type", "application/vnd.api+json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"` + digest + `"}x`},
		{"missing attachmentId", "application/json", `{"size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"empty attachmentId", "application/json", `{"attachmentId":"","size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"numeric attachmentId", "application/json", `{"attachmentId":9,"size":5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"missing size", "application/json", `{"attachmentId":"a","chunkSize":4,"sha256":"` + digest + `"}`},
		{"zero size", "application/json", `{"attachmentId":"a","size":0,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"negative size", "application/json", `{"attachmentId":"a","size":-1,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"float size", "application/json", `{"attachmentId":"a","size":5.5,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"string size", "application/json", `{"attachmentId":"a","size":"5","chunkSize":4,"sha256":"` + digest + `"}`},
		{"boolean size", "application/json", `{"attachmentId":"a","size":true,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"null size", "application/json", `{"attachmentId":"a","size":null,"chunkSize":4,"sha256":"` + digest + `"}`},
		{"missing chunkSize", "application/json", `{"attachmentId":"a","size":5,"sha256":"` + digest + `"}`},
		{"zero chunkSize", "application/json", `{"attachmentId":"a","size":5,"chunkSize":0,"sha256":"` + digest + `"}`},
		{"negative chunkSize", "application/json", `{"attachmentId":"a","size":5,"chunkSize":-4,"sha256":"` + digest + `"}`},
		{"float chunkSize", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4.5,"sha256":"` + digest + `"}`},
		{"missing sha256", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4}`},
		{"uppercase hex", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"` + bytesUpper(digest) + `"}`},
		{"short hex", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"abc"}`},
		{"non-hex", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":"zz3ef39972498569ae6417da2b2ca490daa0ab1e64036900c22f26317d5be3f6"}`},
		{"numeric sha256", "application/json", `{"attachmentId":"a","size":5,"chunkSize":4,"sha256":123}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, url, tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Unregistered device -> 404.
	w, body := postJSON(t, h, "/v1/devices/ghost/attachments", map[string]any{
		"attachmentId": "a", "size": 5, "chunkSize": 4, "sha256": digest,
	})
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown device = %d %v", w.Code, body)
	}
}

func bytesUpper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'f' {
			b[i] = c - ('a' - 'A')
		}
	}
	return string(b)
}

func TestCreateAttachmentHTTPIdempotencyAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	digest := httpSHAHex([]byte("hello"))

	createAttachmentHTTP(t, h, "dev-1", "att", 5, 4, digest)

	// Identical retry by the owner -> 200 created=false.
	w, body := postJSON(t, h, "/v1/devices/dev-1/attachments", map[string]any{
		"attachmentId": "att", "size": 5, "chunkSize": 4, "sha256": digest,
	})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("retry = %d %s", w.Code, w.Body.String())
	}

	// Other device, even with identical metadata -> 409.
	w, body = postJSON(t, h, "/v1/devices/dev-2/attachments", map[string]any{
		"attachmentId": "att", "size": 5, "chunkSize": 4, "sha256": digest,
	})
	if w.Code != http.StatusConflict || body["error"] == nil {
		t.Fatalf("cross-device = %d %s", w.Code, w.Body.String())
	}

	// Owner retry still idempotent: the conflict did not move or mutate state.
	w, body = postJSON(t, h, "/v1/devices/dev-1/attachments", map[string]any{
		"attachmentId": "att", "size": 5, "chunkSize": 4, "sha256": digest,
	})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("owner retry after conflict = %d %s", w.Code, w.Body.String())
	}
}

func TestPutChunkHTTPStatusCodes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("0123456789")
	digest := httpSHAHex(content)
	createAttachmentHTTP(t, h, "dev-1", "att", 10, 4, digest)

	// Wrong content type -> 400.
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "text/plain", content[0:4]); w.Code != http.StatusBadRequest {
		t.Fatalf("wrong content type = %d", w.Code)
	}
	// Missing content type -> 400.
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "", content[0:4]); w.Code != http.StatusBadRequest {
		t.Fatalf("missing content type = %d", w.Code)
	}
	// Non-numeric index -> 400.
	r := httptest.NewRequest(http.MethodPut, "/v1/devices/dev-1/attachments/att/chunks/abc", bytes.NewReader(content[0:4]))
	r.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad index = %d", w.Code)
	}

	// Unknown attachment -> 404.
	if w := putChunkHTTP(t, h, "dev-1", "ghost", 0, "application/octet-stream", content[0:4]); w.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment = %d", w.Code)
	}
	// Other device -> 403.
	if w := putChunkHTTP(t, h, "dev-2", "att", 0, "application/octet-stream", content[0:4]); w.Code != http.StatusForbidden {
		t.Fatalf("other device = %d", w.Code)
	}
	// Out-of-range index -> 400.
	if w := putChunkHTTP(t, h, "dev-1", "att", 3, "application/octet-stream", []byte("x")); w.Code != http.StatusBadRequest {
		t.Fatalf("out of range = %d", w.Code)
	}
	// Non-final chunk wrong length -> 400.
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "application/octet-stream", content[0:3]); w.Code != http.StatusBadRequest {
		t.Fatalf("wrong non-final length = %d", w.Code)
	}
	// Final chunk exceeding the declared remainder -> 400.
	if w := putChunkHTTP(t, h, "dev-1", "att", 2, "application/octet-stream", []byte("890")); w.Code != http.StatusBadRequest {
		t.Fatalf("final chunk too long = %d", w.Code)
	}

	// Valid chunks, out of order.
	if w := putChunkHTTP(t, h, "dev-1", "att", 2, "application/octet-stream", content[8:10]); w.Code != http.StatusOK {
		t.Fatalf("chunk 2 = %d %s", w.Code, w.Body.String())
	}
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "application/octet-stream", content[0:4]); w.Code != http.StatusOK {
		t.Fatalf("chunk 0 = %d %s", w.Code, w.Body.String())
	}
	// Identical re-post -> 200 idempotent.
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "application/octet-stream", content[0:4]); w.Code != http.StatusOK {
		t.Fatalf("identical repost = %d", w.Code)
	}
	// Different bytes -> 409, first content retained.
	if w := putChunkHTTP(t, h, "dev-1", "att", 0, "application/octet-stream", []byte("ZZZZ")); w.Code != http.StatusConflict {
		t.Fatalf("different bytes = %d, want 409", w.Code)
	}

	// GET the chunk back: first bytes survive.
	gw, _ := doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att/chunks/0")
	if gw.Code != http.StatusOK || gw.Body.String() != "0123" {
		t.Fatalf("get chunk = %d %q", gw.Code, gw.Body.String())
	}
}

func TestCompleteAndDedupHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	content := []byte("the-quick-brown-fox") // 19 bytes
	digest := httpSHAHex(content)

	// Create two attachments on dev-1 and one on dev-2, all with identical
	// content. The second/third completions must reuse, not copy.
	createAttachmentHTTP(t, h, "dev-1", "a1", 19, 7, digest)
	uploadAllChunksHTTP(t, h, "dev-1", "a1", content, 7)
	w, body := doRequest(t, h, http.MethodPost, "/v1/devices/dev-1/attachments/a1/complete")
	if w.Code != http.StatusOK || body["completed"] != true || body["reused"] != false {
		t.Fatalf("a1 complete = %d %s", w.Code, w.Body.String())
	}

	createAttachmentHTTP(t, h, "dev-1", "a2", 19, 5, digest)
	uploadAllChunksHTTP(t, h, "dev-1", "a2", content, 5)
	w, body = doRequest(t, h, http.MethodPost, "/v1/devices/dev-1/attachments/a2/complete")
	if w.Code != http.StatusOK || body["reused"] != true || body["reusedFrom"] != "a1" {
		t.Fatalf("a2 complete = %d %s", w.Code, w.Body.String())
	}

	// Repeating completion returns the same result.
	w, body2 := doRequest(t, h, http.MethodPost, "/v1/devices/dev-1/attachments/a1/complete")
	if w.Code != http.StatusOK || body2["completed"] != true || body2["reused"] != false {
		t.Fatalf("a1 repeat = %d %s", w.Code, w.Body.String())
	}

	// Non-creator cannot complete.
	createAttachmentHTTP(t, h, "dev-2", "a3", 19, 6, digest)
	uploadAllChunksHTTP(t, h, "dev-2", "a3", content, 6)
	w, _ = doRequest(t, h, http.MethodPost, "/v1/devices/dev-1/attachments/a3/complete")
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-device complete = %d, want 403", w.Code)
	}
	// The creator can still complete, reusing dev-1's content.
	w, body = doRequest(t, h, http.MethodPost, "/v1/devices/dev-2/attachments/a3/complete")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("a3 creator complete = %d %s", w.Code, w.Body.String())
	}
}

func TestCompleteHTTPIncompleteAndDigestFailure(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	content := []byte("0123456789")

	// Missing chunks -> 409, upload stays resumable.
	digest := httpSHAHex(content)
	createAttachmentHTTP(t, h, "dev", "inc", 10, 4, digest)
	if w := putChunkHTTP(t, h, "dev", "inc", 0, "application/octet-stream", content[0:4]); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	cw := doRequest2(t, h, http.MethodPost, "/v1/devices/dev/attachments/inc/complete")
	if cw.Code != http.StatusConflict {
		t.Fatalf("incomplete complete = %d, want 409", cw.Code)
	}
	// Still resumable: post the remaining indices explicitly and complete.
	for off, idx := int64(4), int64(1); off < 10; off, idx = off+4, idx+1 {
		end := off + 4
		if end > 10 {
			end = 10
		}
		if pw := putChunkHTTP(t, h, "dev", "inc", idx, "application/octet-stream", content[off:end]); pw.Code != http.StatusOK {
			t.Fatalf("resume chunk %d = %d %s", idx, pw.Code, pw.Body.String())
		}
	}
	cw = doRequest2(t, h, http.MethodPost, "/v1/devices/dev/attachments/inc/complete")
	if cw.Code != http.StatusOK {
		t.Fatalf("resumed complete = %d %s", cw.Code, cw.Body.String())
	}

	// Wrong digest but correct total length -> 422, stays unsealed.
	wrong := httpSHAHex([]byte("different-bytes!"))
	createAttachmentHTTP(t, h, "dev", "bad", 10, 4, wrong)
	uploadAllChunksHTTP(t, h, "dev", "bad", content, 4)
	cw = doRequest2(t, h, http.MethodPost, "/v1/devices/dev/attachments/bad/complete")
	if cw.Code != http.StatusUnprocessableEntity {
		t.Fatalf("digest mismatch = %d, want 422", cw.Code)
	}
	gw, body := doRequest(t, h, http.MethodGet, "/v1/devices/dev/attachments/bad")
	if gw.Code != http.StatusOK || body["completed"] != false {
		t.Fatalf("metadata after 422 = %d %v", gw.Code, body)
	}
}

// doRequest2 issues a request with a nil body (POST complete carries none).
func doRequest2(t *testing.T, h http.Handler, method, url string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, url, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestGetAttachmentHTTPMetadataAndChunk(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("0123456789")
	digest := httpSHAHex(content)
	createAttachmentHTTP(t, h, "dev-1", "att", 10, 4, digest)
	putChunkHTTP(t, h, "dev-1", "att", 1, "application/octet-stream", content[4:8])

	// Metadata: received indices, completion false, original metadata.
	w, body := doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att")
	if w.Code != http.StatusOK {
		t.Fatalf("meta = %d %s", w.Code, w.Body.String())
	}
	if body["attachmentId"] != "att" || body["size"].(float64) != 10 || body["chunkSize"].(float64) != 4 ||
		body["sha256"] != digest || body["completed"] != false {
		t.Fatalf("meta body = %v", body)
	}
	recv := body["received"].([]any)
	if len(recv) != 1 || recv[0].(float64) != 1 {
		t.Fatalf("received = %v", recv)
	}

	// Non-creator metadata -> 403; unknown -> 404.
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att"); w.Code != http.StatusForbidden {
		t.Fatalf("other-device meta = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/ghost"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown meta = %d, want 404", w.Code)
	}

	// Chunk reads.
	gw, _ := doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att/chunks/1")
	if gw.Code != http.StatusOK || gw.Body.String() != "4567" {
		t.Fatalf("get chunk = %d %q", gw.Code, gw.Body.String())
	}
	gw, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att/chunks/0")
	if gw.Code != http.StatusNotFound {
		t.Fatalf("missing chunk = %d, want 404", gw.Code)
	}
	gw, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att/chunks/1")
	if gw.Code != http.StatusForbidden {
		t.Fatalf("other-device chunk = %d, want 403", gw.Code)
	}
	gw, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/ghost/chunks/0")
	if gw.Code != http.StatusNotFound {
		t.Fatalf("unknown attachment chunk = %d, want 404", gw.Code)
	}
	gw, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att/chunks/9")
	if gw.Code != http.StatusBadRequest {
		t.Fatalf("illegal index = %d, want 400", gw.Code)
	}
}

func TestAttachmentHTTPEmptyIDs(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	for _, p := range []string{
		"/v1/devices//attachments",
		"/v1/devices/dev/attachments/",
		"/v1/devices/dev/attachments//chunks/0",
	} {
		r := httptest.NewRequest(http.MethodPost, p, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", p, w.Code)
		}
		assertJSONError(t, w)
	}
}

func TestAttachmentHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev")
	content := []byte("restart-me-now!") // 15 bytes
	digest := httpSHAHex(content)
	createAttachmentHTTP(t, h, "dev", "att", 15, 6, digest)
	uploadAllChunksHTTP(t, h, "dev", "att", content, 6)
	if w := doRequest2(t, h, http.MethodPost, "/v1/devices/dev/attachments/att/complete"); w.Code != http.StatusOK {
		t.Fatalf("complete = %d %s", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Completion, metadata, chunk bytes and create idempotency all survive.
	w := doRequest2(t, h2, http.MethodPost, "/v1/devices/dev/attachments/att/complete")
	if w.Code != http.StatusOK {
		t.Fatalf("repeat complete after restart = %d %s", w.Code, w.Body.String())
	}
	gw, body := doRequest(t, h2, http.MethodGet, "/v1/devices/dev/attachments/att")
	if gw.Code != http.StatusOK || body["completed"] != true || len(body["received"].([]any)) != 3 {
		t.Fatalf("meta after restart = %d %v", gw.Code, body)
	}
	cgw, _ := doRequest(t, h2, http.MethodGet, "/v1/devices/dev/attachments/att/chunks/2")
	if cgw.Code != http.StatusOK || cgw.Body.String() != "ow!" {
		t.Fatalf("chunk after restart = %d %q", cgw.Code, cgw.Body.String())
	}
	pw, pbody := postJSON(t, h2, "/v1/devices/dev/attachments", map[string]any{
		"attachmentId": "att", "size": 15, "chunkSize": 6, "sha256": digest,
	})
	if pw.Code != http.StatusOK || pbody["created"] != false {
		t.Fatalf("create replay after restart = %d %s", pw.Code, pw.Body.String())
	}

	// A second identical attachment after restart still dedups.
	createAttachmentHTTP(t, h2, "dev", "att2", 15, 6, digest)
	uploadAllChunksHTTP(t, h2, "dev", "att2", content, 6)
	rw, rbody := doRequest(t, h2, http.MethodPost, "/v1/devices/dev/attachments/att2/complete")
	if rw.Code != http.StatusOK || rbody["reused"] != true || rbody["reusedFrom"] != "att" {
		t.Fatalf("dedup after restart = %d %s", rw.Code, rw.Body.String())
	}
}
