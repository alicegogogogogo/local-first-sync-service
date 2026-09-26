package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func deleteAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/v1/devices/%s/attachments/%s", deviceID, attachmentID))
}

// The full delete contract over the public HTTP surface: the creator removes
// a sealed attachment, after which every read and write on it is a 404 for
// everyone (granted readers included), the repeat delete is a state-free 404,
// and the id starts a brand-new upload.
func TestDeleteAttachmentEndToEndContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world") // 11 bytes, chunks of 4
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	for i, c := range []string{"hell", "o wo", "rld"} {
		putChunk(t, h, "dev-1", "att-1", int64(i), []byte(c))
	}
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// The success response is one JSON line with the id and the delete marker.
	w, body = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d body=%s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("delete content type = %q, want application/json", ct)
	}
	if body["attachmentId"] != "att-1" || body["deleted"] != true || len(body) != 2 {
		t.Fatalf("delete body = %v, want exactly attachmentId and deleted", body)
	}
	if strings.Count(w.Body.String(), "\n") != 1 || !strings.HasSuffix(w.Body.String(), "\n") {
		t.Fatalf("delete response is not one JSON line: %q", w.Body.String())
	}

	// Metadata and chunks are 404 for the creator and the granted device alike.
	for _, dev := range []string{"dev-1", "dev-2"} {
		if w := getAttachmentMeta(t, h, dev, "att-1"); w.Code != http.StatusNotFound {
			t.Fatalf("meta after delete as %s = %d, want 404", dev, w.Code)
		}
		if w := getChunk(t, h, dev, "att-1", 0); w.Code != http.StatusNotFound {
			t.Fatalf("chunk after delete as %s = %d, want 404", dev, w.Code)
		}
	}

	// Further chunk uploads, finishes and access changes are 404 and write
	// nothing.
	if w, _ := putChunk(t, h, "dev-1", "att-1", 0, []byte("hell")); w.Code != http.StatusNotFound {
		t.Fatalf("chunk after delete = %d, want 404", w.Code)
	}
	if w, _ := completeAttachment(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("complete after delete = %d, want 404", w.Code)
	}
	if w, _ := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant"); w.Code != http.StatusNotFound {
		t.Fatalf("grant after delete = %d, want 404", w.Code)
	}

	// A repeat delete is a 404 JSON error that changes nothing.
	w, body = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("repeat delete = %d %v, want 404 JSON error", w.Code, body)
	}
	assertJSONError(t, w)

	// The same id starts a brand-new upload: created=true and no carried-over
	// chunks or seal.
	w, body = createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("recreate = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w = getAttachmentMeta(t, h, "dev-1", "att-1")
	if !strings.Contains(w.Body.String(), `"complete":false`) ||
		!strings.Contains(w.Body.String(), `"receivedChunks":[]`) {
		t.Fatalf("recreated attachment carries old state: %s", w.Body.String())
	}
	if w, body := putChunk(t, h, "dev-1", "att-1", 0, []byte("hell")); w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk after recreate = %d %v", w.Code, body)
	}
}

// Only the creator may delete: a granted reader and a stranger get a 403 and
// change nothing; an unknown attachment (including under an unregistered
// caller device) gets a 404.
func TestDeleteAttachmentAccessContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// A granted reader still cannot delete: 403, and the grant keeps working.
	w, body := deleteAttachment(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("granted-reader delete = %d %v, want 403 JSON error", w.Code, body)
	}
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("grant broken by forbidden delete: %d", w.Code)
	}

	// Unknown attachment under a registered device: 404.
	w, body = deleteAttachment(t, h, "dev-1", "nope")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown delete = %d %v, want 404 JSON error", w.Code, body)
	}
	// An unregistered caller on an existing attachment is a non-creator: 403.
	w, _ = deleteAttachment(t, h, "ghost", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("unregistered-caller delete = %d, want 403", w.Code)
	}
	// An unregistered caller on an unknown attachment: 404.
	w, _ = deleteAttachment(t, h, "ghost", "nope")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered-caller unknown delete = %d, want 404", w.Code)
	}

	// Zero writes: the attachment is still intact with its one chunk.
	w = getAttachmentMeta(t, h, "dev-1", "att-1")
	if !strings.Contains(w.Body.String(), `"receivedChunks":[0]`) {
		t.Fatalf("attachment changed by rejected deletes: %s", w.Body.String())
	}
}

// An unfinished upload is removable: its partial chunks and progress
// disappear and a fresh upload under the same id starts at zero.
func TestDeleteIncompleteAttachmentContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	// The partial upload cannot finish yet.
	if w, _ := completeAttachment(t, h, "dev-1", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("incomplete finish = %d, want 409", w.Code)
	}

	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete incomplete = %d %v", w.Code, body)
	}
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("meta after incomplete delete = %d, want 404", w.Code)
	}

	// Brand-new upload: the previously received chunks are gone.
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	w = getAttachmentMeta(t, h, "dev-1", "att-1")
	if !strings.Contains(w.Body.String(), `"receivedChunks":[]`) {
		t.Fatalf("recreated upload carries partial chunks: %s", w.Body.String())
	}
	if w, b := putChunk(t, h, "dev-1", "att-1", 2, []byte("rld")); w.Code != http.StatusOK || b["created"] != true {
		t.Fatalf("old chunk range occupied: %d %v", w.Code, b)
	}
}

// Digest dedup is unaffected by a delete while another completed attachment
// still references the bytes; deleting the last reference reclaims them, so a
// later identical upload stores fresh bytes (reused=false).
func TestDeleteAttachmentDedupAndGCContract(t *testing.T) {
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
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 8)
	putChunk(t, h, "dev-2", "att-2", 0, content[:8])
	putChunk(t, h, "dev-2", "att-2", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("second complete = %d %v, want reused=true", w.Code, body)
	}

	// Delete one reference: the other attachment still reads its bytes.
	w, body = deleteAttachment(t, h, "dev-2", "att-2")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete att-2 = %d %v", w.Code, body)
	}
	if w := getChunk(t, h, "dev-1", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != string(content[:8]) {
		t.Fatalf("remaining attachment bytes after one delete = %d %q", w.Code, w.Body.String())
	}
	// A third identical upload still reuses the surviving bytes.
	mustCreateAttachment(t, h, "dev-2", "att-3", content, 8)
	putChunk(t, h, "dev-2", "att-3", 0, content[:8])
	putChunk(t, h, "dev-2", "att-3", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-2", "att-3")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("complete after one delete = %d %v, want reused=true", w.Code, body)
	}

	// Remove both remaining references; the bytes are now reclaimed.
	deleteAttachment(t, h, "dev-2", "att-3")
	deleteAttachment(t, h, "dev-1", "att-1")
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("att-1 still readable after last-reference delete: %d", w.Code)
	}

	// A brand-new identical attachment stores its own bytes: reused=false.
	mustCreateAttachment(t, h, "dev-1", "att-4", content, 8)
	putChunk(t, h, "dev-1", "att-4", 0, content[:8])
	putChunk(t, h, "dev-1", "att-4", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-1", "att-4")
	if w.Code != http.StatusOK || body["reused"] != false {
		t.Fatalf("complete after GC = %d %v, want reused=false", w.Code, body)
	}
}

// Grants vanish with the attachment and are not resurrected by a same-id
// recreate: the previously granted device has no access to the new upload.
func TestDeleteAttachmentRemovesGrantsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, content)
	completeAttachment(t, h, "dev-1", "att-1")
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("granted read before delete = %d", w.Code)
	}

	deleteAttachment(t, h, "dev-1", "att-1")

	// Recreating under the same id must not carry the old grant forward.
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("old grant survived delete/recreate: %d, want 403", w.Code)
	}
	if w, _ := putChunk(t, h, "dev-2", "att-1", 0, content); w.Code != http.StatusForbidden {
		t.Fatalf("granted device can write to recreated attachment: %d", w.Code)
	}
}

// Method and path shape matrix for the delete endpoint: a wrong verb on the
// member path and a missing/extra segment on a DELETE are JSON 400s, never a
// redirect or HTML.
func TestDeleteAttachmentMethodAndPathShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// A non-DELETE verb on the exact member path is a method mismatch: 400 JSON.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
		r := httptest.NewRequest(method, "/v1/devices/dev-1/attachments/att-1", strings.NewReader("{}"))
		r.Header.Set("Content-Type", "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s member = %d, want 400", method, w.Code)
		}
		assertJSONError(t, w)
	}

	// Malformed DELETE shapes: collection path (missing id), sub-resources
	// (extra segments), trailing slash, empty segments — all 400 JSON.
	for _, path := range []string{
		"/v1/devices/dev-1/attachments",
		"/v1/devices/dev-1/attachments/att-1/chunks/0",
		"/v1/devices/dev-1/attachments/att-1/chunks",
		"/v1/devices/dev-1/attachments/att-1/complete",
		"/v1/devices/dev-1/attachments/att-1/access",
		"/v1/devices/dev-1/attachments/",
		"/v1/devices//attachments/att-1",
		"/v1/devices/dev-1/attachments//",
	} {
		r := httptest.NewRequest(http.MethodDelete, path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("DELETE %s = %d, want 400", path, w.Code)
		}
		assertJSONError(t, w)
	}

	// The rejections deleted nothing: the real delete still succeeds once.
	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete after rejected shapes = %d %v", w.Code, body)
	}
}

// A device literally named "attachments" keeps its ordinary routes, and a
// DELETE carrying a body is still accepted (the body is simply ignored).
func TestDeleteAttachmentPathKeywordEdgeCases(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "attachments")
	content := []byte("data")
	mustCreateAttachment(t, h, "attachments", "att-1", content, 4)

	r := httptest.NewRequest(http.MethodDelete, "/v1/devices/attachments/attachments/att-1", strings.NewReader(`{"ignored":true}`))
	r.Header.Set("Content-Type", "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("delete with device named attachments = %d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["deleted"] != true {
		t.Fatalf("delete body = %s", w.Body.String())
	}
}

// The deletion survives a restart: after reopen the attachment, its chunks
// and the grants are still gone, the repeat delete still misses and the id is
// free for a new upload.
func TestDeleteAttachmentSurvivesRestart(t *testing.T) {
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
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	completeAttachment(t, h, "dev-1", "att-1")
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	deleteAttachment(t, h, "dev-1", "att-1")
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	for _, dev := range []string{"dev-1", "dev-2"} {
		if w := getAttachmentMeta(t, h, dev, "att-1"); w.Code != http.StatusNotFound {
			t.Fatalf("meta after restart as %s = %d, want 404", dev, w.Code)
		}
		if w := getChunk(t, h, dev, "att-1", 0); w.Code != http.StatusNotFound {
			t.Fatalf("chunk after restart as %s = %d, want 404", dev, w.Code)
		}
	}
	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete after restart = %d, want 404", w.Code)
	}
	w, body = createAttachment(t, h, "dev-1", "att-1", 11, 4, sha256Hex(content))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("recreate after restart = %d %v", w.Code, body)
	}
}

// Concurrent deletes of the same attachment take effect at most once: exactly
// one answers 200, every other answers 404.
func TestDeleteAttachmentConcurrentContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("data")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := deleteAttachment(t, h, "dev-1", "att-1")
			switch w.Code {
			case http.StatusOK:
				if body["deleted"] != true {
					errs <- fmt.Errorf("success body = %v", body)
					return
				}
				mu.Lock()
				succeeded++
				mu.Unlock()
			case http.StatusNotFound:
			default:
				errs <- fmt.Errorf("status %d: %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if succeeded != 1 {
		t.Fatalf("successful deletes = %d, want exactly 1", succeeded)
	}
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("attachment after concurrent delete = %d, want 404", w.Code)
	}
}
