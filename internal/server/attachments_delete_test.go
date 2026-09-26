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

// deleteAttachment issues a DELETE for the attachment and returns the
// recorder and decoded body.
func deleteAttachment(t *testing.T, h http.Handler, deviceID, attachmentID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodDelete,
		fmt.Sprintf("/v1/devices/%s/attachments/%s", deviceID, attachmentID), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var body map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &body)
	}
	return w, body
}

// The delete lifecycle: a successful delete answers the id and the deletion
// marker, after which every read, write, finish and access change against the
// id is a 404 — for the creator and for a previously granted device alike —
// and a repeat delete is a 404 that changes nothing.
func TestDeleteAttachmentLifecycleContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}
	w, body = setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["authorized"] != true {
		t.Fatalf("grant = %d %v", w.Code, body)
	}
	// The granted device reads before the delete.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("granted meta before delete = %d, want 200", w.Code)
	}

	w, body = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["attachmentId"] != "att-1" || body["deleted"] != true {
		t.Fatalf("delete = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Every later operation on the id is a 404 JSON error: metadata and chunk
	// reads for the creator and the formerly granted device, chunk writes,
	// the finish and access changes.
	for _, device := range []string{"dev-1", "dev-2"} {
		if w := getAttachmentMeta(t, h, device, "att-1"); w.Code != http.StatusNotFound {
			t.Fatalf("meta after delete (%s) = %d, want 404", device, w.Code)
		}
		if w := getChunk(t, h, device, "att-1", 0); w.Code != http.StatusNotFound {
			t.Fatalf("chunk after delete (%s) = %d, want 404", device, w.Code)
		}
	}
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("chunk write after delete = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	w, _ = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("complete after delete = %d, want 404", w.Code)
	}
	w, _ = setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusNotFound {
		t.Fatalf("access after delete = %d, want 404", w.Code)
	}

	// A repeat delete is a 404 JSON error and changes nothing.
	w, _ = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
}

// Only the creator may delete: another device's delete is a 403 JSON error
// that writes nothing — the attachment, its chunks and its grants are
// untouched. An unknown id is a 404.
func TestDeleteAttachmentAccessContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	// Unknown attachment: 404 JSON.
	w, _ := deleteAttachment(t, h, "dev-1", "nope")
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Non-creator: 403 JSON, zero writes.
	w, _ = deleteAttachment(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("delete non-creator = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "hello") || strings.Contains(w.Body.String(), sha256Hex(content)) {
		t.Fatalf("403 body leaks content: %s", w.Body.String())
	}

	// Nothing changed: the creator still reads the metadata and the chunk.
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("meta after forbidden delete = %d, want 200", w.Code)
	}
	if w := getChunk(t, h, "dev-1", "att-1", 0); w.Code != http.StatusOK || w.Body.String() != "hell" {
		t.Fatalf("chunk after forbidden delete = %d %q", w.Code, w.Body.String())
	}
}

// Request-shape matrix for the delete entry: empty identifiers, missing or
// extra path segments and mismatched methods are all 400 JSON errors — never
// a redirect or HTML — and write nothing.
func TestDeleteAttachmentShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifiers and trailing slashes.
		{http.MethodDelete, "/v1/devices//attachments/att-1"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments/"},
		{http.MethodDelete, "/v1/devices/dev-1//attachments/att-1"},
		// Missing the attachment id segment.
		{http.MethodDelete, "/v1/devices/dev-1/attachments"},
		// Extra segments past the delete path.
		{http.MethodDelete, "/v1/devices/dev-1/attachments/att-1/extra"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments/att-1/chunks/0"},
		// Methods other than GET/DELETE on the item path.
		{http.MethodPost, "/v1/devices/dev-1/attachments/att-1"},
		{http.MethodPut, "/v1/devices/dev-1/attachments/att-1"},
		{http.MethodPatch, "/v1/devices/dev-1/attachments/att-1"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s content-type = %q, want application/json", tc.method, tc.path, ct)
		}
	}

	// Zero writes: the attachment is still fully readable.
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("meta after rejected deletes = %d, want 200", w.Code)
	}
}

// An incomplete upload can be deleted: its received chunks and progress
// vanish, and re-creating the same id — even with different metadata — is a
// brand-new upload that accepts chunks again.
func TestDeleteIncompleteAttachmentThenRecreate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))

	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete incomplete = %d %v", w.Code, body)
	}
	// The progress is gone with the record.
	if w := getChunk(t, h, "dev-1", "att-1", 0); w.Code != http.StatusNotFound {
		t.Fatalf("chunk of deleted upload = %d, want 404", w.Code)
	}

	// Re-creating the same id is a fresh upload, even with different
	// metadata; the old chunks do not leak into it.
	replacement := []byte("goodbye")
	mustCreateAttachment(t, h, "dev-1", "att-1", replacement, 2)
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, []byte("go"))
	if w.Code != http.StatusOK {
		t.Fatalf("chunk on recreated upload = %d, want 200", w.Code)
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	if !strings.Contains(w.Body.String(), `"receivedChunks":[0]`) ||
		!strings.Contains(w.Body.String(), `"totalBytes":7`) {
		t.Fatalf("recreated meta = %s", w.Body.String())
	}
}

// Digest-addressed content survives the delete of any attachment that is not
// its last referrer: other finished attachments keep reading their bytes and
// new identical uploads still reuse the content. Only when the last completed
// reference is deleted are the bytes reclaimed, so the next identical upload
// stores them fresh (reused=false).
func TestDeleteAttachmentContentReclamation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("shared content")

	for _, tc := range []struct {
		device, id string
	}{
		{"dev-1", "att-1"},
		{"dev-2", "att-2"},
	} {
		mustCreateAttachment(t, h, tc.device, tc.id, content, 8)
		putChunk(t, h, tc.device, tc.id, 0, content[:8])
		putChunk(t, h, tc.device, tc.id, 1, content[8:])
		w, _ := completeAttachment(t, h, tc.device, tc.id)
		if w.Code != http.StatusOK {
			t.Fatalf("complete %s = %d", tc.id, w.Code)
		}
	}

	// Deleting the first referrer leaves the second fully readable, and a new
	// identical upload still reuses the stored bytes.
	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete first = %d %v", w.Code, body)
	}
	if w := getChunk(t, h, "dev-2", "att-2", 0); w.Code != http.StatusOK || w.Body.String() != string(content[:8]) {
		t.Fatalf("surviving chunk = %d %q", w.Code, w.Body.String())
	}
	mustCreateAttachment(t, h, "dev-1", "att-3", content, 8)
	putChunk(t, h, "dev-1", "att-3", 0, content[:8])
	putChunk(t, h, "dev-1", "att-3", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-1", "att-3")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("complete while referenced = %d %v, want reused=true", w.Code, body)
	}

	// Deleting the last two referrers reclaims the bytes: the next identical
	// upload stores them fresh.
	for _, tc := range []struct {
		device, id string
	}{
		{"dev-2", "att-2"},
		{"dev-1", "att-3"},
	} {
		w, _ = deleteAttachment(t, h, tc.device, tc.id)
		if w.Code != http.StatusOK {
			t.Fatalf("delete %s = %d", tc.id, w.Code)
		}
	}
	mustCreateAttachment(t, h, "dev-1", "att-4", content, 8)
	putChunk(t, h, "dev-1", "att-4", 0, content[:8])
	putChunk(t, h, "dev-1", "att-4", 1, content[8:])
	w, body = completeAttachment(t, h, "dev-1", "att-4")
	if w.Code != http.StatusOK || body["reused"] != false {
		t.Fatalf("complete after reclamation = %d %v, want reused=false", w.Code, body)
	}
}

// Concurrent deletes of the same attachment commit at most once: exactly one
// caller sees 200 and every other sees 404, and the attachment stays gone.
func TestDeleteAttachmentConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	const deleters = 8
	codes := make([]int, deleters)
	var wg sync.WaitGroup
	for i := 0; i < deleters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := deleteAttachment(t, h, "dev-1", "att-1")
			codes[i] = w.Code
		}(i)
	}
	wg.Wait()

	ok, notFound := 0, 0
	for _, c := range codes {
		switch c {
		case http.StatusOK:
			ok++
		case http.StatusNotFound:
			notFound++
		default:
			t.Fatalf("unexpected delete status %d", c)
		}
	}
	if ok != 1 || notFound != deleters-1 {
		t.Fatalf("concurrent deletes: ok=%d notFound=%d, want 1/%d", ok, notFound, deleters-1)
	}
	if w := getAttachmentMeta(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("meta after concurrent deletes = %d, want 404", w.Code)
	}
}

// The delete result is durable: after a restart the deleted attachment is
// still 404 for everyone, the shared content of a surviving duplicate is
// still readable, and re-creating the deleted id is still a fresh upload.
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
	content := []byte("shared content")
	for _, tc := range []struct {
		device, id string
	}{
		{"dev-1", "att-1"},
		{"dev-2", "att-2"},
	} {
		mustCreateAttachment(t, h, tc.device, tc.id, content, 8)
		putChunk(t, h, tc.device, tc.id, 0, content[:8])
		putChunk(t, h, tc.device, tc.id, 1, content[8:])
		completeAttachment(t, h, tc.device, tc.id)
	}
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete = %d %v", w.Code, body)
	}
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	// The deleted attachment stays deleted for everyone, including the
	// formerly granted device; the surviving duplicate still reads its bytes.
	for _, device := range []string{"dev-1", "dev-2"} {
		if w := getAttachmentMeta(t, h, device, "att-1"); w.Code != http.StatusNotFound {
			t.Fatalf("meta after restart (%s) = %d, want 404", device, w.Code)
		}
	}
	w, _ = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete after restart = %d, want 404", w.Code)
	}
	if w := getChunk(t, h, "dev-2", "att-2", 1); w.Code != http.StatusOK || w.Body.String() != string(content[8:]) {
		t.Fatalf("surviving chunk after restart = %d %q", w.Code, w.Body.String())
	}

	// Re-creating the deleted id is a brand-new upload.
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 8)
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	if !strings.Contains(w.Body.String(), `"complete":false`) ||
		!strings.Contains(w.Body.String(), `"receivedChunks":[]`) {
		t.Fatalf("recreated meta after restart = %s", w.Body.String())
	}
}
