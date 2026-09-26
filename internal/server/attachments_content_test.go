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

// getContent issues the whole-content read and returns the raw recorder.
func getContent(t *testing.T, h http.Handler, deviceID, attachmentID string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/v1/devices/%s/attachments/%s/content", deviceID, attachmentID), nil)
	return serveRecorder(h, r)
}

// sealUpload creates an attachment over content, pushes every chunk and seals
// it, failing the test on any error.
func sealUpload(t *testing.T, h http.Handler, deviceID, attachmentID string, content []byte, chunkSize int64) {
	t.Helper()
	mustCreateAttachment(t, h, deviceID, attachmentID, content, chunkSize)
	for i, off := int64(0), int64(0); off < int64(len(content)); i, off = i+1, off+chunkSize {
		end := off + chunkSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		w, body := putChunk(t, h, deviceID, attachmentID, i, content[off:end])
		if w.Code != http.StatusOK || body["created"] != true {
			t.Fatalf("chunk %d = %d %v body=%s", i, w.Code, body, w.Body.String())
		}
	}
	w, body := completeAttachment(t, h, deviceID, attachmentID)
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

// The whole-content read returns exactly the bytes the per-chunk reads would
// reassemble, as application/octet-stream, and repeated reads are stable.
func TestGetAttachmentContentMatchesChunkReassembly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]
	sealUpload(t, h, "dev-1", "att-1", content, 4)

	// Reassemble from the per-chunk reads as the reference.
	var want strings.Builder
	for i := int64(0); i < 3; i++ {
		r := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/v1/devices/dev-1/attachments/att-1/chunks/%d", i), nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusOK {
			t.Fatalf("chunk %d = %d", i, w.Code)
		}
		want.WriteString(w.Body.String())
	}

	for i := 0; i < 3; i++ {
		w := getContent(t, h, "dev-1", "att-1")
		if w.Code != http.StatusOK {
			t.Fatalf("content read %d = %d body=%s", i, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("content type = %q, want application/octet-stream", ct)
		}
		if w.Body.String() != want.String() || w.Body.String() != string(content) {
			t.Fatalf("content = %q, want %q", w.Body.String(), content)
		}
	}
}

// A granted device reads the same bytes as the creator; any other device gets
// a 403 JSON error without a single byte of content, and an unknown or
// deleted id is a 404 JSON error.
func TestGetAttachmentContentAccessJudgments(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")
	content := []byte("shared secret bytes")
	sealUpload(t, h, "dev-1", "att-1", content, 8)

	// Unknown attachment: 404 JSON, no content.
	w := getContent(t, h, "dev-1", "nope")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// A device with no grant: 403 JSON, not one byte leaks.
	w = getContent(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("ungranted = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "shared") {
		t.Fatalf("403 body leaks content: %s", w.Body.String())
	}

	// Grant dev-2: it now reads the identical bytes.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	w = getContent(t, h, "dev-2", "att-1")
	if w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("granted read = %d %q", w.Code, w.Body.String())
	}

	// dev-3 is still out; revoking dev-2 returns its read to 403 at once.
	w = getContent(t, h, "dev-3", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("dev-3 = %d, want 403", w.Code)
	}
	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	w = getContent(t, h, "dev-2", "att-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d, want 403", w.Code)
	}
	assertJSONError(t, w)

	// After a delete the id misses with a 404, for the creator as well.
	w, _ = deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d", w.Code)
	}
	w = getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
}

// An unfinished upload answers the content read with a 409 JSON error and
// stays resumable: chunks keep landing, the seal succeeds, and the sealed
// content then reads out in full.
func TestGetAttachmentContentUnsealedIs409AndResumable(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))

	w := getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusConflict {
		t.Fatalf("unsealed content read = %d, want 409 body=%s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// The 409 wrote nothing: the upload resumes and seals.
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w2, body := completeAttachment(t, h, "dev-1", "att-1")
	if w2.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete after 409 = %d %v", w2.Code, body)
	}
	w = getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("sealed content = %d %q", w.Code, w.Body.String())
	}
}

// Request-shape matrix for the content path: non-GET verbs, extra segments
// and empty identifiers are all 400 JSON errors that write nothing.
func TestGetAttachmentContentShapeRejections(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	sealUpload(t, h, "dev-1", "att-1", content, 4)

	// Every verb other than GET on the exact content path: 400 JSON.
	for _, method := range []string{
		http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions,
	} {
		r := httptest.NewRequest(method, "/v1/devices/dev-1/attachments/att-1/content", nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s content = %d, want 400", method, w.Code)
		}
		assertJSONError(t, w)
	}

	// Extra segments past the content path, missing segments and empty
	// identifiers: 400 JSON.
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/content/extra"},
		{http.MethodPost, "/v1/devices/dev-1/attachments/att-1/content/extra"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments/att-1/content/extra"},
		{http.MethodGet, "/v1/devices//attachments/att-1/content"},
		{http.MethodGet, "/v1/devices/dev-1/attachments//content"},
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/content/"},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
	}

	// None of the rejections touched anything: the content still reads out.
	w := getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("content after rejections = %d %q", w.Code, w.Body.String())
	}
}

// The content read is a pure read: it changes no metadata, allocates no
// change-log cursor and wakes no subscription.
func TestGetAttachmentContentIsReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("hello world")
	sealUpload(t, h, "dev-1", "att-1", content, 4)

	w := getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK {
		t.Fatalf("content = %d", w.Code)
	}

	// The attachment's metadata view is byte-for-byte what it was.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	w = serveRecorder(h, r)
	if !strings.Contains(w.Body.String(), `"complete":true`) ||
		!strings.Contains(w.Body.String(), `"receivedChunks":[0,1,2]`) {
		t.Fatalf("meta after content read = %s", w.Body.String())
	}

	// No change-log cursor was allocated anywhere: an unknown document still
	// reports cursor 0.
	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=0")
	if body["nextCursor"] != float64(0) {
		t.Fatalf("nextCursor after content read = %v, want 0", body["nextCursor"])
	}
}

// The content bytes and the sealed judgment survive a restart, and a delete
// committed before the restart still misses with a 404 after it.
func TestGetAttachmentContentSurvivesRestart(t *testing.T) {
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
	sealUpload(t, h, "dev-1", "att-1", content, 4)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	// A second upload stays unsealed across the restart.
	mustCreateAttachment(t, h, "dev-1", "att-2", content, 4)
	putChunk(t, h, "dev-1", "att-2", 0, []byte("hell"))
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	// The sealed content reads out identically, for the creator and the
	// granted device alike.
	for _, device := range []string{"dev-1", "dev-2"} {
		w := getContent(t, h, device, "att-1")
		if w.Code != http.StatusOK || w.Body.String() != string(content) {
			t.Fatalf("content after restart (%s) = %d %q", device, w.Code, w.Body.String())
		}
	}
	// The unsealed upload is still a 409 and still resumable.
	w := getContent(t, h, "dev-1", "att-2")
	if w.Code != http.StatusConflict {
		t.Fatalf("unsealed after restart = %d, want 409", w.Code)
	}
	putChunk(t, h, "dev-1", "att-2", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-2", 2, []byte("rld"))
	if w, body := completeAttachment(t, h, "dev-1", "att-2"); w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete after restart = %d %v", w.Code, body)
	}
	w = getContent(t, h, "dev-1", "att-2")
	if w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("newly sealed content = %d %q", w.Code, w.Body.String())
	}

	// A delete committed now is still a 404 after the next restart.
	w2, _ := deleteAttachment(t, h, "dev-1", "att-1")
	if w2.Code != http.StatusOK {
		t.Fatalf("delete = %d", w2.Code)
	}
	_ = s.Close()
	h, s = open(t)
	defer func() { _ = s.Close() }()
	w = getContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted across restart = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
}
