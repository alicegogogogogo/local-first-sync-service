package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// getAttachmentContent issues the whole-content read and returns the raw
// recorder so tests can assert status, content type and body bytes.
func getAttachmentContent(t *testing.T, h http.Handler, deviceID, attachmentID string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet,
		"/v1/devices/"+deviceID+"/attachments/"+attachmentID+"/content", nil)
	return serveRecorder(h, r)
}

// The whole-content read over the public HTTP surface: create, push chunks
// out of order, seal, then fetch the assembled bytes in one request. The body
// is byte-for-byte the per-chunk concatenation, the content type is
// application/octet-stream and repeated reads are stable.
func TestAttachmentContentEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// Before the seal the read is a 409 JSON error and the upload stays
	// resumable: chunks are still accepted afterwards.
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("content before seal = %d, want 409", w.Code)
	} else {
		assertJSONError(t, w)
	}
	w, body := putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("chunk after 409 = %d %v body=%s", w.Code, body, w.Body.String())
	}
	w, body = completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Sealed: the whole content comes back in one read, identical on repeats.
	assertContent := func(device string) {
		t.Helper()
		w := getAttachmentContent(t, h, device, "att-1")
		if w.Code != http.StatusOK {
			t.Fatalf("content read = %d body=%s", w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("content type = %q, want application/octet-stream", ct)
		}
		if w.Body.String() != string(content) {
			t.Fatalf("content = %q, want %q", w.Body.String(), content)
		}
		again := getAttachmentContent(t, h, device, "att-1")
		if again.Code != http.StatusOK || again.Body.String() != w.Body.String() {
			t.Fatalf("repeat read = %d %q, want identical bytes", again.Code, again.Body.String())
		}
	}
	assertContent("dev-1")

	// A granted device reads the same bytes; a stranger gets a 403 that leaks
	// nothing, and an unknown id a 404.
	w, _ = postJSON(t, h, "/v1/devices/dev-1/attachments/att-1/access", map[string]any{
		"deviceId": "dev-2", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d %s", w.Code, w.Body.String())
	}
	assertContent("dev-2")

	registerDevice(t, h, "dev-3")
	if w := getAttachmentContent(t, h, "dev-3", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("stranger content read = %d, want 403", w.Code)
	} else {
		assertJSONError(t, w)
		if strings.Contains(w.Body.String(), "hello") {
			t.Fatalf("403 body leaks content: %s", w.Body.String())
		}
	}
	if w := getAttachmentContent(t, h, "dev-1", "nope"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown content read = %d, want 404", w.Code)
	} else {
		assertJSONError(t, w)
	}

	// A revoke returns the granted device's read to 403 at once.
	w, _ = postJSON(t, h, "/v1/devices/dev-1/attachments/att-1/access", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	if w := getAttachmentContent(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("revoked content read = %d, want 403", w.Code)
	}
}

// A reused (digest-deduplicated) attachment still reads its own assembled
// bytes through the whole-content endpoint, identical to the first copy.
func TestAttachmentContentReadOnReusedUpload(t *testing.T) {
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
		w, body := completeAttachment(t, h, tc.device, tc.id)
		if w.Code != http.StatusOK {
			t.Fatalf("complete %s = %d body=%s", tc.id, w.Code, w.Body.String())
		}
		if body["reused"] != (tc.id == "att-2") {
			t.Fatalf("complete %s reused=%v", tc.id, body["reused"])
		}
	}

	for _, tc := range []struct {
		device, id string
	}{
		{"dev-1", "att-1"},
		{"dev-2", "att-2"},
	} {
		w := getAttachmentContent(t, h, tc.device, tc.id)
		if w.Code != http.StatusOK || w.Body.String() != string(content) {
			t.Fatalf("content %s = %d %q", tc.id, w.Code, w.Body.String())
		}
	}
}

// The request-shape matrix of the content endpoint: empty identifiers,
// missing or extra segments and non-GET methods are all 400 JSON errors,
// judged before any existence or authorization check, and nothing is written.
func TestAttachmentContentShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
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

	// Empty identifiers, trailing slashes and extra segments: 400 JSON.
	for _, path := range []string{
		"/v1/devices//attachments/att-1/content",
		"/v1/devices/dev-1/attachments//content",
		"/v1/devices/dev-1/attachments/att-1/content/",
		"/v1/devices/dev-1/attachments/att-1/content/extra",
		"/v1/devices/dev-1/attachments/att-1/content/extra/more",
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d, want 400", path, w.Code)
		} else {
			assertJSONError(t, w)
		}
	}

	// Non-GET methods on the exact path and on extra segments: 400 JSON.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		for _, path := range []string{
			"/v1/devices/dev-1/attachments/att-1/content",
			"/v1/devices/dev-1/attachments/att-1/content/extra",
		} {
			r := httptest.NewRequest(method, path, nil)
			if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400", method, path, w.Code)
			} else {
				assertJSONError(t, w)
			}
		}
	}

	// Shape is judged before existence: a non-GET request against an unknown
	// attachment is still a 400, not a 404.
	r := httptest.NewRequest(http.MethodDelete, "/v1/devices/dev-1/attachments/nope/content", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("DELETE unknown content = %d, want 400", w.Code)
	}

	// The rejections wrote nothing: the content still reads back intact.
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("content after rejections = %d %q", w.Code, w.Body.String())
	}
}

// The judgment order past the shape checks: existence (404) before
// authorization (403) before the seal state (409).
func TestAttachmentContentJudgmentOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)

	// Unsealed upload: a non-creator non-granted device gets 403 (not 409),
	// the creator gets 409, and an unknown id gets 404 either way.
	if w := getAttachmentContent(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("non-creator unsealed = %d, want 403", w.Code)
	}
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("creator unsealed = %d, want 409", w.Code)
	}
	if w := getAttachmentContent(t, h, "dev-2", "nope"); w.Code != http.StatusNotFound {
		t.Fatalf("non-creator unknown = %d, want 404", w.Code)
	}

	// A granted device reaches the seal check: 409 like the creator.
	w, _ := postJSON(t, h, "/v1/devices/dev-1/attachments/att-1/access", map[string]any{
		"deviceId": "dev-2", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d %s", w.Code, w.Body.String())
	}
	if w := getAttachmentContent(t, h, "dev-2", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("granted unsealed = %d, want 409", w.Code)
	} else {
		assertJSONError(t, w)
	}

	// The failed reads left no record: the upload resumes and seals normally.
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	putChunk(t, h, "dev-1", "att-1", 1, []byte("o wo"))
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete after 409 reads = %d %v", w.Code, body)
	}
	if w := getAttachmentContent(t, h, "dev-2", "att-1"); w.Code != http.StatusOK || w.Body.String() != string(content) {
		t.Fatalf("granted sealed read = %d %q", w.Code, w.Body.String())
	}
}

// A deleted attachment's content read is a 404 for everyone, and re-creating
// the id starts a brand-new upload whose unsealed read is a 409 again.
func TestAttachmentContentAfterDelete(t *testing.T) {
	h, _ := newTestHandler(t)
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
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("sealed read = %d", w.Code)
	}

	r := httptest.NewRequest(http.MethodDelete, "/v1/devices/dev-1/attachments/att-1", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusOK {
		t.Fatalf("delete = %d", w.Code)
	}
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("content after delete = %d, want 404", w.Code)
	} else {
		assertJSONError(t, w)
	}

	// The id is free again: a fresh upload under it is unsealed, so the
	// whole-content read is a 409 until the new upload finishes.
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	if w := getAttachmentContent(t, h, "dev-1", "att-1"); w.Code != http.StatusConflict {
		t.Fatalf("recreated unsealed read = %d, want 409", w.Code)
	}
}

// The whole-content read and its judgments are durable: after a restart the
// sealed bytes read back identically and the 403/404/409 decisions are
// unchanged.
func TestAttachmentContentSurvivesRestart(t *testing.T) {
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
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h, "/v1/devices/dev-1/attachments/att-1/access", map[string]any{
		"deviceId": "dev-2", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d %s", w.Code, w.Body.String())
	}
	// A second upload stays unsealed across the restart.
	mustCreateAttachment(t, h, "dev-1", "att-2", []byte("data"), 2)
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	for _, device := range []string{"dev-1", "dev-2"} {
		w := getAttachmentContent(t, h, device, "att-1")
		if w.Code != http.StatusOK || w.Body.String() != string(content) ||
			w.Header().Get("Content-Type") != "application/octet-stream" {
			t.Fatalf("content after restart (%s) = %d %q %q",
				device, w.Code, w.Body.String(), w.Header().Get("Content-Type"))
		}
	}
	if w := getAttachmentContent(t, h, "dev-1", "att-2"); w.Code != http.StatusConflict {
		t.Fatalf("unsealed after restart = %d, want 409", w.Code)
	}
	if w := getAttachmentContent(t, h, "dev-1", "nope"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown after restart = %d, want 404", w.Code)
	}
}

// The content read is read-only: it changes neither the metadata view nor the
// chunk-level reads, and it coexists with the per-chunk contract — the
// assembled body is exactly the per-chunk bytes in ascending index order.
func TestAttachmentContentMatchesPerChunkReads(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("the quick brown fox")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 3)
	for i := 0; i*3 < len(content); i++ {
		lo := i * 3
		hi := lo + 3
		if hi > len(content) {
			hi = len(content)
		}
		putChunk(t, h, "dev-1", "att-1", int64(i), content[lo:hi])
	}
	w, body := completeAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}

	// Assemble the expected body from the per-chunk reads.
	var want strings.Builder
	for i := int64(0); i*3 < int64(len(content)); i++ {
		r := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/v1/devices/dev-1/attachments/att-1/chunks/%d", i), nil)
		cw := serveRecorder(h, r)
		if cw.Code != http.StatusOK {
			t.Fatalf("chunk %d = %d", i, cw.Code)
		}
		want.WriteString(cw.Body.String())
	}

	w = getAttachmentContent(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || w.Body.String() != want.String() {
		t.Fatalf("whole content = %d %q, want per-chunk concatenation %q", w.Code, w.Body.String(), want.String())
	}

	// The read left no trace: metadata and chunk reads are unchanged.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1", nil)
	meta := serveRecorder(h, r)
	if !strings.Contains(meta.Body.String(), `"complete":true`) ||
		!strings.Contains(meta.Body.String(), `"receivedChunks":[0,1,2,3,4,5,6]`) {
		t.Fatalf("meta after content read = %s", meta.Body.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/0", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusOK || w.Body.String() != "the" {
		t.Fatalf("chunk 0 after content read = %d %q", w.Code, w.Body.String())
	}
}

// The whole-content read is read-only with respect to the document channels:
// it neither occupies a change cursor nor pushes a frame to a live
// subscription. The push channel still works — a real commit afterwards is
// delivered — proving the silence is the read's, not a dead subscription.
func TestAttachmentContentReadNoCursorOrSubscriptionPush(t *testing.T) {
	srv, _ := newWSTestServer(t)

	// Set up a creator with a sealed attachment, a session and one seeded
	// change, all over the public HTTP surface.
	resp, err := srv.Client().Post(srv.URL+"/v1/devices", "application/json",
		strings.NewReader(`{"deviceId":"dev-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register = %d", resp.StatusCode)
	}
	resp, err = srv.Client().Post(srv.URL+"/v1/devices/dev-1/sessions", "application/json",
		strings.NewReader(`{"sessionId":"sess"}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session = %d", resp.StatusCode)
	}
	resp, err = srv.Client().Post(srv.URL+"/v1/documents/doc/changes", "application/json",
		strings.NewReader(`{"deviceId":"dev-1","changes":[{"id":"seed","payload":{"n":1}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("seed change = %d", resp.StatusCode)
	}
	content := []byte("whole content bytes")
	mustCreateAttachment(t, srv.Config.Handler, "dev-1", "att-1", content, 4)
	for i := 0; i*4 < len(content); i++ {
		lo := i * 4
		hi := lo + 4
		if hi > len(content) {
			hi = len(content)
		}
		putChunk(t, srv.Config.Handler, "dev-1", "att-1", int64(i), content[lo:hi])
	}
	w, body := completeAttachment(t, srv.Config.Handler, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete = %d %v", w.Code, body)
	}

	// Assertion 1: the read does not occupy the change cursor.
	getContent := func() {
		t.Helper()
		r, err := http.NewRequest(http.MethodGet,
			srv.URL+"/v1/devices/dev-1/attachments/att-1/content", nil)
		if err != nil {
			t.Fatal(err)
		}
		cresp, err := srv.Client().Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := io.ReadAll(cresp.Body)
		_ = cresp.Body.Close()
		if cresp.StatusCode != http.StatusOK || !bytes.Equal(data, content) {
			t.Fatalf("content read = %d %q", cresp.StatusCode, data)
		}
	}
	getContent()
	getContent()
	lresp, err := srv.Client().Get(srv.URL + "/v1/documents/doc/changes")
	if err != nil {
		t.Fatal(err)
	}
	var list struct {
		NextCursor int64 `json:"nextCursor"`
	}
	if err := json.NewDecoder(lresp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	_ = lresp.Body.Close()
	if list.NextCursor != 1 {
		t.Fatalf("nextCursor = %d, want 1: the content read moved the cursor", list.NextCursor)
	}

	// Assertion 2: the read pushes nothing over a live subscription parked at
	// the high-water cursor.
	conn, upgrade := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("subscribe = %d", upgrade.StatusCode)
	}
	defer conn.close()
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before the content read")
	}
	conn.clearReadDeadline()

	getContent()

	conn.setReadDeadline(500 * time.Millisecond)
	if _, opcode, payload, ok := conn.readFrameMaybe(); ok {
		t.Fatalf("the content read pushed a frame (opcode %d): %s", opcode, payload)
	}
	conn.clearReadDeadline()

	// The subscription is alive: the next genuine commit is delivered at once.
	resp, err = srv.Client().Post(srv.URL+"/v1/documents/doc/changes", "application/json",
		strings.NewReader(`{"deviceId":"dev-1","changes":[{"id":"live","payload":{"n":2}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	frame := conn.readChange()
	if frame.Cursor != 2 || frame.ID != "live" {
		t.Fatalf("live frame after content read = %+v", frame)
	}
}
