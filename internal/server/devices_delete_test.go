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
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func deleteDevice(t *testing.T, h http.Handler, deviceID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodDelete, "/v1/devices/"+deviceID)
}

// A registered device deregisters with 200 {"deviceId","deleted":true}; an
// unknown id and a repeat deregistration both miss with a 404 JSON error and
// write nothing.
func TestDeleteDeviceHTTPSuccessAndRepeat(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, body := deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d body = %s", w.Code, w.Body.String())
	}
	if body["deviceId"] != "dev-1" || body["deleted"] != true || len(body) != 2 {
		t.Fatalf("body = %v", body)
	}
	// The success body is a single JSON line carrying exactly the device id
	// and the deletion marker, in that key order.
	if got := strings.TrimSuffix(w.Body.String(), "\n"); got != `{"deviceId":"dev-1","deleted":true}` {
		t.Fatalf("raw body = %q", got)
	}

	// Repeat deregistration and a never-registered id both miss.
	for _, id := range []string{"dev-1", "ghost"} {
		w, body = deleteDevice(t, h, id)
		if w.Code != http.StatusNotFound || body["error"] == nil {
			t.Fatalf("delete %q = %d %v", id, w.Code, body)
		}
		assertJSONError(t, w)
	}

	// Zero writes on the misses: the id registers as a brand-new device.
	w, body = postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("re-register = %d %v", w.Code, body)
	}
}

// Empty identifiers, missing or extra path segments and mismatched methods on
// the deregistration path are all 400 JSON errors — never a redirect or HTML.
func TestDeleteDeviceShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifier and trailing slash.
		{http.MethodDelete, "/v1/devices//"},
		{http.MethodDelete, "/v1/devices/dev-1/"},
		// Missing the device id segment.
		{http.MethodDelete, "/v1/devices"},
		// Extra segments past the device id.
		{http.MethodDelete, "/v1/devices/dev-1/extra"},
		{http.MethodDelete, "/v1/devices/dev-1/extra/more"},
		// Methods other than DELETE on the item path.
		{http.MethodGet, "/v1/devices/dev-1"},
		{http.MethodPost, "/v1/devices/dev-1"},
		{http.MethodPut, "/v1/devices/dev-1"},
		{http.MethodPatch, "/v1/devices/dev-1"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
	}

	// Zero writes: the device is still registered.
	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("device vanished after rejected deletes: %d %v", w.Code, body)
	}
}

// Deregistration hard-deletes the device's sessions: session-scoped reads and
// the subscription handshake miss with 404 afterwards, and the session id is
// free again under the re-registered device.
func TestDeleteDeviceRemovesSessions(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ = deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The session view of the change log now misses.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("session read after deregistration = %d %v", w.Code, body)
	}
	// The session-scoped CRDT read misses the same way.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("session crdt read after deregistration = %d, want 404", w.Code)
	}
	// The document's own change log is untouched.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("document changes after deregistration = %d %v", w.Code, body)
	}

	// The session id does not leak into the next device of the same name.
	registerDevice(t, h, "dev-1")
	w, body = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("session recreate = %d %v", w.Code, body)
	}
}

// Document permission rows naming the device are cleared: a device that
// registers again under the same id starts authorized (the default), while
// every other device's permission state is untouched.
func TestDeleteDeviceClearsPermissions(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	for _, dev := range []string{"dev-1", "dev-2"} {
		w, _ := postJSON(t, h, "/v1/devices/"+dev+"/sessions", map[string]any{"sessionId": "sess-" + dev})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	// Revoke both devices on the document.
	for _, dev := range []string{"dev-1", "dev-2"} {
		w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": dev, "action": "revoke"})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	w, _ := deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Permission writes targeting the gone device miss with 404.
	w, body := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("permission for gone device = %d %v", w.Code, body)
	}

	// The other device's revocation survives.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-dev-2/documents/doc/changes")
	if w.Code != http.StatusForbidden {
		t.Fatalf("other device session read = %d, want 403", w.Code)
	}

	// A same-named new device inherits nothing: it is authorized by default.
	registerDevice(t, h, "dev-1")
	w, _ = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-new"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-new/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("re-registered device session read = %d, want 200", w.Code)
	}
}

// The device's attachments vanish with it — metadata, chunks, completion
// state and grants — while deduplicated content shared with another device's
// completed attachment keeps its bytes, and grants other devices extended to
// the gone device lapse.
func TestDeleteDeviceCascadesAttachments(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	shared := []byte("shared content")
	// dev-1 owns a completed attachment; dev-2 owns a completed attachment
	// with the same digest (reused bytes) and one still incomplete.
	mustCreateAttachment(t, h, "dev-1", "att-owned", shared, 7)
	putChunk(t, h, "dev-1", "att-owned", 0, shared[:7])
	putChunk(t, h, "dev-1", "att-owned", 1, shared[7:])
	if w, _ := completeAttachment(t, h, "dev-1", "att-owned"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	mustCreateAttachment(t, h, "dev-2", "att-shared", shared, 7)
	putChunk(t, h, "dev-2", "att-shared", 0, shared[:7])
	putChunk(t, h, "dev-2", "att-shared", 1, shared[7:])
	w, body := completeAttachment(t, h, "dev-2", "att-shared")
	if w.Code != http.StatusOK || body["reused"] != true {
		t.Fatalf("dev-2 complete = %d %v", w.Code, body)
	}
	mustCreateAttachment(t, h, "dev-2", "att-open", []byte("unfinished upload"), 4)
	// dev-2 grants dev-1 read access to its open upload.
	w, _ = postJSON(t, h, "/v1/devices/dev-2/attachments/att-open/access", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ = deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The gone device's attachment reads 404 for everyone, creator paths
	// included; its uploads no longer accept chunks or completion.
	for _, url := range []string{
		"/v1/devices/dev-1/attachments/att-owned",
		"/v1/devices/dev-2/attachments/att-owned",
		"/v1/devices/dev-1/attachments/att-owned/chunks/0",
		"/v1/devices/dev-1/attachments/att-owned/access",
	} {
		w, _ = doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusNotFound {
			t.Fatalf("GET %s after deregistration = %d, want 404", url, w.Code)
		}
	}
	w, _ = putChunk(t, h, "dev-1", "att-owned", 0, shared[:7])
	if w.Code != http.StatusNotFound {
		t.Fatalf("chunk write to gone device's attachment = %d, want 404", w.Code)
	}
	w, _ = completeAttachment(t, h, "dev-1", "att-owned")
	if w.Code != http.StatusNotFound {
		t.Fatalf("complete on gone device's attachment = %d, want 404", w.Code)
	}

	// The deduplicated bytes survive: dev-2's completed attachment still
	// reads its content.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-shared/chunks/0")
	if w.Code != http.StatusOK {
		t.Fatalf("shared content chunk after deregistration = %d, want 200", w.Code)
	}

	// The grant dev-2 extended to dev-1 lapsed: the roster no longer lists
	// it, and dev-2's own grant on its attachment is the only state left.
	w, body = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-open/access")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if devices := body["devices"].([]any); len(devices) != 0 {
		t.Fatalf("roster after deregistration = %v, want empty", devices)
	}

	// The gone device appears in no listing; the surviving device's listing
	// still shows its own two attachments.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments")
	if w.Code != http.StatusNotFound {
		t.Fatalf("gone device listing = %d, want 404", w.Code)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments")
	if w.Code != http.StatusOK || len(body["attachments"].([]any)) != 2 {
		t.Fatalf("surviving device listing = %d %v", w.Code, body)
	}

	// A same-named new device inherits nothing: the old attachment id is
	// free and the re-registered device has no read access to dev-2's upload.
	registerDevice(t, h, "dev-1")
	w, body = createAttachment(t, h, "dev-1", "att-owned", int64(len(shared)), 7, sha256Hex(shared))
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("attachment recreate = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att-open")
	if w.Code != http.StatusForbidden {
		t.Fatalf("re-registered device reading granted-then-gone upload = %d, want 403", w.Code)
	}
}

// Concurrent deregistrations of the same device commit at most once: exactly
// one 200, every other a 404, and the device is gone afterwards.
func TestDeleteDeviceConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	deleted, missed := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, _ := deleteDevice(t, h, "dev")
			mu.Lock()
			switch w.Code {
			case http.StatusOK:
				deleted++
			case http.StatusNotFound:
				missed++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if deleted != 1 || missed != n-1 {
		t.Fatalf("deleted=%d missed=%d, want 1 and %d", deleted, missed, n-1)
	}

	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("re-register after race = %d %v", w.Code, body)
	}
}

// A deregistration raced against chunk uploads and completion of the device's
// own attachment leaves no half-written state: the upload either completed
// before the delete (and vanished with it) or fails afterwards with 404.
func TestDeleteDeviceRacingUploadLeavesNoHalfState(t *testing.T) {
	for i := 0; i < 8; i++ {
		h, _ := newTestHandler(t)
		registerDevice(t, h, "dev")
		content := []byte("racing bytes")
		mustCreateAttachment(t, h, "dev", "att", content, 4)

		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			putChunk(t, h, "dev", "att", 0, content[:4])
		}()
		go func() {
			defer wg.Done()
			putChunk(t, h, "dev", "att", 1, content[4:8])
		}()
		go func() {
			defer wg.Done()
			deleteDevice(t, h, "dev")
		}()
		wg.Wait()
		// Whatever the interleaving, the attachment is gone whole: no chunks,
		// no metadata, no completion state.
		w, _ := doRequest(t, h, http.MethodGet, "/v1/devices/dev/attachments/att")
		if w.Code != http.StatusNotFound {
			t.Fatalf("iteration %d: attachment after race = %d, want 404", i, w.Code)
		}
		w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev/attachments/att/chunks/0")
		if w.Code != http.StatusNotFound {
			t.Fatalf("iteration %d: chunk after race = %d, want 404", i, w.Code)
		}
	}
}

// Deregistration ends the device's live subscriptions immediately: the push
// channel closes the connection without waiting for the client.
func TestDeleteDeviceEndsSubscriptions(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed frame = %+v", c)
	}

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/v1/devices/dev-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	delResp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("delete device: %v", err)
	}
	delResp.Body.Close()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete device status = %d", delResp.StatusCode)
	}

	// The connection is ended by the server; the close code is the
	// access-withdrawn one the push channel already uses.
	conn.setReadDeadline(2 * time.Second)
	code := conn.readCloseCode()
	conn.clearReadDeadline()
	if code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}

	// The session is gone, so a fresh subscription handshake misses with 404.
	conn2, resp2 := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn2 != nil {
		conn2.close()
		t.Fatal("subscription succeeded under a deregistered device")
	}
	if resp2.StatusCode != http.StatusNotFound {
		t.Fatalf("re-subscribe = %d, want 404", resp2.StatusCode)
	}
}

// The deregistration result is durable: after a restart the device is still
// gone, a repeat deregistration misses with 404 and writes nothing, and the
// cascaded state (sessions, permissions, attachments) stays removed.
func TestDeleteDeviceHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	content := []byte("durable bytes")
	mustCreateAttachment(t, h, "dev-1", "att", content, 4)
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// The device and everything it owned are still gone.
	w, body := deleteDevice(t, h2, "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("repeat delete after restart = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("session read after restart = %d, want 404", w.Code)
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/devices/dev-1/attachments/att")
	if w.Code != http.StatusNotFound {
		t.Fatalf("attachment read after restart = %d, want 404", w.Code)
	}

	// The id registers again as a brand-new device: no inherited sessions,
	// permissions or attachments.
	w, body = postJSON(t, h2, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("re-register after restart = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h2, http.MethodGet, "/v1/devices/dev-1/attachments")
	if w.Code != http.StatusOK || len(body["attachments"].([]any)) != 0 {
		t.Fatalf("re-registered device listing = %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h2, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("re-registered device session read = %d, want 200 (no inherited revoke)", w.Code)
	}
}

// Other devices' change history, snapshots and document content are not
// affected by a deregistration.
func TestDeleteDeviceLeavesOtherDevicesData(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	for i, dev := range []string{"dev-1", "dev-2"} {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
			"deviceId": dev,
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c-%s", dev), "payload": map[string]any{"n": i}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ = deleteDevice(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Both devices' changes remain in the log, including the gone device's.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("changes after deregistration = %d %v", w.Code, body)
	}
	// The snapshot reads back unchanged.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/2")
	if w.Code != http.StatusOK || body["state"] == nil {
		t.Fatalf("snapshot after deregistration = %d %v", w.Code, body)
	}
	// The surviving device keeps writing and reading as before.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "c-after", "payload": map[string]any{"n": 3}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("surviving device write = %d %s", w.Code, w.Body.String())
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(w.Body.String()), &decoded); err != nil {
		t.Fatal(err)
	}
}
