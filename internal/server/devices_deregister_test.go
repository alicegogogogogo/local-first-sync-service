package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func deregister(t *testing.T, h http.Handler, deviceID string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodDelete, "/v1/devices/"+deviceID)
}

// deleteHTTP issues a bodyless DELETE against a real test server.
func deleteHTTP(t *testing.T, srv *httptest.Server, path string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, srv.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// TestDeregisterDeviceHTTPSuccessShape: one JSON line naming the device and the
// deletion marker and nothing else.
func TestDeregisterDeviceHTTPSuccessShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, body := deregister(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["deviceId"] != "dev-1" || body["deleted"] != true || len(body) != 2 {
		t.Fatalf("body = %v, want exactly {deviceId, deleted}", body)
	}
	// A single JSON object on one line.
	raw := strings.TrimRight(w.Body.String(), "\n")
	if strings.ContainsAny(raw, "\r\n") || !json.Valid([]byte(raw)) {
		t.Fatalf("body %q is not one JSON line", w.Body.String())
	}
}

func TestDeregisterDeviceHTTPUnknownAndRepeat404(t *testing.T) {
	h, _ := newTestHandler(t)

	// Never registered.
	w, body := deregister(t, h, "ghost")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown device = %d %v", w.Code, body)
	}

	registerDevice(t, h, "dev-1")
	w, _ = deregister(t, h, "dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Repeat deregistration: 404 JSON, zero writes.
	w, body = deregister(t, h, "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("repeat deregistration = %d %v", w.Code, body)
	}
}

func TestDeregisterDeviceHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	cases := []struct {
		method string
		path   string
	}{
		// Method mismatch on the item path.
		{http.MethodGet, "/v1/devices/dev-1"},
		{http.MethodPost, "/v1/devices/dev-1"},
		{http.MethodPut, "/v1/devices/dev-1"},
		{http.MethodPatch, "/v1/devices/dev-1"},
		// Missing id segment and extra segments.
		{http.MethodDelete, "/v1/devices"},
		{http.MethodDelete, "/v1/devices/dev-1/extra"},
		{http.MethodDelete, "/v1/devices/dev-1/extra/more"},
		// Empty segment / trailing slash.
		{http.MethodDelete, "/v1/devices//"},
		{http.MethodDelete, "/v1/devices/dev-1/"},
	}
	for _, tc := range cases {
		r := newJSONRequest(tc.method, tc.path, "", "")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
	}

	// Zero writes: the device still exists (its session endpoint works).
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "s"})
	if w.Code != http.StatusOK {
		t.Fatalf("device should survive rejected deregistrations: %d %s", w.Code, w.Body.String())
	}
}

// TestDeregisterDeviceHTTPCascade verifies the full HTTP-visible cascade:
// sessions, owned attachments, grants in both directions, lists and rosters.
func TestDeregisterDeviceHTTPCascade(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// dev-1 and dev-2 sessions.
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/devices/dev-2/sessions", map[string]any{"sessionId": "sess-2"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// dev-1 owns a completed attachment dev-2 is granted access to.
	content1 := []byte("dev-one bytes")
	mustCreateAttachment(t, h, "dev-1", "att-1", content1, 8)
	w, _ = putChunk(t, h, "dev-1", "att-1", 0, content1[:8])
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = putChunk(t, h, "dev-1", "att-1", 1, content1[8:])
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ = completeAttachment(t, h, "dev-1", "att-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/devices/dev-1/attachments/att-1/access",
		map[string]any{"deviceId": "dev-2", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// dev-2 owns an attachment dev-1 is granted access to.
	content2 := []byte("dev-two bytes!!")
	mustCreateAttachment(t, h, "dev-2", "att-2", content2, 8)
	for i, off := 0, 0; off < len(content2); i, off = i+1, off+8 {
		end := off + 8
		if end > len(content2) {
			end = len(content2)
		}
		if w, _ = putChunk(t, h, "dev-2", "att-2", int64(i), content2[off:end]); w.Code != http.StatusOK {
			t.Fatalf("chunk %d: %s", i, w.Body.String())
		}
	}
	if w, _ = completeAttachment(t, h, "dev-2", "att-2"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/devices/dev-2/attachments/att-2/access",
		map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A change posted by dev-1 survives deregistration: other devices' reads
	// are untouched.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// dev-1's document permission is revoked before deregistration, so the
	// fresh re-registration must not inherit either the row or the denial.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w, _ = deregister(t, h, "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Sessions hard-deleted: session-scoped reads 404.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted session read = %d, want 404", w.Code)
	}
	// dev-2's session still serves.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-2/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("other device session read = %d, want 200", w.Code)
	}

	// Owned attachment gone for everyone: 404 on read, chunks and roster.
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-1"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted attachment read = %d, want 404", w.Code)
	}
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-1/chunks/0"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted attachment chunk = %d, want 404", w.Code)
	}
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-1/access"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted attachment roster = %d, want 404", w.Code)
	}
	// dev-2's listing no longer contains the deregistered device's attachment.
	w, body := doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for _, item := range body["attachments"].([]any) {
		if item.(map[string]any)["attachmentId"] == "att-1" {
			t.Fatal("deleted attachment still listed for a former grantee")
		}
	}
	// dev-2's own attachment survives and its roster no longer names dev-1.
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-2"); w.Code != http.StatusOK {
		t.Fatalf("other device attachment = %d, want 200", w.Code)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/devices/dev-2/attachments/att-2/access")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if devices := body["devices"].([]any); len(devices) != 0 {
		t.Fatalf("deregistered grantee still rostered: %v", devices)
	}

	// Change log and document content survive.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("change log after deregistration = %d %v", w.Code, body)
	}

	// Re-register: brand-new device. It inherits no sessions, attachments or
	// grants, and the prior revoke is gone (a replay is authorized again).
	registerDevice(t, h, "dev-1")
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc/changes"); w.Code != http.StatusNotFound {
		t.Fatalf("inherited session = %d, want 404", w.Code)
	}
	if w, _ = doRequest(t, h, http.MethodGet, "/v1/devices/dev-1/attachments/att-2"); w.Code != http.StatusForbidden {
		t.Fatalf("inherited read grant = %d, want 403", w.Code)
	}
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev-1",
		"operations": []any{
			map[string]any{"id": "c2", "payload": map[string]any{"k2": 2}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("fresh device replay = %d %s, want 200", w.Code, w.Body.String())
	}
}

func TestDeregisterDeviceHTTPConcurrentAtMostOnce(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "hot")

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, miss := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := serveRecorder(h, newJSONRequest(http.MethodDelete, "/v1/devices/hot", "", ""))
			mu.Lock()
			defer mu.Unlock()
			switch w.Code {
			case http.StatusOK:
				ok++
			case http.StatusNotFound:
				miss++
			default:
				t.Errorf("status = %d body = %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if ok != 1 || miss != n-1 {
		t.Fatalf("ok = %d, miss = %d, want 1 and %d", ok, miss, n-1)
	}

	// Re-registration after the concurrent cascade is a fresh device.
	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "hot"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("re-register = %d %v", w.Code, body)
	}
}

func TestDeregisterDeviceHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deregister-http.db")
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
	if w, _ = deregister(t, h, "dev-1"); w.Code != http.StatusOK {
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

	w, body := deregister(t, h2, "dev-1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("repeat deregistration after restart = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h2, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("re-register after restart = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/d/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("session read after restart = %d, want 404", w.Code)
	}
}

// TestDeregisterDeviceHTTPClosesSubscriptions: a deregistration ends the
// device's live change-log and CRDT subscriptions immediately, stickily.
func TestDeregisterDeviceHTTPClosesSubscriptions(t *testing.T) {
	t.Run("change log", func(t *testing.T) {
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

		if code := deleteHTTP(t, srv, "/v1/devices/dev-1"); code != http.StatusOK {
			t.Fatalf("deregister = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4403 {
			t.Fatalf("close code = %d, want 4403", code)
		}
		conn.clearReadDeadline()

		// A reconnect finds the backing session gone: 404 before the upgrade.
		if _, resp2 := dialWS(t, subscribeURL(srv, "sess", "doc", "0")); resp2.StatusCode != http.StatusNotFound {
			t.Fatalf("reconnect = %d, want 404", resp2.StatusCode)
		}
	})

	t.Run("crdt state", func(t *testing.T) {
		srv, _ := newWSTestServer(t)
		setupSession(t, srv, "dev-1", "sess", "doc", 0)

		conn, resp := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
		if conn == nil {
			t.Fatalf("status = %d", resp.StatusCode)
		}
		defer conn.close()

		if code := deleteHTTP(t, srv, "/v1/devices/dev-1"); code != http.StatusOK {
			t.Fatalf("deregister = %d", code)
		}
		conn.setReadDeadline(2 * time.Second)
		if code := conn.readCloseCode(); code != 4403 {
			t.Fatalf("close code = %d, want 4403", code)
		}
		conn.clearReadDeadline()
	})
}
