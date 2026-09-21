package server

import (
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func TestSetPermissionHTTPSuccessAndIdempotency(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	const url = "/v1/documents/doc1/permissions"

	// Devices start authorized: the first grant is a no-op.
	w, body := postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d body = %s", w.Code, w.Body.String())
	}
	if body["deviceId"] != "dev-1" || body["authorized"] != true || body["changed"] != false {
		t.Fatalf("grant body = %v", body)
	}

	// First revoke flips the state.
	w, body = postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != true {
		t.Fatalf("revoke = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Repeating the revoke is idempotent.
	w, body = postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != false {
		t.Fatalf("revoke retry = %d %v", w.Code, body)
	}

	// Re-grant flips back; repeating it is idempotent again.
	w, body = postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK || body["authorized"] != true || body["changed"] != true {
		t.Fatalf("re-grant = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("grant retry = %d %v", w.Code, body)
	}
}

func TestSetPermissionHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	const url = "/v1/documents/doc1/permissions"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-1","action":"revoke"}`},
		{"missing content type", "", `{"deviceId":"dev-1","action":"revoke"}`},
		{"json suffix content type", "application/vnd.api+json", `{"deviceId":"dev-1","action":"revoke"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"dev-1","action":"revoke"}x`},
		{"empty deviceId", "application/json", `{"deviceId":"","action":"revoke"}`},
		{"missing deviceId", "application/json", `{"action":"revoke"}`},
		{"numeric deviceId", "application/json", `{"deviceId":7,"action":"revoke"}`},
		{"null deviceId", "application/json", `{"deviceId":null,"action":"revoke"}`},
		{"missing action", "application/json", `{"deviceId":"dev-1"}`},
		{"empty action", "application/json", `{"deviceId":"dev-1","action":""}`},
		{"unknown action", "application/json", `{"deviceId":"dev-1","action":"block"}`},
		{"numeric action", "application/json", `{"deviceId":"dev-1","action":1}`},
		{"null action", "application/json", `{"deviceId":"dev-1","action":null}`},
		{"array body", "application/json", `[1,2]`},
		{"string body", "application/json", `"revoke"`},
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

	// Zero writes: the device is still on the default authorized state, so a
	// real revoke now must report changed=true.
	if ok, err := s.DocumentAuthorized("doc1", "dev-1"); err != nil || !ok {
		t.Fatalf("zero-write violated: authorized=%v err=%v", ok, err)
	}
	w, body := postJSON(t, h, url, map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke after rejects = %d %v", w.Code, body)
	}
}

func TestSetPermissionHTTPUnknownDevice(t *testing.T) {
	h, s := newTestHandler(t)

	w, body := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "ghost",
		"action":   "revoke",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
	if body["error"] == "" {
		t.Fatalf("body = %v", body)
	}

	// Zero writes: registering the id later still starts authorized, so the
	// first real revoke reports changed=true.
	registerDevice(t, h, "ghost")
	if ok, err := s.DocumentAuthorized("doc1", "ghost"); err != nil || !ok {
		t.Fatalf("zero-write violated: authorized=%v err=%v", ok, err)
	}
	w, body = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "ghost",
		"action":   "revoke",
	})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke after late register = %d %v", w.Code, body)
	}
}

func TestSessionChangesHTTPRevokedDevice403(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for i := 0; i < 2; i++ {
		w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c%d", i), "payload": map[string]any{"i": i}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	// Authorized by default: the session view works.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 2 {
		t.Fatalf("before revoke = %d %v", w.Code, body)
	}

	// Revoke the session's device for this document.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1",
		"action":   "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The session view is now a 403 JSON error carrying neither changes nor
	// nextCursor.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked read = %d, want 403, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
	if _, ok := body["changes"]; ok {
		t.Fatalf("403 must not return changes: %v", body)
	}
	if _, ok := body["nextCursor"]; ok {
		t.Fatalf("403 must not return nextCursor: %v", body)
	}

	// Revocation deletes nothing: the document view, merge and restore surface
	// are untouched.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 2 {
		t.Fatalf("document view after revoke = %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"i": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post after revoke = %d %s", w.Code, w.Body.String())
	}

	// Validation order is unchanged: bad params still 400, unknown session
	// still 404, even while the permission is revoked.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes?limit=0")
	if w.Code != http.StatusBadRequest || body["error"] == nil {
		t.Fatalf("bad params while revoked = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/ghost/documents/doc1/changes")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session while revoked = %d %v", w.Code, body)
	}

	// The revocation is per document: another document reads fine.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/other/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("other document while revoked = %d %v", w.Code, body)
	}

	// Re-grant restores the original session view.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1",
		"action":   "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 3 || body["nextCursor"].(float64) != 3 {
		t.Fatalf("after re-grant = %d %v", w.Code, body)
	}
}

func TestPermissionHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1",
		"action":   "revoke",
	})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke = %d %v", w.Code, body)
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

	// The revoked state and its idempotency survive the restart.
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusForbidden {
		t.Fatalf("read after restart = %d, want 403", w.Code)
	}
	w, body = postJSON(t, h2, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1",
		"action":   "revoke",
	})
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("revoke replay after restart = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h2, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1",
		"action":   "grant",
	})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant after restart = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("read after re-grant = %d, want 200", w.Code)
	}
}

func TestConcurrentGrantRevokeHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	const url = "/v1/documents/doc/permissions"

	// Concurrent identical revokes: exactly one reports changed=true.
	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	changed := 0
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := postJSON(t, h, url, map[string]any{"deviceId": "dev", "action": "revoke"})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("status %d: %s", w.Code, w.Body.String())
				return
			}
			if body["changed"] == true {
				mu.Lock()
				changed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if changed != 1 {
		t.Fatalf("changed reporters = %d, want 1", changed)
	}

	// Concurrent grant vs. revoke settles into one coherent state: a repeat of
	// the observed state is a no-op.
	var wg2 sync.WaitGroup
	statuses := make(chan int, 2)
	wg2.Add(2)
	for _, action := range []string{"grant", "revoke"} {
		go func(action string) {
			defer wg2.Done()
			w, _ := postJSON(t, h, url, map[string]any{"deviceId": "dev", "action": action})
			statuses <- w.Code
		}(action)
	}
	wg2.Wait()
	close(statuses)
	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("racing grant/revoke status = %d", code)
		}
	}
	// Settle: force a known state, then confirm a repeat is a no-op.
	w, _ := postJSON(t, h, url, map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, url, map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("settled revoke repeat = %d %v", w.Code, body)
	}
}

// Ensure a racing read observes either the full pre- or post-revoke state,
// never an error or a partial response.
func TestConcurrentRevokeAndSessionReadHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
			"deviceId": "dev",
			"action":   "revoke",
		})
		if w.Code != http.StatusOK {
			errs <- fmt.Errorf("revoke status %d", w.Code)
		}
	}()
	go func() {
		defer wg.Done()
		w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
		switch w.Code {
		case http.StatusOK:
			if len(body["changes"].([]any)) != 1 {
				errs <- fmt.Errorf("200 without the change: %v", body)
			}
		case http.StatusForbidden:
			if _, ok := body["changes"]; ok {
				errs <- fmt.Errorf("403 with changes: %v", body)
			}
		default:
			errs <- fmt.Errorf("read status %d", w.Code)
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
