package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// setPermission posts a grant/revoke request and returns the raw response.
func setPermission(t *testing.T, h http.Handler, docID, deviceID, action string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+docID+"/permissions",
		map[string]any{"deviceId": deviceID, "action": action})
}

func TestPermissionValidationWritesNothing(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	const url = "/v1/documents/doc/permissions"
	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-1","action":"revoke"}`},
		{"missing content type", "", `{"deviceId":"dev-1","action":"revoke"}`},
		{"malformed JSON", "application/json", `{"deviceId":`},
		{"trailing content", "application/json", `{"deviceId":"dev-1","action":"revoke"} {}`},
		{"empty deviceId", "application/json", `{"deviceId":"","action":"revoke"}`},
		{"missing deviceId", "application/json", `{"action":"revoke"}`},
		{"unknown action", "application/json", `{"deviceId":"dev-1","action":"deny"}`},
		{"missing action", "application/json", `{"deviceId":"dev-1"}`},
		{"non-string deviceId", "application/json", `{"deviceId":7,"action":"revoke"}`},
	}
	for _, tc := range cases {
		r := newJSONRequest(http.MethodPost, url, tc.body, tc.contentType)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400 (body %s)", tc.name, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Every rejected request left the permission state untouched.
	if ok, err := s.IsAuthorized("doc", "dev-1"); err != nil || !ok {
		t.Fatalf("after rejected requests: authorized=%v err=%v, want default authorized", ok, err)
	}
	// A first real revoke must still report a change, proving no partial write.
	w, body := setPermission(t, h, "doc", "dev-1", "revoke")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("first revoke after 400s = %d %v, want changed=true", w.Code, body)
	}
}

func TestPermissionUnknownDevice(t *testing.T) {
	h, _ := newTestHandler(t)

	w, body := setPermission(t, h, "doc", "ghost", "revoke")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if body["error"] == "" {
		t.Fatalf("body = %v, want a JSON error", body)
	}
}

func TestPermissionGrantRevokeSemantics(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Grant on the default-authorized state is a no-op.
	w, body := setPermission(t, h, "doc", "dev-1", "grant")
	if w.Code != http.StatusOK || body["deviceId"] != "dev-1" || body["authorized"] != true || body["changed"] != false {
		t.Fatalf("grant on default = %d %v", w.Code, body)
	}

	// First revoke flips the state.
	w, body = setPermission(t, h, "doc", "dev-1", "revoke")
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != true {
		t.Fatalf("first revoke = %d %v", w.Code, body)
	}
	// A repeat revoke is idempotent.
	w, body = setPermission(t, h, "doc", "dev-1", "revoke")
	if w.Code != http.StatusOK || body["authorized"] != false || body["changed"] != false {
		t.Fatalf("repeat revoke = %d %v", w.Code, body)
	}

	// Grant restores access; a repeat grant is idempotent.
	w, body = setPermission(t, h, "doc", "dev-1", "grant")
	if w.Code != http.StatusOK || body["authorized"] != true || body["changed"] != true {
		t.Fatalf("grant after revoke = %d %v", w.Code, body)
	}
	w, body = setPermission(t, h, "doc", "dev-1", "grant")
	if w.Code != http.StatusOK || body["authorized"] != true || body["changed"] != false {
		t.Fatalf("repeat grant = %d %v", w.Code, body)
	}
}

func TestSessionChangesForbiddenAfterRevoke(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	if w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"}); w.Code != http.StatusOK {
		t.Fatalf("create session = %d", w.Code)
	}
	if w, _ := postJSON(t, h, "/v1/devices/dev-2/sessions", map[string]any{"sessionId": "sess-2"}); w.Code != http.StatusOK {
		t.Fatalf("create session = %d", w.Code)
	}

	// Seed the document with a change.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"a": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post changes = %d", w.Code)
	}

	const sessURL = "/v1/sessions/sess-1/documents/doc/changes"
	w, body := doRequest(t, h, http.MethodGet, sessURL)
	if w.Code != http.StatusOK || body["nextCursor"] != float64(1) {
		t.Fatalf("before revoke = %d %v", w.Code, body)
	}

	if w, _ := setPermission(t, h, "doc", "dev-1", "revoke"); w.Code != http.StatusOK {
		t.Fatal("revoke failed")
	}

	// The revoked device's session is denied, with no page data in the body.
	w, body = doRequest(t, h, http.MethodGet, sessURL)
	if w.Code != http.StatusForbidden {
		t.Fatalf("after revoke = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	if _, ok := body["changes"]; ok {
		t.Fatalf("403 body must not carry changes: %v", body)
	}
	if _, ok := body["nextCursor"]; ok {
		t.Fatalf("403 body must not carry nextCursor: %v", body)
	}

	// Another device's session on the same document is unaffected.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-2/documents/doc/changes")
	if w.Code != http.StatusOK || body["nextCursor"] != float64(1) {
		t.Fatalf("other device = %d %v", w.Code, body)
	}
	// The document-scoped listing is unaffected.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK || body["nextCursor"] != float64(1) {
		t.Fatalf("document listing = %d %v", w.Code, body)
	}

	// Grant restores access, and the revoke did not delete anything.
	if w, _ := setPermission(t, h, "doc", "dev-1", "grant"); w.Code != http.StatusOK {
		t.Fatal("grant failed")
	}
	w, body = doRequest(t, h, http.MethodGet, sessURL)
	if w.Code != http.StatusOK || body["nextCursor"] != float64(1) {
		t.Fatalf("after grant = %d %v", w.Code, body)
	}
	changes, ok := body["changes"].([]any)
	if !ok || len(changes) != 1 {
		t.Fatalf("changes after grant = %v, want the original change", body["changes"])
	}
}

func TestSessionChangesValidationOrderWithRevocation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	if w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"}); w.Code != http.StatusOK {
		t.Fatalf("create session = %d", w.Code)
	}
	if w, _ := setPermission(t, h, "doc", "dev-1", "revoke"); w.Code != http.StatusOK {
		t.Fatal("revoke failed")
	}

	// Malformed query parameters still win over the permission check.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?after=-1")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad after = %d, want 400", w.Code)
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?limit=0")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad limit = %d, want 400", w.Code)
	}
	// An unknown session is still a 404, revocation or not.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/ghost/documents/doc/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d, want 404", w.Code)
	}
}

func TestPermissionSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perm.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	if w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"}); w.Code != http.StatusOK {
		t.Fatalf("create session = %d", w.Code)
	}
	if w, _ := setPermission(t, h, "doc", "dev-1", "revoke"); w.Code != http.StatusOK {
		t.Fatal("revoke failed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	// The denial is still enforced...
	w, _ := doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusForbidden {
		t.Fatalf("after restart = %d, want 403", w.Code)
	}
	// ...and idempotency is preserved: the revoke is not news.
	w, body := setPermission(t, h2, "doc", "dev-1", "revoke")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("revoke after restart = %d %v, want changed=false", w.Code, body)
	}
}

func TestPermissionConcurrentGrantRevoke(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Concurrent identical revokes: every response is a well-formed 200 and
	// exactly one reports the change.
	const n = 16
	results := make(chan map[string]any, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := setPermission(t, h, "doc", "dev-1", "revoke")
			if w.Code != http.StatusOK {
				t.Errorf("revoke = %d, want 200", w.Code)
				return
			}
			results <- body
		}()
	}
	wg.Wait()
	close(results)

	changed := 0
	for body := range results {
		if body["authorized"] != false {
			t.Errorf("authorized = %v, want false", body["authorized"])
		}
		if body["changed"] == true {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("changed=true count = %d, want exactly 1", changed)
	}
}
