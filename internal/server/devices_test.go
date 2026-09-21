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

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func registerDevice(t *testing.T, h http.Handler, deviceID string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": deviceID})
	if w.Code != http.StatusOK {
		t.Fatalf("register %s: status = %d body = %s", deviceID, w.Code, w.Body.String())
	}
}

func newRawRequest(method, url string) *http.Request {
	return httptest.NewRequest(method, url, nil)
}

func newJSONRequest(t *testing.T, method, url, contentType, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, url, strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	return r
}

func serveRecorder(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func assertJSONError(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("error content type = %q", ct)
	}
	var b map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
		t.Fatalf("body = %q, want JSON error", w.Body.String())
	}
}

func decodeBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var b map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode body: %v body=%s", err, w.Body.String())
	}
	return b
}

func TestRegisterDeviceHTTP(t *testing.T) {
	h, _ := newTestHandler(t)

	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["deviceId"] != "dev-1" || body["created"] != true {
		t.Fatalf("body = %v", body)
	}

	// Retry: idempotent.
	w, body = postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != false || body["deviceId"] != "dev-1" {
		t.Fatalf("retry = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

func TestRegisterDeviceHTTPValidation(t *testing.T) {
	h, s := newTestHandler(t)
	const url = "/v1/devices"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"d"}`},
		{"missing content type", "", `{"deviceId":"d"}`},
		{"json suffix content type", "application/vnd.api+json", `{"deviceId":"d"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"d"} garbage`},
		{"empty body", "application/json", ``},
		{"json null", "application/json", `null`},
		{"json array", "application/json", `["d"]`},
		{"json scalar", "application/json", `"d"`},
		{"missing deviceId", "application/json", `{}`},
		{"empty deviceId", "application/json", `{"deviceId":""}`},
		{"numeric deviceId", "application/json", `{"deviceId":7}`},
		{"boolean deviceId", "application/json", `{"deviceId":true}`},
		{"null deviceId", "application/json", `{"deviceId":null}`},
		{"array deviceId", "application/json", `{"deviceId":["d"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := serveRecorder(h, newJSONRequest(t, http.MethodPost, url, tc.contentType, tc.body))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: no rejected body registered a device (the ids used in the
	// cases above are all absent).
	if exists, _ := s.DeviceExists("d"); exists {
		t.Fatal("zero-write violated: rejected bodies registered a device")
	}

	// Unknown fields are tolerated (matches the existing endpoints' decoder).
	w := serveRecorder(h, newJSONRequest(t, http.MethodPost, url, "application/json", `{"deviceId":"ok-extra","extra":1}`))
	if w.Code != http.StatusOK {
		t.Fatalf("extra field status = %d body = %s", w.Code, w.Body.String())
	}

	// A charset parameter is still application/json.
	w = serveRecorder(h, newJSONRequest(t, http.MethodPost, url, "application/json; charset=utf-8", `{"deviceId":"e"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("charset status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestCreateSessionHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	registerDevice(t, h, "dev-B")

	// Unknown device -> 404.
	w, body := postJSON(t, h, "/v1/devices/ghost/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown device = %d %v", w.Code, body)
	}

	// First create.
	w, body = postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["sessionId"] != "s1" || body["created"] != true {
		t.Fatalf("body = %v", body)
	}

	// Retry on owning device: idempotent.
	w, body = postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK || body["created"] != false || body["sessionId"] != "s1" {
		t.Fatalf("retry = %d %v", w.Code, body)
	}

	// Same id on another device: 409 JSON.
	w, body = postJSON(t, h, "/v1/devices/dev-B/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("conflict body = %s", w.Body.String())
	}
}

func TestCreateSessionHTTPValidation(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	const url = "/v1/devices/dev-A/sessions"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"sessionId":"s1"}`},
		{"missing content type", "", `{"sessionId":"s1"}`},
		{"json suffix content type", "application/vnd.api+json", `{"sessionId":"s1"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"sessionId":"s1"}xx`},
		{"empty body", "application/json", ``},
		{"json null", "application/json", `null`},
		{"json array", "application/json", `[]`},
		{"missing sessionId", "application/json", `{}`},
		{"empty sessionId", "application/json", `{"sessionId":""}`},
		{"numeric sessionId", "application/json", `{"sessionId":1}`},
		{"boolean sessionId", "application/json", `{"sessionId":false}`},
		{"null sessionId", "application/json", `{"sessionId":null}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := serveRecorder(h, newJSONRequest(t, http.MethodPost, url, tc.contentType, tc.body))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: no session exists for any rejected id, including against
	// unknown devices.
	w, _ := postJSON(t, h, "/v1/devices/ghost/sessions", map[string]any{"sessionId": "s2"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d", w.Code)
	}
	if exists, _ := s.SessionExists("s1"); exists {
		t.Fatal("zero-write violated: session s1 created by rejected request")
	}
	if exists, _ := s.SessionExists("s2"); exists {
		t.Fatal("zero-write violated: session s2 created against unknown device")
	}
}

func TestCreateSessionConflictZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	registerDevice(t, h, "dev-B")

	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The conflicting create must leave ownership with dev-A.
	w, _ = postJSON(t, h, "/v1/devices/dev-B/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d", w.Code)
	}
	// Retry as dev-A is still idempotent, proving dev-B never took the id.
	w, body := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("owner retry = %d %v", w.Code, body)
	}
	if exists, _ := s.SessionExists("s1"); !exists {
		t.Fatal("session vanished after conflict")
	}
}

func TestDeleteSessionHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	registerDevice(t, h, "dev-B")
	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/devices/dev-B/sessions", map[string]any{"sessionId": "s2"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	delete := func(url string) *httptest.ResponseRecorder {
		t.Helper()
		return serveRecorder(h, newRawRequest(http.MethodDelete, url))
	}

	// Ownership mismatch: 404 and the session stays.
	if w := delete("/v1/devices/dev-B/sessions/s1"); w.Code != http.StatusNotFound {
		t.Fatalf("wrong owner = %d", w.Code)
	}
	if w := delete("/v1/devices/dev-A/sessions/nope"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d", w.Code)
	}
	if w := delete("/v1/devices/ghost/sessions/s1"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d", w.Code)
	}

	// Success.
	w2 := delete("/v1/devices/dev-A/sessions/s1")
	if w2.Code != http.StatusOK {
		t.Fatalf("delete status = %d body = %s", w2.Code, w2.Body.String())
	}
	if got := decodeBody(t, w2)["deleted"]; got != true {
		t.Fatalf("delete body = %s", w2.Body.String())
	}

	// Re-delete: 404.
	if w := delete("/v1/devices/dev-A/sessions/s1"); w.Code != http.StatusNotFound {
		t.Fatalf("re-delete = %d, want 404", w.Code)
	}

	// The freed id can be claimed by another device.
	w, body := postJSON(t, h, "/v1/devices/dev-B/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("recreate = %d %v", w.Code, body)
	}

	// Unrelated session untouched: deleting as its owner still works once.
	if w := delete("/v1/devices/dev-B/sessions/s2"); w.Code != http.StatusOK {
		t.Fatalf("unrelated session delete = %d", w.Code)
	}
}

func TestSessionChangesHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Seed a document through the existing write API.
	for i := 0; i < 3; i++ {
		w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
			"deviceId": "dev-A",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"i": i + 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	// Live session: same query semantics as GET /v1/documents/{id}/changes.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	list := body["changes"].([]any)
	if len(list) != 3 || body["nextCursor"].(float64) != 3 {
		t.Fatalf("list = %v next=%v", list, body["nextCursor"])
	}
	first := list[0].(map[string]any)
	if first["id"] != "c1" || first["deviceId"] != "dev-A" || first["cursor"].(float64) != 1 {
		t.Fatalf("first = %v", first)
	}

	// Pagination parameters behave identically.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes?after=1&limit=1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := len(body["changes"].([]any)); got != 1 {
		t.Fatalf("page len = %d", got)
	}
	if body["changes"].([]any)[0].(map[string]any)["id"] != "c2" || body["nextCursor"].(float64) != 2 {
		t.Fatalf("page = %v", body)
	}

	// At/after high-water mark: empty, nextCursor == after.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes?after=3")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 3 {
		t.Fatalf("tail = %d %v", w.Code, body)
	}

	// Unknown document under a live session: empty list, nextCursor 0.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/ghost/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("unknown doc = %d %v", w.Code, body)
	}
}

func TestSessionChangesHTTPSessionGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Seed a known document: the 404 must come from the session gate, not the
	// document lookup.
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-A",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"i": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Never-created session.
	w2, body := doRequest(t, h, http.MethodGet, "/v1/sessions/ghost/documents/doc1/changes")
	if w2.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w2.Code, body)
	}

	// Deleted session.
	if w3 := serveRecorder(h, newRawRequest(http.MethodDelete, "/v1/devices/dev-A/sessions/sess-1")); w3.Code != http.StatusOK {
		t.Fatalf("delete = %d", w3.Code)
	}
	w2, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes")
	if w2.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w2.Code, body)
	}
}

func TestSessionChangesHTTPBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Invalid query params are 400 even for an unknown session: validation
	// precedes the existence gate.
	bad := []string{
		"/v1/sessions/sess-1/documents/doc1/changes?after=-1",
		"/v1/sessions/sess-1/documents/doc1/changes?after=x",
		"/v1/sessions/ghost/documents/doc1/changes?after=1.5",
		"/v1/sessions/sess-1/documents/doc1/changes?limit=0",
		"/v1/sessions/sess-1/documents/doc1/changes?limit=1001",
		"/v1/sessions/ghost/documents/doc1/changes?limit=abc",
		"/v1/sessions/sess-1/documents/doc1/changes?after=1&limit=-5",
	}
	for _, url := range bad {
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusBadRequest || body["error"] == nil {
			t.Fatalf("%s = %d body=%s, want 400 JSON", url, w.Code, w.Body.String())
		}
	}
}

func TestDevicesEmptySegmentsReturnJSON400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": "s1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		method string
		path   string
		body   string
	}{
		// Empty deviceId.
		{http.MethodPost, "/v1/devices//sessions", `{"sessionId":"s1"}`},
		{http.MethodDelete, "/v1/devices//sessions/s1", ""},
		{http.MethodDelete, "/v1/devices//sessions/", ""},
		{http.MethodDelete, "/v1/devices//sessions//", ""},
		// Empty sessionId.
		{http.MethodDelete, "/v1/devices/dev-A/sessions/", ""},
		{http.MethodDelete, "/v1/devices/dev-A/sessions//", ""},
		// GET session changes: empty sessionId or documentId.
		{http.MethodGet, "/v1/sessions//documents/doc1/changes", ""},
		{http.MethodGet, "/v1/sessions//documents//changes", ""},
		{http.MethodGet, "/v1/sessions/s1/documents//changes", ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			var r *http.Request
			if tc.body != "" {
				r = newJSONRequest(t, tc.method, tc.path, "application/json", tc.body)
			} else {
				r = newRawRequest(tc.method, tc.path)
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, w.Code)
			}
			assertJSONError(t, w)
		})
	}
}

func TestDevicesPersistAcrossRestartHTTP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("register = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("session = %d %v", w.Code, body)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h2 := NewHandler(s2)

	// Registration and session are still known: retries are idempotent.
	w, body = postJSON(t, h2, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("device retry = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h2, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("session retry = %d %v", w.Code, body)
	}

	// Ownership survived: another device still gets 409.
	registerDevice(t, h2, "dev-2")
	w, _ = postJSON(t, h2, "/v1/devices/dev-2/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d", w.Code)
	}

	// A live session can still read changes.
	w, _ = postJSON(t, h2, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"i": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, b := doRequest(t, h2, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes")
	if w.Code != http.StatusOK || b["nextCursor"].(float64) != 1 {
		t.Fatalf("changes after restart = %d %v", w.Code, b)
	}

	// Delete persists and flips the session gate after a second restart.
	if w := serveRecorder(h2, newRawRequest(http.MethodDelete, "/v1/devices/dev-1/sessions/sess-1")); w.Code != http.StatusOK {
		t.Fatalf("delete = %d", w.Code)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	h3 := NewHandler(s3)
	w, _ = doRequest(t, h3, http.MethodGet, "/v1/sessions/sess-1/documents/doc1/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted session after restart = %d, want 404", w.Code)
	}
	if w := serveRecorder(h3, newRawRequest(http.MethodDelete, "/v1/devices/dev-1/sessions/sess-1")); w.Code != http.StatusNotFound {
		t.Fatalf("re-delete after restart = %d, want 404", w.Code)
	}
}

func TestDevicesHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-A")
	registerDevice(t, h, "dev-B")

	// Concurrent identical device registration: exactly one creator.
	const n = 30
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators := 0
	statuses := make([]int, 0, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "same"})
			mu.Lock()
			statuses = append(statuses, w.Code)
			if body["created"] == true {
				creators++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for _, code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("register status = %d", code)
		}
	}
	if creators != 1 {
		t.Fatalf("device creators = %d, want 1", creators)
	}

	// Two devices racing on one session id: one 200, one 409.
	results := make(chan int, 2)
	wg.Add(2)
	race := func(device string) {
		defer wg.Done()
		w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": "contested"})
		results <- w.Code
	}
	go race("dev-A")
	go race("dev-B")
	wg.Wait()
	close(results)
	var okCount, conflictCount int
	for code := range results {
		switch code {
		case http.StatusOK:
			okCount++
		case http.StatusConflict:
			conflictCount++
		default:
			t.Fatalf("race status = %d", code)
		}
	}
	if okCount != 1 || conflictCount != 1 {
		t.Fatalf("race results: 200=%d 409=%d, want 1/1", okCount, conflictCount)
	}

	// Concurrent create vs delete on the same id never corrupts state: after
	// settling, a final create always succeeds.
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("flip-%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": id})
		}()
		go func() {
			defer wg.Done()
			_ = serveRecorder(h, newRawRequest(http.MethodDelete, "/v1/devices/dev-A/sessions/"+id))
		}()
		wg.Wait()
		w, _ := postJSON(t, h, "/v1/devices/dev-A/sessions", map[string]any{"sessionId": id})
		if w.Code != http.StatusOK {
			t.Fatalf("settle create %s = %d", id, w.Code)
		}
	}
}

// TestExistingEndpointsUnchanged sanity-checks that the registration layer
// does not alter the legacy document routes, including the empty-ID 400 guard.
func TestExistingEndpointsUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK || body["nextCursor"].(float64) != 1 {
		t.Fatalf("legacy list = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents//changes")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("legacy empty segment = %d", w.Code)
	}
	// Unrelated paths keep the framework's normal answer.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/nope")
	if w.Code != http.StatusNotFound || strings.Contains(w.Header().Get("Content-Type"), "json") {
		t.Fatalf("unknown route = %d ct=%s", w.Code, w.Header().Get("Content-Type"))
	}
}
