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

func newJSONRequest(method, url, body, contentType string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, url, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, url, nil)
	}
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
		t.Fatalf("content type = %q, want application/json", ct)
	}
	var b map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
		t.Fatalf("body = %q, want a JSON error", w.Body.String())
	}
}

func registerDevice(t *testing.T, h http.Handler, deviceID string) {
	t.Helper()
	w, body := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": deviceID})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("register %q = %d %v body=%s", deviceID, w.Code, body, w.Body.String())
	}
}

func TestNewFamiliesAlwaysJSON(t *testing.T) {
	h, _ := newTestHandler(t)

	// Every failure on the new surface answers a JSON error — no HTML
	// redirects or plain-text 404s — including unmatched paths and methods.
	cases := []struct {
		method string
		path   string
		want   int
	}{
		{http.MethodGet, "/v1/devices", http.StatusNotFound},
		{http.MethodPut, "/v1/devices", http.StatusNotFound},
		{http.MethodGet, "/v1/sessions", http.StatusNotFound},
		{http.MethodGet, "/v1/devices/dev1/sessions", http.StatusNotFound},
		// A non-DELETE verb on the device item path is a method mismatch on
		// the deregistration endpoint: a JSON 400, not the subtree 404.
		{http.MethodPost, "/v1/devices/dev1", http.StatusBadRequest},
		{http.MethodDelete, "/v1/devices/dev1/sessions/s1/extra", http.StatusNotFound},
		{http.MethodPost, "/v1/sessions/s1/documents/d/changes", http.StatusNotFound},
	}
	for _, tc := range cases {
		r := newJSONRequest(tc.method, tc.path, `{"deviceId":"x"}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != tc.want {
			t.Fatalf("%s %s = %d, want %d", tc.method, tc.path, w.Code, tc.want)
		}
		assertJSONError(t, w)
	}

	// The pre-existing document surface keeps its old plain 404 for a trailing
	// slash; only the empty documentID segment was ever the JSON-400 case.
	w := serveRecorder(h, newJSONRequest(http.MethodGet, "/v1/documents/doc/changes/", "", ""))
	if w.Code != http.StatusNotFound || w.Header().Get("Content-Type") == "application/json" {
		t.Fatalf("legacy trailing slash changed: %d %q", w.Code, w.Header().Get("Content-Type"))
	}
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

	// Retry: created=false.
	w, body = postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || body["created"] != false || body["deviceId"] != "dev-1" {
		t.Fatalf("retry = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

func TestRegisterDeviceHTTPRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"d"}`},
		{"missing content type", "", `{"deviceId":"d"}`},
		{"json suffix content type", "application/vnd.api+json", `{"deviceId":"d"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"d"}x`},
		{"empty deviceId", "application/json", `{"deviceId":""}`},
		{"missing deviceId", "application/json", `{}`},
		{"numeric deviceId", "application/json", `{"deviceId":7}`},
		{"boolean deviceId", "application/json", `{"deviceId":true}`},
		{"null deviceId", "application/json", `{"deviceId":null}`},
		{"array body", "application/json", `[1,2]`},
		{"string body", "application/json", `"d"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, "/v1/devices", tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: no device registered, so a session create must 404.
	w, body := postJSON(t, h, "/v1/devices/d/sessions", map[string]any{"sessionId": "s"})
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("session under unregistered device = %d %v", w.Code, body)
	}
}

func TestCreateSessionHTTPSuccessAndRetry(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, body := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["sessionId"] != "sess-1" || body["created"] != true {
		t.Fatalf("body = %v", body)
	}

	// Same device/session retry -> created=false.
	w, body = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess-1"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("retry = %d %v body=%s", w.Code, body, w.Body.String())
	}
}

func TestCreateSessionHTTPUnknownDevice404(t *testing.T) {
	h, s := newTestHandler(t)

	w, body := postJSON(t, h, "/v1/devices/ghost/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("body = %v", body)
	}

	// Zero writes: the session must not exist even under a subsequently
	// registered device of the same id.
	registerDevice(t, h, "ghost")
	w, body = postJSON(t, h, "/v1/devices/ghost/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("create after late register = %d %v body=%s", w.Code, body, w.Body.String())
	}
	exists, err := s.SessionExists("sess")
	if err != nil || !exists {
		t.Fatalf("session exists=%v err=%v", exists, err)
	}
}

func TestCreateSessionHTTPCrossOwner409ZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Other device claims the same session id -> 409.
	w, body := postJSON(t, h, "/v1/devices/dev-2/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("body = %v", body)
	}

	// Zero write: owner's repeat is still idempotent (created=false), proving
	// ownership never moved and no duplicate row appeared.
	w, body = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("owner repeat = %d %v", w.Code, body)
	}
	exists, _ := s.SessionExists("sess")
	if !exists {
		t.Fatal("session vanished after conflict")
	}
}

func TestCreateSessionHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	const url = "/v1/devices/dev-1/sessions"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"sessionId":"s"}`},
		{"missing content type", "", `{"sessionId":"s"}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"sessionId":"s"}garbage`},
		{"empty sessionId", "application/json", `{"sessionId":""}`},
		{"missing sessionId", "application/json", `{}`},
		{"numeric sessionId", "application/json", `{"sessionId":5}`},
		{"boolean sessionId", "application/json", `{"sessionId":false}`},
		{"null sessionId", "application/json", `{"sessionId":null}`},
		{"array body", "application/json", `[1]`},
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

	// Zero writes: none of the rejected ids landed.
	if exists, _ := s.SessionExists("s"); exists {
		t.Fatal("zero-write violated: session s created by a rejected request")
	}
}

func TestDeleteSessionHTTPOwnerScoped(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Non-owner delete -> 404, session still there.
	w, body := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-2/sessions/sess")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("other-device delete = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/any/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("session should still exist: %d %s", w.Code, w.Body.String())
	}

	// Unknown session under a known device -> 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/ghost")
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session status = %d", w.Code)
	}

	// Owner delete -> 200 {"deleted":true}.
	w, body = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("owner delete = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Repeat delete and non-owner delete after removal -> 404.
	for _, url := range []string{
		"/v1/devices/dev-1/sessions/sess",
		"/v1/devices/dev-2/sessions/sess",
	} {
		w, body = doRequest(t, h, http.MethodDelete, url)
		if w.Code != http.StatusNotFound || body["error"] == nil {
			t.Fatalf("%s after delete = %d %v", url, w.Code, body)
		}
	}

	// Session-scoped reads now 404.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/any/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d, want 404", w.Code)
	}

	// Unknown device path -> 404 too (nothing to match).
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/ghost/sessions/sess")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device delete = %d, want 404", w.Code)
	}
}

func TestSessionChangesHTTPMirrorsDocumentChanges(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Seed a document through the existing endpoint.
	for i := 0; i < 3; i++ {
		w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c%d", i), "payload": map[string]any{"i": i}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	// Full list: same shape and values as the document endpoint.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	list := body["changes"].([]any)
	if len(list) != 3 || body["nextCursor"].(float64) != 3 {
		t.Fatalf("list = %v next=%v", list, body["nextCursor"])
	}
	first := list[0].(map[string]any)
	if first["id"] != "c0" || first["deviceId"] != "dev-1" || first["cursor"].(float64) != 1 {
		t.Fatalf("first = %v", first)
	}

	// Pagination carries over identically.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes?after=1&limit=1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	list = body["changes"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["id"] != "c1" || body["nextCursor"].(float64) != 2 {
		t.Fatalf("paged = %v next=%v", list, body["nextCursor"])
	}

	// Unknown document through a live session: empty list, nextCursor 0 — the
	// existing semantics, not a 404.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/ghost/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("unknown doc = %d %v", w.Code, body)
	}
}

func TestSessionChangesHTTPSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Never-created session -> 404 even against a document that has changes.
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/ghost/documents/doc1/changes")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}

	// Delete the session -> reads now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}
}

func TestSessionChangesHTTPBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, q := range []string{
		"after=-1", "after=x", "after=1.5", "limit=0", "limit=1001", "limit=abc",
	} {
		url := "/v1/sessions/sess/documents/doc/changes?" + q
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusBadRequest || body["error"] == nil {
			t.Fatalf("%s = %d %v", q, w.Code, body)
		}
	}

	// Illegal params 400 even when the session is also unknown: shape errors
	// take precedence over the 404.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/ghost/documents/doc/changes?limit=0")
	if w.Code != http.StatusBadRequest || body["error"] == nil {
		t.Fatalf("bad param + missing session = %d %v", w.Code, body)
	}
}

func TestDeviceSessionEmptyIDsReturnJSON400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	paths := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/devices//sessions"},
		{http.MethodDelete, "/v1/devices//sessions/sess"},
		{http.MethodDelete, "/v1/devices/dev/sessions/"},
		{http.MethodDelete, "/v1/devices/dev/sessions//"},
		{http.MethodGet, "/v1/sessions//documents/doc/changes"},
		{http.MethodGet, "/v1/sessions/sess/documents//changes"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc/changes/"},
	}
	for _, tc := range paths {
		r := newJSONRequest(tc.method, tc.path, `{"deviceId":"d"}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
	}
}

func TestDeviceSessionHTTPRestartPersists(t *testing.T) {
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
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Replay is idempotent and the session-scoped read still works.
	w, body := postJSON(t, h2, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("session replay = %d %v", w.Code, body)
	}
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("session read after restart = %d %s", w.Code, w.Body.String())
	}

	// Delete persists across the next restart.
	w, _ = doRequest(t, h2, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	h3 := NewHandler(s3)
	w, _ = doRequest(t, h3, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted session after restart = %d, want 404", w.Code)
	}
	w, body = postJSON(t, h3, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("recreate after restart = %d %v", w.Code, body)
	}
}

func TestConcurrentSessionLifecycleHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	// Concurrent identical creates: exactly one creator.
	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators := 0
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "hot"})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("status %d: %s", w.Code, w.Body.String())
				return
			}
			if body["created"] == true {
				mu.Lock()
				creators++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if creators != 1 {
		t.Fatalf("creators = %d, want 1", creators)
	}

	// Concurrent delete vs. create: in the end exactly one of {live, absent}
	// holds consistently, and the next state is coherent (repeat delete 404s
	// iff the delete won; create reports created=true iff it ran after removal).
	var wg2 sync.WaitGroup
	statuses := make(chan int, 2)
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/hot")
		statuses <- w.Code
	}()
	go func() {
		defer wg2.Done()
		w, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "hot"})
		statuses <- w.Code
	}()
	wg2.Wait()
	close(statuses)
	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("racing delete/create status = %d", code)
		}
	}

	// The race settles into exactly one coherent state (live or absent). A
	// final owner delete then forces "absent": 200 if it was live, 404 if the
	// racing delete already won. Afterwards both the read and a repeat delete
	// must observe the same absent state — no half-deleted or duplicated row.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/hot")
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Fatalf("settling delete = %d", w.Code)
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/hot/documents/d/changes")
	if w.Code != http.StatusNotFound {
		t.Fatalf("session should be absent after settle, GET = %d", w.Code)
	}
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/hot")
	if w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete after settle = %d, want 404", w.Code)
	}
}
