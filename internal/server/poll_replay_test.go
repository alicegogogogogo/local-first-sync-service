package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func pollPage(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodGet, url)
}

func TestPollReturnsExistingPage(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc", 3)

	w, body := pollPage(t, h, "/v1/documents/doc/changes/poll?after=1&limit=2&waitMs=1000")
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 || body["nextCursor"].(float64) != 3 || body["timedOut"] != false {
		t.Fatalf("page = %v", body)
	}
}

func TestPollUnknownDocumentImmediateEmpty(t *testing.T) {
	h, _ := newTestHandler(t)

	done := make(chan struct {
		code int
		body map[string]any
	}, 1)
	go func() {
		w, body := pollPage(t, h, "/v1/documents/ghost/changes/poll?waitMs=30000")
		done <- struct {
			code int
			body map[string]any
		}{w.Code, body}
	}()
	select {
	case r := <-done:
		if r.code != 200 {
			t.Fatalf("status = %d", r.code)
		}
		if len(r.body["changes"].([]any)) != 0 || r.body["nextCursor"].(float64) != 0 || r.body["timedOut"] != false {
			t.Fatalf("unknown doc page = %v", r.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown document parked the poll")
	}
}

func TestPollTimeoutEchoesCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc", 2)

	start := time.Now()
	w, body := pollPage(t, h, "/v1/documents/doc/changes/poll?after=2&waitMs=80")
	elapsed := time.Since(start)
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if elapsed < 60*time.Millisecond {
		t.Fatalf("poll returned after %v, wait did not elapse", elapsed)
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 2 || body["timedOut"] != true {
		t.Fatalf("timeout page = %v", body)
	}
}

func TestPollRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc", 1)
	bad := []string{
		"/v1/documents/doc/changes/poll?waitMs=-1",
		"/v1/documents/doc/changes/poll?waitMs=30001",
		"/v1/documents/doc/changes/poll?waitMs=abc",
		"/v1/documents/doc/changes/poll?waitMs=1.5",
		"/v1/documents/doc/changes/poll?after=-1&waitMs=10",
		"/v1/documents/doc/changes/poll?after=x",
		"/v1/documents/doc/changes/poll?limit=0",
		"/v1/documents/doc/changes/poll?limit=1001",
	}
	for _, url := range bad {
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", url, w.Code)
		}
		if body["error"] == nil {
			t.Fatalf("%s body = %s", url, w.Body.String())
		}
	}
}

// TestPollWakesOnCommit exercises a real listener: one client parks with a
// long wait, another request commits the first new change, and the parked
// client receives the page promptly with timedOut=false.
func TestPollWakesOnCommit(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Seed one change so the document is known and the poll parks after it.
	seedDoc(t, h, "doc", 1)

	type resp struct {
		status int
		body   map[string]any
		err    error
	}
	got := make(chan resp, 1)
	go func() {
		r, err := http.Get(srv.URL + "/v1/documents/doc/changes/poll?after=1&waitMs=10000")
		if err != nil {
			got <- resp{err: err}
			return
		}
		defer r.Body.Close()
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		got <- resp{status: r.StatusCode, body: body}
	}()

	// Wait until the poll is parked, then commit from a second client.
	time.Sleep(100 * time.Millisecond)
	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"n": 2}}},
	})

	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("poll request: %v", r.err)
		}
		if r.status != 200 {
			t.Fatalf("status = %d body = %v", r.status, r.body)
		}
		rows := r.body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c2" {
			t.Fatalf("woken rows = %v", rows)
		}
		if r.body["nextCursor"].(float64) != 2 || r.body["timedOut"] != false {
			t.Fatalf("woken page = %v", r.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked poll was not woken by the commit")
	}
}

// TestPollClientDisconnectWritesNothing verifies a disconnect ends the wait
// and a later poll sees the same cursor: waiting never persists anything.
func TestPollClientDisconnectWritesNothing(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	seedDoc(t, h, "doc", 1)

	client := &http.Client{Timeout: 100 * time.Millisecond}
	_, err := client.Get(srv.URL + "/v1/documents/doc/changes/poll?after=1&waitMs=30000")
	if err == nil {
		t.Fatal("expected the short client timeout to abort the request")
	}

	// Give the server a moment to observe the disconnect, then confirm no
	// state changed.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows, next, lerr := s.ListChanges("doc", 0, 100)
		if lerr != nil {
			t.Fatal(lerr)
		}
		if len(rows) == 1 && next == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("canceled wait appears to have written state")
}

func TestPollAndReplayMethodAndPathShape(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc", 1)

	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"POST on poll", http.MethodPost, "/v1/documents/doc/changes/poll", ""},
		{"PUT on poll", http.MethodPut, "/v1/documents/doc/changes/poll", ""},
		{"GET on replay", http.MethodGet, "/v1/documents/doc/replay", ""},
		{"DELETE on replay", http.MethodDelete, "/v1/documents/doc/replay", ""},
		{"empty documentID on poll", http.MethodGet, "/v1/documents//changes/poll?waitMs=1", ""},
		{"empty documentID on replay", http.MethodPost, "/v1/documents//replay", `{"deviceId":"d","operations":[{"id":"a","payload":1}]}`},
		{"poll trailing slash", http.MethodGet, "/v1/documents/doc/changes/poll/", ""},
		{"replay trailing slash", http.MethodPost, "/v1/documents/doc/replay/", `{"deviceId":"d","operations":[{"id":"a","payload":1}]}`},
		{"poll extra segment", http.MethodGet, "/v1/documents/doc/changes/poll/extra", ""},
		{"poll empty middle segment", http.MethodGet, "/v1/documents/doc//poll?waitMs=1", ""},
		{"poll empty changes segment", http.MethodGet, "/v1/documents/doc/changes//poll", ""},
		{"replay extra segment", http.MethodPost, "/v1/documents/doc/replay/extra", `{"deviceId":"d","operations":[{"id":"a","payload":1}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.body != "" {
				r = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				r.Header.Set("Content-Type", "application/json")
			} else {
				r = httptest.NewRequest(tc.method, tc.path, nil)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			var b map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
				t.Fatalf("body = %q, want JSON error", w.Body.String())
			}
		})
	}
}

func TestDocumentNamedPollOrReplayKeepsLegacyRoutes(t *testing.T) {
	h, _ := newTestHandler(t)

	// A document literally named "poll": its ordinary changes route must not
	// be mistaken for the new endpoint by the malformed-path guard.
	w, _ := postJSON(t, h, "/v1/documents/poll/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != 200 {
		t.Fatalf("post to doc named poll = %d %s", w.Code, w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/poll/changes")
	if w.Code != 200 || body["nextCursor"].(float64) != 1 {
		t.Fatalf("list doc named poll = %d %v", w.Code, body)
	}

	// A document literally named "replay": merge still works.
	w, _ = postJSON(t, h, "/v1/documents/replay/merge", map[string]any{
		"deviceId":   "dev",
		"baseCursor": 0,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"a": 1}},
	})
	if w.Code != 200 {
		t.Fatalf("merge on doc named replay = %d %s", w.Code, w.Body.String())
	}
}

func TestReplaySuccessSharesCursors(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	// An ordinary commit at cursor 1.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "p1", "payload": map[string]any{"n": 0}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	w, body := postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev",
		"operations": []any{
			map[string]any{"id": "o1", "payload": map[string]any{"n": 1}},
			map[string]any{"id": "o2", "payload": []any{1, 2}},
		},
	})
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("results = %v", results)
	}
	r0 := results[0].(map[string]any)
	if r0["id"] != "o1" || r0["created"] != true || r0["cursor"].(float64) != 2 {
		t.Fatalf("r0 = %v", r0)
	}
	r1 := results[1].(map[string]any)
	if r1["id"] != "o2" || r1["created"] != true || r1["cursor"].(float64) != 3 {
		t.Fatalf("r1 = %v", r1)
	}

	// Idempotent replay: same decoded payloads, original cursors, created=false.
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev",
		"operations": []any{
			map[string]any{"id": "o1", "payload": map[string]any{"n": 1.0}},
			map[string]any{"id": "o2", "payload": []any{1, 2}},
		},
	})
	if w.Code != 200 {
		t.Fatalf("idempotent status = %d body = %s", w.Code, w.Body.String())
	}
	results = body["results"].([]any)
	if results[0].(map[string]any)["created"] != false || results[0].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("idempotent r0 = %v", results[0])
	}
	if results[1].(map[string]any)["created"] != false || results[1].(map[string]any)["cursor"].(float64) != 3 {
		t.Fatalf("idempotent r1 = %v", results[1])
	}
}

func TestReplayRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []store.Change{
		{ID: "existing", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	const url = "/v1/documents/doc/replay"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev","operations":[{"id":"a","payload":1}]}`},
		{"missing content type", "", `{"deviceId":"dev","operations":[{"id":"a","payload":1}]}`},
		{"json suffix type", "application/vnd.api+json", `{"deviceId":"dev","operations":[{"id":"a","payload":1}]}`},
		{"malformed json", "application/json", `{"deviceId":"dev","operations":[`},
		{"trailing content", "application/json", `{"deviceId":"dev","operations":[{"id":"a","payload":1}]}x`},
		{"missing deviceId", "application/json", `{"operations":[{"id":"a","payload":1}]}`},
		{"empty deviceId", "application/json", `{"deviceId":"","operations":[{"id":"a","payload":1}]}`},
		{"missing operations", "application/json", `{"deviceId":"dev"}`},
		{"empty operations", "application/json", `{"deviceId":"dev","operations":[]}`},
		{"missing op id", "application/json", `{"deviceId":"dev","operations":[{"payload":1}]}`},
		{"empty op id", "application/json", `{"deviceId":"dev","operations":[{"id":"","payload":1}]}`},
		{"numeric op id", "application/json", `{"deviceId":"dev","operations":[{"id":1,"payload":1}]}`},
		{"missing payload", "application/json", `{"deviceId":"dev","operations":[{"id":"a"}]}`},
		{"duplicate op ids", "application/json", `{"deviceId":"dev","operations":[{"id":"a","payload":1},{"id":"a","payload":2}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, url, strings.NewReader(tc.body))
			if tc.contentType != "" {
				r.Header.Set("Content-Type", tc.contentType)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			var b map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
				t.Fatalf("body = %q", w.Body.String())
			}
		})
	}

	// Every rejected request left the seeded change untouched.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || rows[0].ID != "existing" || next != 1 {
		t.Fatalf("zero-write violated: %+v next=%d", rows, next)
	}
}

func TestReplayConflictReportsIDAndZeroWrites(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"v": 1}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	w, body := postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId": "dev",
		"operations": []any{
			map[string]any{"id": "new", "payload": map[string]any{"v": 2}},
			map[string]any{"id": "x", "payload": map[string]any{"v": 99}},
		},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409, body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil || body["conflictId"] != "x" {
		t.Fatalf("conflict body = %v", body)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("conflict leaked writes: %+v next=%d", rows, next)
	}
}

func TestReplayDeviceStates(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	seedDoc(t, h, "doc", 1)

	// Unregistered device: 404 with no change content in the error.
	w, body := postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "stranger",
		"operations": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil || body["results"] != nil {
		t.Fatalf("404 body = %v", body)
	}

	// Revoke permission: 403 and again no change content.
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "o1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil || body["results"] != nil {
		t.Fatalf("403 body = %v", body)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("revoked replay leaked writes: %+v", rows)
	}

	// Grant again and the replay commits with the next cursor.
	if _, err := s.SetDocumentPermission("doc", "dev", true); err != nil {
		t.Fatal(err)
	}
	w, body = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "o1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != 200 {
		t.Fatalf("granted status = %d body = %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("post-grant result = %v", body["results"])
	}
}

func TestReplayConcurrentWithPosts(t *testing.T) {
	h, _ := newTestHandler(t)
	// Devices used by every goroutine must be registered first.
	register := func(id string) {
		w, _ := postJSON(t, h, "/v1/devices", map[string]string{"deviceId": id})
		if w.Code != 200 {
			t.Fatalf("register %s: %d %s", id, w.Code, w.Body.String())
		}
	}
	register("dev")
	register("dev2")

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
				"deviceId": "dev",
				"changes":  []any{map[string]any{"id": fmt.Sprintf("post-%02d", i), "payload": map[string]any{"i": i}}},
			})
			if w.Code != 200 {
				errs <- fmt.Errorf("post %d: %d %s", i, w.Code, w.Body.String())
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			w, _ := postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
				"deviceId":   "dev2",
				"operations": []any{map[string]any{"id": fmt.Sprintf("replay-%02d", i), "payload": map[string]any{"i": i}}},
			})
			if w.Code != 200 {
				errs <- fmt.Errorf("replay %d: %d %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if got := len(body["changes"].([]any)); got != 2*n {
		t.Fatalf("stored %d changes, want %d", got, 2*n)
	}
	if body["nextCursor"].(float64) != float64(2*n) {
		t.Fatalf("nextCursor = %v, want %d", body["nextCursor"], 2*n)
	}
}

func TestReplayPersistsAcrossRestartHTTP(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)

	w, _ := postJSON(t, h, "/v1/devices", map[string]string{"deviceId": "dev"})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "o1", "payload": map[string]any{"v": 1}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
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

	// Identical replay after restart is still idempotent at cursor 1.
	w, body := postJSON(t, h2, "/v1/documents/doc/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "o1", "payload": map[string]any{"v": 1.0}}},
	})
	if w.Code != 200 {
		t.Fatalf("idempotent after restart = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("post-restart result = %v", r)
	}

	// A poll parked on the restarted store times out at the same cursor.
	w, body = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes/poll?after=1&waitMs=20")
	if w.Code != 200 || body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
		t.Fatalf("poll after restart = %d %v", w.Code, body)
	}
}
