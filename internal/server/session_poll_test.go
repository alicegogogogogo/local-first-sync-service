package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionPollPath is the session-scoped long-poll path.
func sessionPollPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/changes/poll"
}

// pollOverHTTP runs one GET against a real test server and reports its
// status and decoded body. A real server (rather than httptest.NewRecorder)
// is required so parked requests observe client cancellation and server
// shutdown through r.Context().
func pollOverHTTP(t *testing.T, srv *httptest.Server, rawURL string) <-chan struct {
	status int
	body   map[string]any
	err    error
} {
	t.Helper()
	out := make(chan struct {
		status int
		body   map[string]any
		err    error
	}, 1)
	go func() {
		req, err := http.NewRequest(http.MethodGet, srv.URL+rawURL, nil)
		if err != nil {
			out <- struct {
				status int
				body   map[string]any
				err    error
			}{err: err}
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- struct {
				status int
				body   map[string]any
				err    error
			}{err: err}
			return
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		out <- struct {
			status int
			body   map[string]any
			err    error
		}{status: resp.StatusCode, body: body}
	}()
	return out
}

// Rows already past after return immediately with timedOut=false — the wait is
// never entered — and the page keeps the ascending cursor order.
func TestSessionPollReturnsExistingPage(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)

	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&limit=2&waitMs=1000")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %v", rows)
	}
	if rows[0].(map[string]any)["cursor"].(float64) != 2 ||
		rows[1].(map[string]any)["cursor"].(float64) != 3 {
		t.Fatalf("rows not ascending by cursor: %v", rows)
	}
	if body["nextCursor"].(float64) != 3 || body["timedOut"] != false {
		t.Fatalf("page = %v", body)
	}

	// Top-level keys arrive in the fixed changes,nextCursor,timedOut order on
	// the wire.
	wire := w.Body.String()
	iChanges := strings.Index(wire, `"changes"`)
	iNext := strings.Index(wire, `"nextCursor"`)
	iTimed := strings.Index(wire, `"timedOut"`)
	if !(iChanges >= 0 && iChanges < iNext && iNext < iTimed) {
		t.Fatalf("key order = %d,%d,%d in %q", iChanges, iNext, iTimed, wire)
	}
}

// An unknown document answers at once: empty list, nextCursor 0, timedOut
// false — even with a long wait, it must never park.
func TestSessionPollUnknownDocumentImmediateEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	srv := httptest.NewServer(h)
	defer srv.Close()

	ch := pollOverHTTP(t, srv, sessionPollPath("sess", "ghost")+"?waitMs=30000")
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("poll request: %v", r.err)
		}
		if r.status != http.StatusOK {
			t.Fatalf("status = %d", r.status)
		}
		if len(r.body["changes"].([]any)) != 0 ||
			r.body["nextCursor"].(float64) != 0 ||
			r.body["timedOut"] != false {
			t.Fatalf("unknown doc page = %v", r.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown document parked the session poll")
	}
}

// A caught-up known document parks; when the deadline lapses it answers the
// empty page, echoes the caller's after and sets timedOut=true without
// advancing the cursor.
func TestSessionPollTimeoutEchoesCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	start := time.Now()
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=2&waitMs=80")
	elapsed := time.Since(start)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if elapsed < 60*time.Millisecond {
		t.Fatalf("poll returned after %v, wait did not elapse", elapsed)
	}
	if len(body["changes"].([]any)) != 0 ||
		body["nextCursor"].(float64) != 2 ||
		body["timedOut"] != true {
		t.Fatalf("timeout page = %v", body)
	}
}

// Defaults: with no parameters after=0, limit=100, waitMs=0. Existing rows
// return immediately; a caught-up document answers timedOut=true at once
// (zero wait is an expired deadline).
func TestSessionPollDefaultParams(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc"))
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 || body["timedOut"] != false {
		t.Fatalf("default with rows = %d %v", w.Code, body)
	}

	w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1")
	if w.Code != http.StatusOK {
		t.Fatalf("caught-up default status = %d", w.Code)
	}
	if len(body["changes"].([]any)) != 0 ||
		body["nextCursor"].(float64) != 1 ||
		body["timedOut"] != true {
		t.Fatalf("zero-wait caught-up page = %v", body)
	}

	// Waiting produces no change and consumes no cursor.
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("wait moved the log: rows=%v next=%d", rows, next)
	}
}

// Bad after/limit/waitMs values are a 400 JSON error; parameter validation
// precedes the session existence check.
func TestSessionPollRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	bad := []string{
		"waitMs=-1",
		"waitMs=30001",
		"waitMs=abc",
		"waitMs=1.5",
		"waitMs=1e3",
		"after=-1",
		"after=x",
		"after=1.5",
		"limit=0",
		"limit=1001",
		"limit=abc",
	}
	for _, q := range bad {
		w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?"+q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s status = %d, want 400", q, w.Code)
		}
		if body["error"] == nil {
			t.Fatalf("?%s body = %s", q, w.Body.String())
		}
		// A shape failure never returns page content.
		if body["changes"] != nil || body["timedOut"] != nil {
			t.Fatalf("?%s leaked page content: %s", q, w.Body.String())
		}

		// The same bad parameter against a never-created session is still a
		// 400, not a 404.
		w, body = doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?"+q)
		if w.Code != http.StatusBadRequest || body["error"] == nil {
			t.Fatalf("?%s against unknown session = %d %v, want 400", q, w.Code, body)
		}
	}
}

// A missing or deleted session is a 404 JSON error with no change content.
func TestSessionPollSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?waitMs=10")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["changes"] != nil || body["nextCursor"] != nil || body["timedOut"] != nil {
		t.Fatalf("404 leaked page content: %s", w.Body.String())
	}

	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?waitMs=10")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}
}

// A session whose owning device lost permission for the document gets a 403
// JSON error immediately — it never parks and no page content is returned.
// Session existence is checked before permission.
func TestSessionPollPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Even with a long wait the verdict is immediate: the permission check
	// runs before any wait registration.
	start := time.Now()
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=30000")
	if time.Since(start) > time.Second {
		t.Fatal("revoked session poll parked")
	}
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["changes"] != nil || body["nextCursor"] != nil || body["timedOut"] != nil {
		t.Fatalf("403 leaked page content: %s", w.Body.String())
	}

	// Unknown session against the revoked document is a 404, not a 403.
	w, _ = doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?waitMs=10")
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Zero writes.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("403 moved the log: rows=%v next=%d", rows, next)
	}

	// Re-grant restores the wait entry; the document is caught up so the zero
	// default wait times out at the same cursor.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=20")
	if w.Code != http.StatusOK || body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
		t.Fatalf("re-granted poll = %d %v", w.Code, body)
	}
}

// Every documented write path wakes a parked session poll right after its
// transaction commits.
func TestSessionPollWakesOnEveryWritePath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	registerDevice(t, h, "dev-2")
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Common seed: cursor 1 from dev, poll parks after it.
	seedOne := func(doc string) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
			"deviceId": "dev",
			"changes":  []any{map[string]any{"id": "seed", "payload": map[string]any{"n": 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	park := func(doc string) <-chan struct {
		status int
		body   map[string]any
		err    error
	} {
		return pollOverHTTP(t, srv, sessionPollPath("sess", doc)+"?after=1&waitMs=10000")
	}
	awaitWake := func(ch <-chan struct {
		status int
		body   map[string]any
		err    error
	}, wantID string) {
		t.Helper()
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("poll request: %v", r.err)
			}
			if r.status != http.StatusOK {
				t.Fatalf("status = %d body = %v", r.status, r.body)
			}
			rows := r.body["changes"].([]any)
			if len(rows) != 1 || rows[0].(map[string]any)["id"] != wantID {
				t.Fatalf("woken rows = %v, want id %s", rows, wantID)
			}
			if r.body["nextCursor"].(float64) != 2 || r.body["timedOut"] != false {
				t.Fatalf("woken page = %v", r.body)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("parked poll was not woken by %s", wantID)
		}
	}

	// 1. Session batch commit.
	seedOne("doc-sess")
	ch := park("doc-sess")
	time.Sleep(80 * time.Millisecond)
	w, _ := postSessionChanges(t, h, "sess", "doc-sess", `{"changes":[{"id":"via-session","payload":{"n":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	awaitWake(ch, "via-session")

	// 2. Document-level commit (from a different registered device).
	seedOne("doc-post")
	ch = park("doc-post")
	time.Sleep(80 * time.Millisecond)
	w, _ = postJSON(t, h, "/v1/documents/doc-post/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "via-post", "payload": map[string]any{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	awaitWake(ch, "via-post")

	// 3. Merge with baseCursor equal to the current cursor.
	seedOne("doc-merge")
	ch = park("doc-merge")
	time.Sleep(80 * time.Millisecond)
	w, _ = postJSON(t, h, "/v1/documents/doc-merge/merge", map[string]any{
		"deviceId":   "dev",
		"baseCursor": 1,
		"change":     map[string]any{"id": "via-merge", "payload": map[string]any{"a": 2}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge = %d %s", w.Code, w.Body.String())
	}
	awaitWake(ch, "via-merge")

	// 4. Restore from a snapshot taken at the seed cursor.
	seedOne("doc-restore")
	w, _ = postJSON(t, h, "/v1/documents/doc-restore/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"n": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	ch = park("doc-restore")
	time.Sleep(80 * time.Millisecond)
	w, _ = postJSON(t, h, "/v1/documents/doc-restore/restore", map[string]any{
		"deviceId":       "dev",
		"changeId":       "via-restore",
		"snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}
	awaitWake(ch, "via-restore")

	// 5. Offline replay from the registered, still-authorized device.
	seedOne("doc-replay")
	ch = park("doc-replay")
	time.Sleep(80 * time.Millisecond)
	w, _ = postJSON(t, h, "/v1/documents/doc-replay/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "via-replay", "payload": map[string]any{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("replay = %d %s", w.Code, w.Body.String())
	}
	awaitWake(ch, "via-replay")
}

// Non-GET verbs and malformed path shapes are a JSON 400: no redirect, no
// HTML, no wait and no write.
func TestSessionPollMethodAndPathShape(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"POST on poll", http.MethodPost, sessionPollPath("sess", "doc")},
		{"PUT on poll", http.MethodPut, sessionPollPath("sess", "doc")},
		{"DELETE on poll", http.MethodDelete, sessionPollPath("sess", "doc")},
		{"PATCH on poll", http.MethodPatch, sessionPollPath("sess", "doc")},
		{"OPTIONS on poll", http.MethodOptions, sessionPollPath("sess", "doc")},
		{"trailing slash", http.MethodGet, sessionPollPath("sess", "doc") + "/"},
		{"extra segment", http.MethodGet, sessionPollPath("sess", "doc") + "/extra"},
		{"extra two segments", http.MethodGet, sessionPollPath("sess", "doc") + "/extra/more"},
		{"empty session id", http.MethodGet, "/v1/sessions//documents/doc/changes/poll?waitMs=1"},
		{"empty document id", http.MethodGet, "/v1/sessions/sess/documents//changes/poll?waitMs=1"},
		{"empty changes segment", http.MethodGet, "/v1/sessions/sess/documents/doc//poll?waitMs=1"},
		{"missing changes segment", http.MethodGet, "/v1/sessions/sess/documents/doc/poll?waitMs=1"},
		{"missing documents segment", http.MethodGet, "/v1/sessions/sess/doc/changes/poll?waitMs=1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("redirected to %q", loc)
			}
			if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
				t.Fatalf("leaked HTML: %q", w.Body.String())
			}
		})
	}

	// Zero writes.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("malformed requests moved the log: rows=%v next=%d", rows, next)
	}
}

// A session or document literally named "poll" keeps its ordinary routes; the
// "poll" keyword is recognized only in the endpoint's terminal segment.
func TestSessionPollIdentifierNamedPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "poll")

	// Document named "poll": its paged collection read and its poll endpoint
	// both work.
	w, _ := postSessionChanges(t, h, "poll", "poll", `{"changes":[{"id":"k1","payload":{"k":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/poll/documents/poll/changes")
	if w.Code != http.StatusOK || body["nextCursor"].(float64) != 1 {
		t.Fatalf("paged read for ids named poll = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/poll/documents/poll/changes/poll?after=1&waitMs=20")
	if w.Code != http.StatusOK || body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
		t.Fatalf("poll endpoint for ids named poll = %d %v", w.Code, body)
	}
}

// A client disconnect ends the wait immediately and persists nothing.
func TestSessionPollClientDisconnectWritesNothing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	srv := httptest.NewServer(h)
	defer srv.Close()

	client := &http.Client{Timeout: 100 * time.Millisecond}
	if _, err := client.Get(srv.URL + sessionPollPath("sess", "doc") + "?after=1&waitMs=30000"); err == nil {
		t.Fatal("expected the short client timeout to abort the request")
	}

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

// When the service begins shutting down, a parked session poll answers a JSON
// 503 rather than waiting out its deadline.
func TestSessionPollServiceClosing503(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	srv := httptest.NewServer(h)
	defer srv.Close()

	ch := pollOverHTTP(t, srv, sessionPollPath("sess", "doc")+"?after=1&waitMs=30000")
	time.Sleep(100 * time.Millisecond)
	s.InterruptWaits()

	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("poll connection error = %v, want 503 response", r.err)
		}
		if r.status != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503, body = %v", r.status, r.body)
		}
		if r.body["error"] == nil {
			t.Fatalf("503 body = %v, want JSON error", r.body)
		}
		if r.body["changes"] != nil || r.body["timedOut"] != nil {
			t.Fatalf("503 leaked page content: %v", r.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked session poll did not return after shutdown")
	}
}

// A parked poll with a canceled context returns without a response; used to
// confirm the handler maps context cancellation to silence rather than a 500.
func TestSessionPollContextCanceledSilent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=30000", nil).
		WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, r)
		close(done)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after context cancellation")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("canceled poll wrote status %d, want no response", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("canceled poll wrote a body: %q", w.Body.String())
	}
}

// After a process restart the same parked request behaves identically: no
// fabricated cursor movement, and a fresh commit still wakes the wait.
func TestSessionPollPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-poll.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionChanges(t, h, "sess", "doc", `{"changes":[{"id":"c1","payload":{"n":1}}]}`)
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
	srv := httptest.NewServer(h2)
	defer srv.Close()

	// Caught-up poll after restart times out at the same cursor without
	// producing a change or advancing it.
	w, body := doRequest(t, h2, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=20")
	if w.Code != http.StatusOK || body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
		t.Fatalf("poll after restart = %d %v", w.Code, body)
	}

	// A session commit after restart wakes a parked poll with cursor 2.
	ch := pollOverHTTP(t, srv, sessionPollPath("sess", "doc")+"?after=1&waitMs=10000")
	time.Sleep(80 * time.Millisecond)
	w, _ = postSessionChanges(t, h2, "sess", "doc", `{"changes":[{"id":"c2","payload":{"n":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	select {
	case r := <-ch:
		if r.status != http.StatusOK || r.err != nil {
			t.Fatalf("woken after restart = %d %v", r.status, r.err)
		}
		rows := r.body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c2" ||
			r.body["nextCursor"].(float64) != 2 || r.body["timedOut"] != false {
			t.Fatalf("woken page after restart = %v", r.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("post-restart commit did not wake the parked poll")
	}
}
