package server

import (
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

// Rows already past after return immediately with timedOut=false; the response
// keeps cursor order, the limit page size and the changes/nextCursor/timedOut
// key order, byte-compatible with the document-level long poll.
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
		t.Fatalf("rows not cursor-ordered: %v", rows)
	}
	if body["nextCursor"].(float64) != 3 || body["timedOut"] != false {
		t.Fatalf("page = %v", body)
	}
}

// An unknown document never parks: it answers at once with an empty list,
// cursor 0 and timedOut=false, even with a long wait.
func TestSessionPollUnknownDocumentImmediateEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	done := make(chan struct {
		code int
		body map[string]any
	}, 1)
	go func() {
		w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "ghost")+"?waitMs=30000")
		done <- struct {
			code int
			body map[string]any
		}{w.Code, body}
	}()
	select {
	case r := <-done:
		if r.code != http.StatusOK {
			t.Fatalf("status = %d", r.code)
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

// A known document caught up to after parks until the deadline, then echoes the
// caller's cursor with an empty list and timedOut=true; it never advances the
// cursor.
func TestSessionPollTimeoutEchoesCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	start := time.Now()
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=2&waitMs=80")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if time.Since(start) < 60*time.Millisecond {
		t.Fatal("poll returned before its wait elapsed")
	}
	if len(body["changes"].([]any)) != 0 ||
		body["nextCursor"].(float64) != 2 ||
		body["timedOut"] != true {
		t.Fatalf("timeout page = %v", body)
	}
}

// The default wait is zero: a caught-up known document answers immediately with
// timedOut=true rather than parking.
func TestSessionPollDefaultWaitIsZero(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	start := time.Now()
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if time.Since(start) > time.Second {
		t.Fatal("default waitMs=0 parked the poll")
	}
	if body["timedOut"] != true || body["nextCursor"].(float64) != 1 {
		t.Fatalf("default-wait page = %v", body)
	}
}

// Illegal after/limit/waitMs values are a 400 and take precedence over the
// session and permission lookups; every rejection writes nothing.
func TestSessionPollRejectsBadParamsBeforeSessionCheck(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	bad := []string{
		"waitMs=-1",
		"waitMs=30001",
		"waitMs=abc",
		"waitMs=1.5",
		"after=-1",
		"after=x",
		"limit=0",
		"limit=1001",
	}
	for _, q := range bad {
		// Bad params win even over an unknown session ...
		w, body := doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?"+q)
		if w.Code != http.StatusBadRequest || body["error"] == nil {
			t.Fatalf("unknown session ?%s = %d %s, want 400", q, w.Code, w.Body.String())
		}
		// ... and over a revoked document.
		w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?"+q)
		if w.Code != http.StatusBadRequest || body["error"] == nil {
			t.Fatalf("revoked doc ?%s = %d %s, want 400", q, w.Code, w.Body.String())
		}
	}
}

// Session existence is checked before document permission: an unknown session
// is 404 even against a revoked document, and neither failure returns change
// content.
func TestSessionPollSessionAndPermissionStates(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	registerDevice(t, h, "other")
	w, _ := postJSON(t, h, "/v1/devices/other/sessions", map[string]any{"sessionId": "sess-other"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	seedDoc(t, h, "doc", 1)

	// Unknown session: 404, no change content.
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?waitMs=1")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["changes"] != nil || body["nextCursor"] != nil || body["timedOut"] != nil {
		t.Fatalf("404 leaked poll content: %s", w.Body.String())
	}

	// Revoke dev's permission; dev's session is 403 while the still-authorized
	// other session reads normally.
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?waitMs=1")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v, want 403", w.Code, body)
	}
	if body["changes"] != nil || body["nextCursor"] != nil || body["timedOut"] != nil {
		t.Fatalf("403 leaked poll content: %s", w.Body.String())
	}
	// Session existence precedes permission: unknown session + revoked doc 404.
	w, _ = doRequest(t, h, http.MethodGet, sessionPollPath("ghost", "doc")+"?waitMs=1")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session against revoked doc = %d, want 404", w.Code)
	}
	w, body = doRequest(t, h, http.MethodGet, sessionPollPath("sess-other", "doc")+"?after=1&waitMs=1")
	if w.Code != http.StatusOK || body["timedOut"] != true {
		t.Fatalf("authorized session after peer revoke = %d %v", w.Code, body)
	}

	// A parked request never got a chance to write: the log is still one row.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("failed polls leaked state: %+v next=%d", rows, next)
	}
}

// A session batch commit wakes a parked session poll right after the
// transaction commits, with the new page and timedOut=false.
func TestSessionPollWakesOnSessionCommit(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	got := parkSessionPoll(t, srv.URL, "sess", "doc", 1)

	time.Sleep(100 * time.Millisecond)
	w, _ := postJSON(t, h, "/v1/sessions/sess/documents/doc/changes", map[string]any{
		"changes": []any{map[string]any{"id": "s1", "payload": map[string]any{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	r := awaitPoll(t, got)
	rows := r.body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "s1" {
		t.Fatalf("woken rows = %v", rows)
	}
	if r.body["nextCursor"].(float64) != 2 || r.body["timedOut"] != false {
		t.Fatalf("woken page = %v", r.body)
	}
}

// The session poll shares the document-level wait registry: a document-level
// commit also wakes it.
func TestSessionPollWakesOnDocumentCommit(t *testing.T) {
	h, _ := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	got := parkSessionPoll(t, srv.URL, "sess", "doc", 1)

	time.Sleep(100 * time.Millisecond)
	postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d2", "payload": map[string]any{"n": 2}}},
	})

	r := awaitPoll(t, got)
	rows := r.body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "d2" {
		t.Fatalf("woken rows = %v", rows)
	}
	if r.body["timedOut"] != false {
		t.Fatalf("woken page = %v", r.body)
	}
}

type pollResponse struct {
	status int
	body   map[string]any
	err    error
}

// parkSessionPoll starts a session long poll against a live test server in the
// background; the caller commits the waking change after parking.
func parkSessionPoll(t *testing.T, baseURL, session, doc string, after int64) <-chan pollResponse {
	t.Helper()
	got := make(chan pollResponse, 1)
	go func() {
		resp, err := http.Get(baseURL + sessionPollPath(session, doc) + "?after=1&waitMs=10000")
		if err != nil {
			got <- pollResponse{err: err}
			return
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		got <- pollResponse{status: resp.StatusCode, body: body}
	}()
	return got
}

func awaitPoll(t *testing.T, got <-chan pollResponse) pollResponse {
	t.Helper()
	select {
	case r := <-got:
		if r.err != nil {
			t.Fatalf("poll request: %v", r.err)
		}
		if r.status != http.StatusOK {
			t.Fatalf("status = %d body = %v", r.status, r.body)
		}
		return r
	case <-time.After(3 * time.Second):
		t.Fatal("parked session poll was not woken")
		return pollResponse{}
	}
}

// A client disconnect cancels the wait immediately and leaves no change, cursor
// or other record.
func TestSessionPollClientDisconnectWritesNothing(t *testing.T) {
	h, s := newTestHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	client := &http.Client{Timeout: 100 * time.Millisecond}
	if _, err := client.Get(srv.URL + sessionPollPath("sess", "doc") + "?after=1&waitMs=30000"); err == nil {
		t.Fatal("expected the short client timeout to abort the request")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows, next, err := s.ListChanges("doc", 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 1 && next == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("canceled session wait appears to have written state")
}

// Wrong methods and malformed shapes around the poll path are JSON 400s — no
// redirect, no HTML — and empty identifier segments are rejected the same way.
func TestSessionPollMethodAndPathShape(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"POST on poll", http.MethodPost, "/v1/sessions/sess/documents/doc/changes/poll"},
		{"PUT on poll", http.MethodPut, "/v1/sessions/sess/documents/doc/changes/poll"},
		{"DELETE on poll", http.MethodDelete, "/v1/sessions/sess/documents/doc/changes/poll"},
		{"PATCH on poll", http.MethodPatch, "/v1/sessions/sess/documents/doc/changes/poll"},
		{"poll trailing slash", http.MethodGet, "/v1/sessions/sess/documents/doc/changes/poll/"},
		{"poll extra segment", http.MethodGet, "/v1/sessions/sess/documents/doc/changes/poll/extra"},
		{"missing changes segment", http.MethodGet, "/v1/sessions/sess/documents/doc/poll"},
		{"missing documents segment", http.MethodGet, "/v1/sessions/sess/doc/changes/poll"},
		{"empty session segment", http.MethodGet, "/v1/sessions//documents/doc/changes/poll"},
		{"empty document segment", http.MethodGet, "/v1/sessions/sess/documents//changes/poll"},
		{"empty changes segment", http.MethodGet, "/v1/sessions/sess/documents/doc//poll"},
		{"unknown suffix on collection", http.MethodGet, "/v1/sessions/sess/documents/doc/changes/extra"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path+"?waitMs=1", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
			var b map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
				t.Fatalf("body = %q, want JSON error", w.Body.String())
			}
		})
	}
}

// A session or document literally named "poll" keeps its ordinary routes: the
// keyword is recognized only in the endpoint's terminal segment position.
func TestSessionIdentifiersNamedPollKeepRoutes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "poll")
	seedDoc(t, h, "doc", 1)

	// Session named "poll": the poll endpoint must still resolve at its shape.
	w, body := doRequest(t, h, http.MethodGet, sessionPollPath("poll", "doc")+"?after=1&waitMs=20")
	if w.Code != http.StatusOK || body["timedOut"] != true {
		t.Fatalf("session named poll = %d %v", w.Code, body)
	}

	// Document named "poll": changes read and long poll both work.
	postJSON(t, h, "/v1/documents/poll/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "k1", "payload": map[string]any{"n": 1}}},
	})
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/poll/documents/poll/changes")
	if w.Code != http.StatusOK || body["nextCursor"].(float64) != 1 {
		t.Fatalf("document named poll read = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/poll/documents/poll/changes/poll?after=0&waitMs=20")
	if w.Code != http.StatusOK || body["timedOut"] != false {
		t.Fatalf("document named poll poll = %d %v", w.Code, body)
	}
}

// Waiting produces no change and consumes no cursor: after a restart the same
// request observes the same verdict, body and cursor.
func TestSessionPollPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-poll.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	w, _ := doRequest(t, h, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=20")
	if w.Code != http.StatusOK {
		t.Fatalf("poll before restart = %d", w.Code)
	}
	before := w.Body.Bytes()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	w2, _ := doRequest(t, h2, http.MethodGet, sessionPollPath("sess", "doc")+"?after=1&waitMs=20")
	if w2.Code != http.StatusOK {
		t.Fatalf("poll after restart = %d", w2.Code)
	}
	if strings.TrimSpace(string(w2.Body.Bytes())) != strings.TrimSpace(string(before)) {
		t.Fatalf("poll body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}

	// The parked wait allocated no cursor: the next commit is still cursor 2.
	postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "after-restart", "payload": map[string]any{"n": 2}}},
	})
	w2, body := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes?after=1")
	if w2.Code != http.StatusOK {
		t.Fatal(w2.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("next commit cursor = %v, want 2 (waiting consumed a cursor)", rows)
	}
}
