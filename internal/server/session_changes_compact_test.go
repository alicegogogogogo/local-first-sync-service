package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCompactPath is the session-scoped change-log compaction path.
func sessionCompactPath(session, doc string) string {
	return sessionChangesPath(session, doc) + "/compact"
}

// postSessionCompact posts one raw compaction body to the session-scoped
// compaction entry and returns the recorder so tests can assert the exact
// single-line body.
func postSessionCompact(t *testing.T, h http.Handler, session, doc string, body any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, sessionCompactPath(session, doc), body)
	return w
}

// snapshotViaHTTP stores a snapshot for doc at cursor, failing the test on
// error.
func snapshotViaHTTP(t *testing.T, h http.Handler, doc string, cursor int) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots", map[string]any{"cursor": cursor, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot %d: %d %s", cursor, w.Code, w.Body.String())
	}
}

func TestSessionCompactEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2) // boundary 2

	// The body only has to be one legal JSON value; a stray deviceId is
	// ignored and never overrides the session's owning device.
	w := postSessionCompact(t, h, "sess", "doc", `{"deviceId":"someone-else"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("compact body = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("compact content type = %q", ct)
	}

	// Only the online tail survives, readable through the session view.
	_, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?after=0")
	rows := body["changes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("changes after compact = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["id"] != "c3" || row["deviceId"] != "dev" || int64(row["cursor"].(float64)) != 3 {
		t.Fatalf("remaining row = %v", row)
	}
	if next := int64(body["nextCursor"].(float64)); next != 3 {
		t.Fatalf("nextCursor = %d, want 3", next)
	}

	// An empty page inside the trimmed range reports the boundary.
	_, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?after=3")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("changes after 3 = %v", got)
	}
	if next := int64(body["nextCursor"].(float64)); next != 3 {
		t.Fatalf("nextCursor after 3 = %d, want 3", next)
	}

	// Repeating the compaction is not an error: same boundary, zero removed.
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":2,\"removed\":0}\n" {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}

	// New changes committed through the session continue the never-reset
	// cursor space at cursor 4.
	w2, body := postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w2.Code != http.StatusOK {
		t.Fatalf("session post after compact: %d %s", w2.Code, w2.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 4 {
		t.Fatalf("new change result = %v, want created cursor 4", result)
	}
}

// Every legal JSON value is an acceptable compaction body; its content never
// affects the result.
func TestSessionCompactAcceptsAnyJSONBody(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	snapshotViaHTTP(t, h, "doc", 2)

	first := true
	for _, raw := range []string{
		`{}`,
		`{"deviceId":"dev"}`,
		`{"unexpected":["nested", 1, true, null]}`,
		`[]`,
		`[1, {"a":2}]`,
		`"a string"`,
		`123`,
		`true`,
		`null`,
	} {
		w := postSessionCompact(t, h, "sess", "doc", raw)
		if w.Code != http.StatusOK {
			t.Fatalf("body %s = %d %s", raw, w.Code, w.Body.String())
		}
		// The first legal body trims both rows; every later legal body is an
		// idempotent repeat against the same boundary.
		want := "{\"boundary\":2,\"removed\":0}\n"
		if first {
			want = "{\"boundary\":2,\"removed\":2}\n"
		}
		first = false
		if got := w.Body.String(); got != want {
			t.Fatalf("body %s -> %q, want %q", raw, got, want)
		}
	}
}

func TestSessionCompactUnknownAndSnapshotless(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// An unknown document compacts successfully with boundary 0.
	if w := postSessionCompact(t, h, "sess", "ghost", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("unknown compact = %d %q", w.Code, w.Body.String())
	}
	_, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/ghost/changes")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("unknown changes = %v", got)
	}
	if next := int64(body["nextCursor"].(float64)); next != 0 {
		t.Fatalf("unknown nextCursor = %d, want 0", next)
	}

	// A document without snapshots has boundary 0 and loses nothing.
	seedDoc(t, h, "doc", 2)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("snapshotless compact = %d %q", w.Code, w.Body.String())
	}
	_, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if got := body["changes"].([]any); len(got) != 2 {
		t.Fatalf("snapshotless changes = %v", got)
	}
}

func TestSessionCompactBodyValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	// Shape failures precede the session lookup: a malformed body against an
	// unknown session is still a 400.
	for _, tc := range []struct {
		name        string
		session     string
		body        string
		contentType string
	}{
		{"invalid JSON", "sess", `{not json`, "application/json"},
		{"trailing content", "sess", `{} {}`, "application/json"},
		{"empty body", "sess", ``, "application/json"},
		{"wrong content type", "sess", `{}`, "text/plain"},
		{"malformed body unknown session", "ghost", `{`, "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, sessionCompactPath(tc.session, "doc"), strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}
}

func TestSessionCompactPathAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"empty session id", http.MethodPost, "/v1/sessions//documents/doc/changes/compact"},
		{"empty document id", http.MethodPost, "/v1/sessions/sess/documents//changes/compact"},
		{"trailing slash", http.MethodPost, "/v1/sessions/sess/documents/doc/changes/compact/"},
		{"extra segment", http.MethodPost, "/v1/sessions/sess/documents/doc/changes/compact/x"},
		{"missing changes segment", http.MethodPost, "/v1/sessions/sess/documents/doc/compact"},
		{"wrong method GET", http.MethodGet, "/v1/sessions/sess/documents/doc/changes/compact"},
		{"wrong method DELETE", http.MethodDelete, "/v1/sessions/sess/documents/doc/changes/compact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}

	// A session or document literally named "compact" is an ordinary
	// identifier, not the endpoint keyword.
	createSessionViaHTTP(t, h, "dev2", "compact")
	w := postSessionCompact(t, h, "compact", "doc", `{}`)
	if w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("session named compact = %d %q", w.Code, w.Body.String())
	}
}

func TestSessionCompactGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	snapshotViaHTTP(t, h, "doc", 1)
	// Revoke the session owner's permission on a second document.
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A deleted session is a 404, even with an otherwise valid body.
	gw, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "gone"})
	if gw.Code != http.StatusOK {
		t.Fatalf("create gone session: %d %s", gw.Code, gw.Body.String())
	}
	dw, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/gone")
	if dw.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", dw.Code, dw.Body.String())
	}
	if w := postSessionCompact(t, h, "gone", "doc", `{}`); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session = %d %s", w.Code, w.Body.String())
	}

	// Revoked permission is a 403 after the shape and session checks pass.
	if w := postSessionCompact(t, h, "sess", "doc2", `{}`); w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d %s", w.Code, w.Body.String())
	}

	// The rejected compaction wrote nothing: doc2 has no boundary and an
	// authorized document is untouched; doc was never compacted.
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":1,\"removed\":1}\n" {
		t.Fatalf("doc compact after rejected doc2 compact = %d %q", w.Code, w.Body.String())
	}
}

func TestSessionCompactTrimmedIdentityAndMerge(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	snapshotViaHTTP(t, h, "doc", 2)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// Re-posting a trimmed id through the session commit with the same source
	// and payload is idempotent: created=false with the first cursor.
	w, body := postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay = %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != false || int64(result["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed replay result = %v, want created=false cursor 1", result)
	}

	// A differing payload through the session commit is a 409 with zero
	// writes.
	w, _ = postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 99}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("payload mismatch = %d, want 409", w.Code)
	}

	// A session merge whose base cursor is below the boundary cannot run the
	// conflict check: 400 and zero writes.
	if w, _ := postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "m0", "payload": map[string]any{"k": 1}},
	}); w.Code != http.StatusBadRequest {
		t.Fatalf("merge base below boundary = %d, want 400", w.Code)
	}

	// No cursor was allocated by either rejection.
	_, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?after=0")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("changes after rejections = %v", got)
	}
}

// Compaction is not a change: a session long poll parked at the tail is not
// woken by it and only returns when its own wait deadline expires.
func TestSessionCompactDoesNotWakeSessionPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2)

	type pollResult struct {
		body    map[string]any
		elapsed time.Duration
	}
	done := make(chan pollResult, 1)
	go func() {
		start := time.Now()
		_, body := doRequest(t, h, http.MethodGet,
			"/v1/sessions/sess/documents/doc/changes/poll?after=3&waitMs=300")
		done <- pollResult{body, time.Since(start)}
	}()
	time.Sleep(50 * time.Millisecond)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	select {
	case got := <-done:
		if got.body["timedOut"] != true {
			t.Fatalf("poll timedOut = %v", got.body)
		}
		if next := int64(got.body["nextCursor"].(float64)); next != 3 {
			t.Fatalf("poll nextCursor = %d, want 3", next)
		}
		if got.elapsed < 200*time.Millisecond {
			t.Fatalf("poll returned after %v: the compaction woke it", got.elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return")
	}
}

func TestSessionCompactRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()

	// The session must be re-established through HTTP after restart (the
	// device and session rows are durable; this is the same create call,
	// reporting created=false).
	w, _ := postJSON(t, h2, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatalf("re-create session after restart: %d %s", w.Code, w.Body.String())
	}

	// The boundary, reads and idempotency decisions survive the restart.
	if w := postSessionCompact(t, h2, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != "{\"boundary\":2,\"removed\":0}\n" {
		t.Fatalf("re-compact after restart = %d %q", w.Code, w.Body.String())
	}
	_, body := doRequest(t, h2, http.MethodGet, "/v1/sessions/sess/documents/doc/changes?after=0")
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c3" {
		t.Fatalf("changes after restart = %v", rows)
	}
	w2, body := postJSON(t, h2, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w2.Code != http.StatusOK {
		t.Fatalf("trimmed replay after restart = %d %s", w2.Code, w2.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != false || int64(result["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed replay result after restart = %v", result)
	}
}
