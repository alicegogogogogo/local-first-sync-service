package server

import (
	"fmt"
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

// postSessionCompact posts one body to the session-scoped compaction entry.
func postSessionCompact(t *testing.T, h http.Handler, session, doc string, body any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, sessionCompactPath(session, doc), body)
	return w
}

// postSessionChangesHTTP commits a batch through the session-scoped change
// collection and fails the test on a non-200 answer.
func postSessionChangesHTTP(t *testing.T, h http.Handler, session, doc string, n int) {
	t.Helper()
	batch := make([]any, n)
	for i := range batch {
		batch[i] = map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}}
	}
	w, _ := postJSON(t, h, sessionChangesPath(session, doc), map[string]any{"changes": batch})
	if w.Code != http.StatusOK {
		t.Fatalf("session post changes: %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCompactEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postSessionChangesHTTP(t, h, "sess", "doc", 3)

	// Snapshot at cursor 2, so the compaction boundary is 2.
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}

	// An empty object is the whole body contract.
	w = postSessionCompact(t, h, "sess", "doc", map[string]any{})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"boundary":2,"removed":2}`+"\n" {
		t.Fatalf("compact body = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("compact content type = %q", ct)
	}

	// Only the online tail is readable through the session view, in order.
	w, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?after=0")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("changes after compact = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["id"] != "c3" || row["deviceId"] != "dev-1" || int64(row["cursor"].(float64)) != 3 {
		t.Fatalf("remaining row = %v", row)
	}
	if next := int64(body["nextCursor"].(float64)); next != 3 {
		t.Fatalf("nextCursor = %d, want 3", next)
	}

	// Repeating the compaction is not an error: same boundary, zero removed.
	w = postSessionCompact(t, h, "sess", "doc", map[string]any{})
	if w.Code != http.StatusOK || w.Body.String() != `{"boundary":2,"removed":0}`+"\n" {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}

	// The cursor space continues past the pre-compaction maximum.
	w, body = postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post after compact: %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 4 {
		t.Fatalf("new change result = %v, want created cursor 4", result)
	}
}

// The body only has to be one legal JSON value: null, scalars, arrays and
// objects with stray fields — including a stray deviceId naming another
// device — are all accepted and never override the session device.
func TestSessionCompactAcceptsAnyJSONBody(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-2")
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postSessionChangesHTTP(t, h, "sess", "doc", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, body := range []string{
		`{}`,
		`null`,
		`true`,
		`42`,
		`"sweep"`,
		`[1, {"x": 2}]`,
		`{"deviceId":"dev-2"}`,
		`{"deviceId":"dev-2","unexpected":[{"nested":[true,null]}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, sessionCompactPath("sess", "doc"), strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusOK {
				t.Fatalf("body %q status = %d %s", body, rec.Code, rec.Body.String())
			}
		})
	}

	// The document was compacted exactly once: boundary 2, the first call
	// removed two rows and the repeats removed none. The session device —
	// never dev-2 — owned every call.
	if got := len(getChanges(t, h, "sess", "doc")); got != 0 {
		t.Fatalf("online changes = %d, want 0 (fully trimmed)", got)
	}
	// The stray dev-2 identity never applied: re-posting c1 as dev-1 is still
	// idempotent against the retained summary.
	w, bodyMap := postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed idempotent re-post = %d %s", w.Code, w.Body.String())
	}
	res := bodyMap["results"].([]any)[0].(map[string]any)
	if res["created"] != false || int64(res["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed re-post result = %v", res)
	}
}

// getChanges reads the session document's online change page.
func getChanges(t *testing.T, h http.Handler, session, doc string) []any {
	t.Helper()
	_, body := doRequest(t, h, http.MethodGet, sessionChangesPath(session, doc)+"?limit=1000")
	return body["changes"].([]any)
}

func TestSessionCompactUnknownAndSnapshotless(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	// An unknown document compacts successfully with boundary 0.
	w := postSessionCompact(t, h, "sess", "ghost", map[string]any{})
	if w.Code != http.StatusOK || w.Body.String() != `{"boundary":0,"removed":0}`+"\n" {
		t.Fatalf("unknown compact = %d %q", w.Code, w.Body.String())
	}

	// A document without snapshots has boundary 0: nothing leaves the log.
	postSessionChangesHTTP(t, h, "sess", "doc", 2)
	w = postSessionCompact(t, h, "sess", "doc", map[string]any{})
	if w.Code != http.StatusOK || w.Body.String() != `{"boundary":0,"removed":0}`+"\n" {
		t.Fatalf("snapshotless compact = %d %q", w.Code, w.Body.String())
	}
	if got := len(getChanges(t, h, "sess", "doc")); got != 2 {
		t.Fatalf("snapshotless changes = %d, want 2", got)
	}
}

func TestSessionCompactValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postSessionChangesHTTP(t, h, "sess", "doc", 1)
	// A second document on which dev-1 is revoked, for the 403 ordering case.
	postDocChanges(t, h, "doc2", "dev-1", 1)
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Body-shape failures are 400 JSON with zero writes, even though the body
	// fields are otherwise uninterpreted.
	for _, tc := range []struct {
		name        string
		contentType string
		body        string
	}{
		{"content type", "text/plain", `{}`},
		{"missing content type", "", `{}`},
		{"invalid JSON", "application/json", `{`},
		{"trailing content", "application/json", `{} {}`},
		{"bare text", "application/json", `not json`},
		{"empty body", "application/json", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, sessionCompactPath("sess", "doc"), strings.NewReader(tc.body))
			if tc.contentType != "" {
				r.Header.Set("Content-Type", tc.contentType)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}

	// Path and method failures: 400 JSON, never a redirect or HTML.
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
		{"missing documents segment", http.MethodPost, "/v1/sessions/sess/doc/changes/compact"},
		{"wrong method GET", http.MethodGet, sessionCompactPath("sess", "doc")},
		{"wrong method DELETE", http.MethodDelete, sessionCompactPath("sess", "doc")},
		{"wrong method PUT", http.MethodPut, sessionCompactPath("sess", "doc")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			r.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if loc := rec.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}

	// A session or document literally named "compact" keeps its ordinary
	// routes: the keyword is an endpoint word only in its own segment.
	w, _ = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "compact"})
	if w.Code != http.StatusOK {
		t.Fatalf("create session named compact: %d %s", w.Code, w.Body.String())
	}
	postSessionChangesHTTP(t, h, "compact", "compact", 1)
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("compact", "compact"))
	if w.Code != http.StatusOK {
		t.Fatalf("identifier named compact read = %d %s", w.Code, w.Body.String())
	}
	if got := len(getChanges(t, h, "compact", "compact")); got != 1 {
		t.Fatalf("identifier named compact changes = %d", got)
	}

	// Shape precedes existence: a malformed body against an unknown session is
	// still a 400, not a 404.
	r := httptest.NewRequest(http.MethodPost, sessionCompactPath("ghost", "doc"), strings.NewReader(`{`))
	r.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad body + unknown session = %d, want 400", rec.Code)
	}

	// An unknown or deleted session is a 404 (after the shape checks).
	w = postSessionCompact(t, h, "no-such-session", "doc", map[string]any{})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d %s", w.Code, w.Body.String())
	}

	// Revoked permission is a 403, after both shape and session checks.
	w = postSessionCompact(t, h, "sess", "doc2", map[string]any{})
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d %s", w.Code, w.Body.String())
	}

	// None of the failures wrote anything: doc still has its one online row
	// and no boundary was recorded for it.
	if got := len(getChanges(t, h, "sess", "doc")); got != 1 {
		t.Fatalf("changes after failed compactions = %d, want 1", got)
	}
	if _, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?after=1"); int64(body["nextCursor"].(float64)) != 1 {
		t.Fatalf("nextCursor after failed compactions = %v, want 1 (no boundary)", body["nextCursor"])
	}
}

// Deleting the session (by deregistering its owning device) turns the entry
// into a 404.
func TestSessionCompactDeletedSession(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("deregister = %d %s", w.Code, w.Body.String())
	}
	w = postSessionCompact(t, h, "sess", "doc", map[string]any{})
	if w.Code != http.StatusNotFound {
		t.Fatalf("deleted session compact = %d %s", w.Code, w.Body.String())
	}
}

// Compaction does not wake a parked session long poll and a session
// subscription starting inside the trimmed range backfills from the first
// online change and then continues live.
func TestSessionCompactPollAndSubscription(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postSessionChangesHTTP(t, h, "sess", "doc", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A long poll parked at the tail is not woken by the compaction: it only
	// returns when its own wait deadline expires.
	type pollResult struct {
		body    map[string]any
		elapsed time.Duration
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		start := time.Now()
		_, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"/poll?after=3&waitMs=300")
		pollDone <- pollResult{body, time.Since(start)}
	}()
	time.Sleep(50 * time.Millisecond)
	if w := postSessionCompact(t, h, "sess", "doc", map[string]any{}); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	select {
	case got := <-pollDone:
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

	// A subscription starting inside the trimmed range backfills from the
	// first online change after the boundary, then continues live.
	srv := httptest.NewServer(h)
	defer srv.Close()
	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("subscribe status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.ID != "c3" || c.Cursor != 3 {
		t.Fatalf("backfill frame = %+v, want c3 at cursor 3", c)
	}
	w, _ = postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if c := conn.readChange(); c.ID != "c4" || c.Cursor != 4 {
		t.Fatalf("live frame = %+v, want c4 at cursor 4", c)
	}
}

// Trimmed ids keep answering idempotency/conflict through the session commit,
// replay, merge and restore entries; the merge base cursor below the boundary
// is a 400.
func TestSessionCompactTrimmedIdentityJudgments(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")
	postSessionChangesHTTP(t, h, "sess", "doc", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postSessionCompact(t, h, "sess", "doc", map[string]any{}); w.Code != http.StatusOK || w.Body.String() != `{"boundary":2,"removed":2}`+"\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}

	// Session commit: same source and payload is idempotent with cursor 1.
	w, body := postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}, "deviceId": "dev-2"}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed session commit = %d %s", w.Code, w.Body.String())
	}
	res := body["results"].([]any)[0].(map[string]any)
	if res["created"] != false || int64(res["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed session commit result = %v", res)
	}

	// Session replay: a differing payload is a 409 with zero writes.
	w, _ = postJSON(t, h, sessionChangesPath("sess", "doc")+"/replay", map[string]any{
		"operations": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 99}, "deviceId": "dev-2"}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("replay payload mismatch = %d %s", w.Code, w.Body.String())
	}

	// Session merge below the boundary: 400 because the conflict check cannot
	// be completed.
	w, _ = postJSON(t, h, sessionChangesPath("sess", "doc")+"/merge", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"k": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("merge below boundary = %d %s", w.Code, w.Body.String())
	}

	// Session merge at the boundary with a trimmed id and matching payload is
	// idempotent with the first cursor.
	w, body = postJSON(t, h, sessionChangesPath("sess", "doc")+"/merge", map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "c1", "payload": map[string]any{"n": 1}, "deviceId": "dev-2"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge trimmed id = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || int64(body["cursor"].(float64)) != 1 {
		t.Fatalf("merge trimmed result = %v", body)
	}
}

// The boundary, the reads and the retained decisions survive a restart when
// the compaction was driven through the session entry.
func TestSessionCompactRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postSessionChangesHTTP(t, h, "sess", "doc", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postSessionCompact(t, h, "sess", "doc", map[string]any{}); w.Code != http.StatusOK || w.Body.String() != `{"boundary":2,"removed":2}`+"\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()

	// Repeat compaction after restart: same boundary, zero removed.
	if w := postSessionCompact(t, h2, "sess", "doc", map[string]any{}); w.Code != http.StatusOK || w.Body.String() != `{"boundary":2,"removed":0}`+"\n" {
		t.Fatalf("re-compact after restart = %d %q", w.Code, w.Body.String())
	}
	if got := len(getChanges(t, h2, "sess", "doc")); got != 1 || getChanges(t, h2, "sess", "doc")[0].(map[string]any)["id"] != "c3" {
		t.Fatalf("changes after restart mismatch")
	}
	// The retained summary still answers idempotency after the restart.
	w, body := postJSON(t, h2, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay after restart = %d %s", w.Code, w.Body.String())
	}
	res := body["results"].([]any)[0].(map[string]any)
	if res["created"] != false || int64(res["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed replay result after restart = %v", res)
	}
}
