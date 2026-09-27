package server

import (
	"bytes"
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

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionPost posts a session-scoped batch with the given raw body.
func sessionPost(t *testing.T, h http.Handler, session, doc, rawBody string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/sessions/"+session+"/documents/"+doc+"/changes", rawBody)
}

// A valid session-scoped batch commits to an unknown document starting at
// cursor 1; the body carries only the change array and the stored rows name
// the device that owns the session, not anything from the body.
func TestSessionPostChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-A", "sess")

	w, body := sessionPost(t, h, "sess", "doc1", `{"changes":[
		{"id":"c1","payload":{"text":"hello"}},
		{"id":"c2","payload":[1,true,null]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	results, ok := body["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("results = %v", body["results"])
	}
	r0 := results[0].(map[string]any)
	if r0["id"] != "c1" || r0["created"] != true || r0["cursor"].(float64) != 1 {
		t.Fatalf("r0 = %v", r0)
	}
	r1 := results[1].(map[string]any)
	if r1["id"] != "c2" || r1["created"] != true || r1["cursor"].(float64) != 2 {
		t.Fatalf("r1 = %v", r1)
	}
	if body["deviceId"] != nil {
		t.Fatalf("response leaked a deviceId: %s", w.Body.String())
	}

	// The committed rows carry the session owner as their device.
	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows = %v", rows)
	}
	for _, row := range rows {
		if row.(map[string]any)["deviceId"] != "dev-A" {
			t.Fatalf("row device = %v, want dev-A", row)
		}
	}

	// The same commit is visible through the session-scoped read.
	w, list = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc1/changes")
	if w.Code != http.StatusOK || len(list["changes"].([]any)) != 2 {
		t.Fatalf("session read = %d %s", w.Code, w.Body.String())
	}
}

// Payloads are arbitrary JSON values stored verbatim, like the document-level
// commit.
func TestSessionPostChangesHTTPPayloadShapes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-9", "sess")

	w, body := sessionPost(t, h, "sess", "doc1", `{"changes":[
		{"id":"x1","payload":null},
		{"id":"x2","payload":42},
		{"id":"x3","payload":"str"},
		{"id":"x4","payload":[1,true,null]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if got := len(body["results"].([]any)); got != 4 {
		t.Fatalf("results = %v", body["results"])
	}

	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=10")
	want := `{"changes":[` +
		`{"id":"x1","deviceId":"dev-9","payload":null,"cursor":1},` +
		`{"id":"x2","deviceId":"dev-9","payload":42,"cursor":2},` +
		`{"id":"x3","deviceId":"dev-9","payload":"str","cursor":3},` +
		`{"id":"x4","deviceId":"dev-9","payload":[1,true,null],"cursor":4}` +
		`],"nextCursor":4}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// A deviceId carried in the body is ignored: the commit always attributes the
// changes to the session owner.
func TestSessionPostChangesHTTPIgnoresBodyDevice(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-A", "sess")

	w, _ := sessionPost(t, h, "sess", "doc1", `{"deviceId":"dev-other","changes":[
		{"id":"c1","payload":{"n":1}}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	row := list["changes"].([]any)[0].(map[string]any)
	if row["deviceId"] != "dev-A" {
		t.Fatalf("row device = %v, want the session owner dev-A", row["deviceId"])
	}
}

// Reposting the same id from the same session (same device) with a
// decoded-equal payload is idempotent: created=false and the first cursor; no
// new cursor is allocated.
func TestSessionPostChangesHTTPIdempotent(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := sessionPost(t, h, "sess", "doc", `{"changes":[{"id":"x","payload":{"a":1,"b":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := sessionPost(t, h, "sess", "doc", `{"changes":[{"id":"x","payload":{"b":2,"a":1.0}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("repost status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("idempotent result = %v", r)
	}

	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("idempotent repost moved the cursor: rows=%d next=%d", len(rows), next)
	}
}

// An existing id with a different source device or a different decoded
// payload is a 409 naming the conflicting id; the whole batch stays out.
func TestSessionPostChangesHTTPConflict(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-A", "sess-a")
	createSessionViaHTTP(t, h, "dev-B", "sess-b")

	// dev-A owns c1.
	w, _ := sessionPost(t, h, "sess-a", "doc", `{"changes":[{"id":"c1","payload":{"n":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A mixed batch from dev-B: a fresh id plus the conflicting id. Nothing in
	// it may land.
	w, body := sessionPost(t, h, "sess-b", "doc", `{"changes":[
		{"id":"c2","payload":{"n":2}},
		{"id":"c1","payload":{"n":1}}
	]}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if body["conflictId"] != "c1" {
		t.Fatalf("conflictId = %v", body["conflictId"])
	}

	// Payload mismatch from the same owner is a 409 too.
	w, body = sessionPost(t, h, "sess-a", "doc", `{"changes":[{"id":"c1","payload":{"n":99}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("payload conflict = %d %v", w.Code, body)
	}

	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || rows[0].ID != "c1" || next != 1 {
		t.Fatalf("conflict batches left data behind: %+v next=%d", rows, next)
	}
}

// Every shape violation is a 400 JSON error and writes nothing; shape is
// judged before the session even exists.
func TestSessionPostChangesHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"changes":[{"id":"a","payload":1}]}`},
		{"missing content type", "", `{"changes":[{"id":"a","payload":1}]}`},
		{"json suffix content type", "application/vnd.api+json", `{"changes":[{"id":"a","payload":1}]}`},
		{"malformed json", "application/json", `{"changes":[`},
		{"trailing content", "application/json", `{"changes":[{"id":"a","payload":1}]}garbage`},
		{"body is an array", "application/json", `[{"id":"a","payload":1}]`},
		{"body is a scalar", "application/json", `42`},
		{"missing changes", "application/json", `{}`},
		{"changes null", "application/json", `{"changes":null}`},
		{"empty changes", "application/json", `{"changes":[]}`},
		{"changes not an array", "application/json", `{"changes":{"id":"a"}}`},
		{"empty id", "application/json", `{"changes":[{"id":"","payload":1}]}`},
		{"numeric id", "application/json", `{"changes":[{"id":7,"payload":1}]}`},
		{"missing payload", "application/json", `{"changes":[{"id":"a"}]}`},
		{"duplicate ids", "application/json", `{"changes":[{"id":"a","payload":1},{"id":"a","payload":2}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/sessions/sess/documents/doc/changes", strings.NewReader(tc.body))
			if tc.contentType != "" {
				r.Header.Set("Content-Type", tc.contentType)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// A malformed body against a never-created session is still a 400: shape
	// beats session existence.
	r := httptest.NewRequest(http.MethodPost, "/v1/sessions/ghost/documents/doc/changes", strings.NewReader(`{"changes":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad body + missing session = %d, want 400", w.Code)
	}

	// Zero writes: nothing rejected above created a document or moved a cursor.
	if ok, _ := s.DocumentExists("doc"); ok {
		t.Fatal("rejected session posts created a document")
	}
	if rows, _, _ := s.ListChanges("doc", 0, 100); len(rows) != 0 {
		t.Fatalf("rejected posts wrote rows: %+v", rows)
	}
}

// A missing or deleted session is a 404 JSON error with no result content.
func TestSessionPostChangesHTTPSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	body := `{"changes":[{"id":"c1","payload":{"n":1}}]}`
	w, decoded := sessionPost(t, h, "ghost", "doc", body)
	if w.Code != http.StatusNotFound || decoded["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, decoded)
	}
	if decoded["results"] != nil {
		t.Fatalf("404 leaked results: %s", w.Body.String())
	}

	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, decoded = sessionPost(t, h, "sess", "doc", body)
	if w.Code != http.StatusNotFound || decoded["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, decoded)
	}
}

// Revoked permission is a 403 JSON error; session existence is checked first,
// and a re-grant restores the commit.
func TestSessionPostChangesHTTPPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	body := `{"changes":[{"id":"c1","payload":{"n":1}}]}`

	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, decoded := sessionPost(t, h, "sess", "doc", body)
	if w.Code != http.StatusForbidden || decoded["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, decoded)
	}
	if decoded["results"] != nil {
		t.Fatalf("403 leaked results: %s", w.Body.String())
	}

	// Session existence precedes permission: an unknown session on a revoked
	// document is a 404, not a 403.
	w, _ = sessionPost(t, h, "ghost", "doc", body)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Re-grant allows the commit through.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, decoded = sessionPost(t, h, "sess", "doc", body)
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted commit = %d %s", w.Code, w.Body.String())
	}
	if decoded["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("re-granted result = %v", decoded["results"])
	}
}

// Wrong methods, trailing slashes and extra segments are all 400 JSON errors,
// never a redirect or HTML.
func TestSessionPostChangesHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	for _, c := range []struct{ method, path string }{
		{http.MethodPut, "/v1/sessions/sess/documents/doc/changes"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc/changes"},
		{http.MethodPatch, "/v1/sessions/sess/documents/doc/changes"},
		{http.MethodOptions, "/v1/sessions/sess/documents/doc/changes"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/changes/"},
		{http.MethodPost, "/v1/sessions/sess/documents//changes"},
		{http.MethodPost, "/v1/sessions//documents/doc/changes"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/changes/extra"},
	} {
		r := httptest.NewRequest(c.method, c.path, strings.NewReader(`{"changes":[{"id":"a","payload":1}]}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400, body = %q", c.method, c.path, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content type = %q", c.method, c.path, ct)
		}
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("%s %s redirected to %q", c.method, c.path, loc)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
			t.Fatalf("%s %s emitted HTML: %q", c.method, c.path, w.Body.String())
		}
	}
}

// The session commit shares the document's one contiguous cursor space with
// the document-level commit, replay and merge.
func TestSessionPostChangesHTTPSharedCursorSpace(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Two document-level rows first.
	postDocChanges(t, h, "doc", "dev", 2)

	// A session-scoped batch continues at cursor 3.
	w, body := sessionPost(t, h, "sess", "doc", `{"changes":[
		{"id":"s1","payload":{"n":3}},
		{"id":"s2","payload":{"n":4}}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	results := body["results"].([]any)
	if results[0].(map[string]any)["cursor"].(float64) != 3 ||
		results[1].(map[string]any)["cursor"].(float64) != 4 {
		t.Fatalf("session cursors = %v", results)
	}

	// A later document-level post keeps going from 5.
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d5", "payload": map[string]any{"n": 5}}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 5 {
		t.Fatalf("document post after session = %v", body)
	}
}

// A session-scoped commit wakes a parked document-level long poll.
func TestSessionPostChangesHTTPWakesLongPoll(t *testing.T) {
	srv, _ := newWSTestServer(t) // same handler/app over a real TCP listener
	httpPost := func(path string, body string) {
		t.Helper()
		resp, err := srv.Client().Post(srv.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			data, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST %s = %d %s", path, resp.StatusCode, data)
		}
	}
	httpPost("/v1/devices", `{"deviceId":"dev"}`)
	httpPost("/v1/devices/dev/sessions", `{"sessionId":"sess"}`)
	// The document must already be known for a caught-up long poll to park
	// rather than return immediately empty; seed one row via the session path.
	httpPost("/v1/sessions/sess/documents/doc/changes", `{"changes":[{"id":"seed","payload":{"n":0}}]}`)

	type pollResult struct {
		status int
		body   map[string]any
	}
	done := make(chan pollResult, 1)
	go func() {
		resp, err := srv.Client().Get(srv.URL + "/v1/documents/doc/changes/poll?after=1&waitMs=10000")
		if err != nil {
			t.Errorf("poll: %v", err)
			return
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		var decoded map[string]any
		_ = json.Unmarshal(data, &decoded)
		done <- pollResult{resp.StatusCode, decoded}
	}()

	// Give the poll time to park, then commit through the session path.
	time.Sleep(100 * time.Millisecond)
	raw := bytes.NewBufferString(`{"changes":[{"id":"c1","payload":{"n":1}}]}`)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sessions/sess/documents/doc/changes", raw)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session post = %d", resp.StatusCode)
	}

	select {
	case r := <-done:
		if r.status != http.StatusOK {
			t.Fatalf("poll status = %d", r.status)
		}
		rows := r.body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c1" {
			t.Fatalf("poll rows = %v", rows)
		}
		if r.body["timedOut"] != false {
			t.Fatalf("poll timed out instead of waking: %v", r.body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked long poll was not woken by the session commit")
	}
}

// A session-scoped commit is pushed to a live subscriber like every other
// write path.
func TestSessionPostChangesHTTPPushesToSubscriber(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev", "sess", "doc", 0)

	client, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("subscribe = %d", resp.StatusCode)
	}
	defer client.close()
	client.setReadDeadline(3 * time.Second)

	raw := bytes.NewBufferString(`{"changes":[
		{"id":"p1","payload":{"n":1}},
		{"id":"p2","payload":{"n":2}}
	]}`)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/sessions/sess/documents/doc/changes", raw)
	req.Header.Set("Content-Type", "application/json")
	postResp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	postResp.Body.Close()

	first := client.readChange()
	second := client.readChange()
	if first.ID != "p1" || first.Cursor != 1 || first.DeviceID != "dev" {
		t.Fatalf("first push = %+v", first)
	}
	if second.ID != "p2" || second.Cursor != 2 {
		t.Fatalf("second push = %+v", second)
	}
}

// Concurrent session-scoped batches allocate each cursor exactly once and
// lose no record.
func TestSessionPostChangesHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"changes":[{"id":"id-%02d","payload":{"i":%d}}]}`, i, i)
			w, _ := postJSON(t, h, "/v1/sessions/sess/documents/doc/changes", body)
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("post %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != n {
		t.Fatalf("stored %d changes, want %d", len(rows), n)
	}
	if list["nextCursor"].(float64) != n {
		t.Fatalf("nextCursor = %v, want %d", list["nextCursor"], n)
	}
	seen := make(map[int64]string, n)
	for _, row := range rows {
		r := row.(map[string]any)
		cursor := int64(r["cursor"].(float64))
		if _, dup := seen[cursor]; dup {
			t.Fatalf("cursor %d allocated twice", cursor)
		}
		seen[cursor] = r["id"].(string)
	}
	for c := int64(1); c <= n; c++ {
		if _, ok := seen[c]; !ok {
			t.Fatalf("cursor %d missing", c)
		}
	}
}

// An id trimmed out of the online log by compaction is still idempotent
// through the session path (same device and payload -> created=false, first
// cursor), and a mismatch is still a 409.
func TestSessionPostChangesHTTPCompactedIdentity(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postDocChanges(t, h, "doc", "dev-1", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// c1 was trimmed. Re-posting it from its owner with an equal payload
	// answers from the retained summary.
	w, body := sessionPost(t, h, "sess", "doc", `{"changes":[{"id":"c1","payload":{"n":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed idempotent = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("trimmed result = %v", r)
	}

	// A differing payload conflicts against the retained summary.
	w, body = sessionPost(t, h, "sess", "doc", `{"changes":[{"id":"c1","payload":{"n":999}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("trimmed conflict = %d %v", w.Code, body)
	}
}

// Everything needed to judge a session commit survives a restart: raw
// payloads, cursors, idempotency and conflicts.
func TestSessionPostChangesHTTPPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-post-changes-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := sessionPost(t, h, "sess", "doc", `{"changes":[
		{"id":"c1","payload":{"a":{"b":[1,2,3]}}},
		{"id":"c2","payload":[true,null,"x"]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("commit before restart = %d", w.Code)
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

	// Raw payloads and cursors read back identically.
	w, list := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 2 {
		t.Fatalf("rows after restart = %v", rows)
	}
	r0 := rows[0].(map[string]any)
	if r0["id"] != "c1" || r0["cursor"].(float64) != 1 {
		t.Fatalf("r0 after restart = %v", r0)
	}
	payload, _ := json.Marshal(r0["payload"])
	if string(payload) != `{"a":{"b":[1,2,3]}}` {
		t.Fatalf("r0 payload after restart = %s", payload)
	}

	// Identical repost is still idempotent with the first cursor.
	w, body := sessionPost(t, h2, "sess", "doc", `{"changes":[
		{"id":"c1","payload":{"a":{"b":[1,2,3]}}},
		{"id":"c2","payload":[true,null,"x"]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent after restart = %d %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	if results[0].(map[string]any)["created"] != false || results[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("r0 idempotency after restart = %v", results[0])
	}
	if results[1].(map[string]any)["created"] != false || results[1].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("r1 idempotency after restart = %v", results[1])
	}

	// A new id continues past the previous high-water mark.
	w, body = sessionPost(t, h2, "sess", "doc", `{"changes":[{"id":"c3","payload":{}}]}`)
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 3 {
		t.Fatalf("new id after restart = %d %v", w.Code, body)
	}

	// A payload mismatch is still a 409 after restart.
	w, body = sessionPost(t, h2, "sess", "doc", `{"changes":[{"id":"c1","payload":{}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("conflict after restart = %d %v", w.Code, body)
	}
}
