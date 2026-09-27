package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// postSessionChanges posts a batch to the session-scoped change collection.
func postSessionChanges(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionChangesPath(session, doc), body)
}

func TestSessionPostChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// First commit on an unknown document allocates from cursor 1, in request
	// order, with the document-level success shape byte-for-byte.
	w, _ := postSessionChanges(t, h, "sess", "doc1", `{"changes":[
		{"id":"c1","payload":{"n":1}},
		{"id":"c2","payload":[1,true,null]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"c1","created":true,"cursor":1},` +
		`{"id":"c2","created":true,"cursor":2}` +
		`]}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The calling device is the session's owning device: the stored rows name
	// it, exactly as a document-level commit by that device would.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("list = %v next=%v", rows, body["nextCursor"])
	}
	for i, row := range rows {
		r := row.(map[string]any)
		if r["deviceId"] != "dev" || r["id"] != fmt.Sprintf("c%d", i+1) {
			t.Fatalf("row %d = %v", i, r)
		}
	}
}

func TestSessionPostChangesHTTPSharesCursorSpace(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2)

	// The session commit continues the document-level cursor space.
	w, body := postSessionChanges(t, h, "sess", "doc1", map[string]any{
		"changes": []any{map[string]any{"id": "s1", "payload": map[string]any{"s": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("result = %v", r)
	}

	// A following document-level commit takes the next cursor: one contiguous
	// space across both commit paths.
	w, body = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d1", "payload": map[string]any{"d": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["cursor"].(float64) != 4 {
		t.Fatalf("document-level cursor = %v", r)
	}
}

func TestSessionPostChangesHTTPIdempotent(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"x","payload":{"a":1,"b":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical repost (decoded-equal payload, different key order and number
	// format): created=false with the first cursor, nothing new written.
	w, body := postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"x","payload":{"b":2.0,"a":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("repost status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["id"] != "x" || r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("repost result = %v", r)
	}

	// A mixed batch resolves per element: the existing id is idempotent, the
	// new id takes the next cursor.
	w, body = postSessionChanges(t, h, "sess", "doc1", `{"changes":[
		{"id":"x","payload":{"a":1,"b":2}},
		{"id":"y","payload":null}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	results := body["results"].([]any)
	r0 := results[0].(map[string]any)
	r1 := results[1].(map[string]any)
	if r0["created"] != false || r0["cursor"].(float64) != 1 {
		t.Fatalf("existing result = %v", r0)
	}
	if r1["id"] != "y" || r1["created"] != true || r1["cursor"].(float64) != 2 {
		t.Fatalf("new result = %v", r1)
	}

	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("rows = %v next = %d", rows, next)
	}
}

func TestSessionPostChangesHTTPConflict(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"x","payload":{"k":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Same id, different payload: 409 naming the conflicting id; the batch's
	// other (valid, new) element is not written either.
	w, body := postSessionChanges(t, h, "sess", "doc1", `{"changes":[
		{"id":"fresh","payload":{"n":1}},
		{"id":"x","payload":{"k":2}}
	]}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("payload conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("conflict content type = %q", ct)
	}
	if body["error"] == nil || body["conflictId"] != "x" {
		t.Fatalf("conflict body = %s", w.Body.String())
	}

	// Same id and payload but a different source device is a conflict too:
	// only the session device AND the decoded payload matching is idempotent.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	w, body = postSessionChanges(t, h, "sess-2", "doc1", `{"changes":[{"id":"x","payload":{"k":1}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "x" {
		t.Fatalf("device conflict = %d %s", w.Code, w.Body.String())
	}

	// Both conflicts left the log untouched: no half batch, cursor unmoved.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "x" || next != 1 {
		t.Fatalf("conflict leaked writes: rows = %v next = %d", rows, next)
	}
}

func TestSessionPostChangesHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	url := sessionChangesPath("sess", "doc1")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"changes":[{"id":"a","payload":1}]}`},
		{"missing content type", "", `{"changes":[{"id":"a","payload":1}]}`},
		{"content type with json suffix", "application/vnd.api+json", `{"changes":[{"id":"a","payload":1}]}`},
		{"malformed json", "application/json", `{"changes":[`},
		{"trailing content", "application/json", `{"changes":[{"id":"a","payload":1}]}garbage`},
		{"second json value", "application/json", `{"changes":[{"id":"a","payload":1}]}{"changes":[{"id":"b","payload":2}]}`},
		{"missing changes", "application/json", `{}`},
		{"null changes", "application/json", `{"changes":null}`},
		{"empty changes", "application/json", `{"changes":[]}`},
		{"changes not an array", "application/json", `{"changes":{"id":"a"}}`},
		{"element not an object", "application/json", `{"changes":[1]}`},
		{"empty id", "application/json", `{"changes":[{"id":"","payload":1}]}`},
		{"missing id", "application/json", `{"changes":[{"payload":1}]}`},
		{"numeric id", "application/json", `{"changes":[{"id":123,"payload":1}]}`},
		{"missing payload", "application/json", `{"changes":[{"id":"a"}]}`},
		{"duplicate ids in batch", "application/json", `{"changes":[{"id":"a","payload":1},{"id":"a","payload":2}]}`},
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

	// Zero writes: every rejected request left the document unknown.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionPostChangesHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Never-created session -> 404 JSON with no results.
	w, body := postSessionChanges(t, h, "ghost", "doc1", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("404 leaked results: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed batch against an
	// unknown session is still a 400.
	w, _ = postSessionChanges(t, h, "ghost", "doc1", `{"changes":[{"id":"a","payload":1}]}trailing`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> commits through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	// Zero writes: nothing reached the change log.
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionPostChangesHTTPPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no results and zero writes.
	w, body := postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("403 leaked results: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionChanges(t, h, "ghost", "doc1", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}

	// Re-grant restores the commit path.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted commit = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 1 {
		t.Fatalf("re-granted result = %v", r)
	}
}

func TestSessionPostChangesHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Verbs other than GET/POST on the collection path are a JSON 400.
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		w, _ := doRequest(t, h, method, sessionChangesPath("sess", "doc1"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash and extra segments are a JSON 400,
	// never a redirect or an HTML page.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/changes",
		"/v1/sessions/sess/documents//changes",
		"/v1/sessions/sess/documents/doc1/changes/",
		"/v1/sessions/sess/documents/doc1/changes/extra",
		"/v1/sessions/sess/documents/doc1/changes/extra/more",
	} {
		r := newJSONRequest(http.MethodPost, p, `{"changes":[{"id":"a","payload":1}]}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("POST %s status = %d, want 400, body = %q", p, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("POST %s redirected to %q", p, loc)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
			t.Fatalf("POST %s leaked HTML: %q", p, w.Body.String())
		}
	}

	// Zero writes: the document is still unknown.
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionPostChangesHTTPWakesLongPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 1)

	done := make(chan map[string]any, 1)
	go func() {
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/poll?after=1&waitMs=30000")
		if w.Code != http.StatusOK {
			t.Errorf("poll status = %d body = %s", w.Code, w.Body.String())
		}
		done <- body
	}()

	// Give the poll a moment to park, then commit through the session.
	time.Sleep(50 * time.Millisecond)
	w, _ := postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"live","payload":{"n":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	select {
	case body := <-done:
		rows := body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "live" {
			t.Fatalf("polled rows = %v", rows)
		}
		row := rows[0].(map[string]any)
		if row["deviceId"] != "dev" || row["cursor"].(float64) != 2 {
			t.Fatalf("polled row = %v", row)
		}
		if body["timedOut"] != false {
			t.Fatalf("poll timed out despite the commit: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session commit did not wake the parked poll")
	}
}

func TestSessionPostChangesHTTPPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// A batch committed through the session-scoped endpoint is pushed to the
	// live subscription like any other write path.
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/changes", map[string]any{
		"changes": []any{map[string]any{"id": "sess-1", "payload": map[string]any{"n": 2}}},
	}); code != http.StatusOK {
		t.Fatalf("session post status = %d", code)
	}
	c := conn.readChange()
	if c.Cursor != 2 || c.ID != "sess-1" || c.DeviceID != "dev-1" {
		t.Fatalf("pushed frame = %+v", c)
	}
}

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
			w, _ := postSessionChanges(t, h, "sess", "doc", map[string]any{
				"changes": []any{map[string]any{"id": fmt.Sprintf("id-%02d", i), "payload": map[string]any{"i": i}}},
			})
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

	// No cursor is allocated twice and no record is lost.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != n || body["nextCursor"].(float64) != n {
		t.Fatalf("stored %d changes, nextCursor = %v, want %d", len(rows), body["nextCursor"], n)
	}
	seen := make(map[float64]bool, n)
	for _, row := range rows {
		cursor := row.(map[string]any)["cursor"].(float64)
		if seen[cursor] {
			t.Fatalf("cursor %v allocated twice", cursor)
		}
		seen[cursor] = true
	}
}

func TestSessionPostChangesHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-post-changes.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionChanges(t, h, "sess", "doc1", `{"changes":[
		{"id":"c1","payload":{"a":1}},
		{"id":"c2","payload":[1,true,null]}
	]}`)
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

	// Idempotency survives the restart: same session device and decoded
	// payload -> created=false with the first cursor.
	w, body := postSessionChanges(t, h2, "sess", "doc1", `{"changes":[{"id":"c1","payload":{"a":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("idempotent after restart = %v", r)
	}

	// So does the conflict decision.
	w, body = postSessionChanges(t, h2, "sess", "doc1", `{"changes":[{"id":"c1","payload":{"a":2}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// Raw payloads read back verbatim and the cursor space continues.
	w, body = doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("list after restart = %v next=%v", rows, body["nextCursor"])
	}
	second := rows[1].(map[string]any)
	if second["deviceId"] != "dev" {
		t.Fatalf("device after restart = %v", second)
	}
	payload := second["payload"].([]any)
	if len(payload) != 3 || payload[0].(float64) != 1 || payload[1] != true || payload[2] != nil {
		t.Fatalf("payload after restart = %v", second["payload"])
	}

	w, body = postSessionChanges(t, h2, "sess", "doc1", `{"changes":[{"id":"c3","payload":42}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("post-restart cursor = %v", r)
	}
}

// A compacted-away id still answers idempotency and conflict decisions from
// its retained summary when the commit arrives through the session path.
func TestSessionPostChangesHTTPCompactedIdentity(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postDocChanges(t, h, "doc", "dev", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// The trimmed id is idempotent with its first cursor.
	w, body := postSessionChanges(t, h, "sess", "doc", `{"changes":[{"id":"c1","payload":{"n":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed idempotent = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("trimmed result = %v", r)
	}

	// A differing payload against the trimmed id is still a 409.
	w, body = postSessionChanges(t, h, "sess", "doc", `{"changes":[{"id":"c1","payload":{"n":9}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("trimmed conflict = %d %s", w.Code, w.Body.String())
	}

	// New ids continue past the pre-compaction cursor space.
	w, body = postSessionChanges(t, h, "sess", "doc", `{"changes":[{"id":"c3","payload":{"n":3}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("post-compaction cursor = %v", r)
	}
}

// The document-level commit is untouched by the new entry: it still requires
// its body deviceId and answers with its own shapes.
func TestSessionPostChangesHTTPDocumentPostUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", `{"changes":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("document post without deviceId = %d, want 400", w.Code)
	}

	w, body := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "a", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 1 {
		t.Fatalf("document post result = %v", r)
	}

	// The session path reads back exactly what the document path wrote.
	w, body = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1"))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["deviceId"] != "dev" {
		t.Fatalf("session read = %v", rows)
	}
}

// The session commit's success body is byte-identical to the document-level
// commit's for the same batch.
func TestSessionPostChangesHTTPSuccessShapeMatchesDocumentPost(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := postJSON(t, h, "/v1/documents/docA/changes", `{"deviceId":"dev","changes":[{"id":"x","payload":{"k":1}},{"id":"y","payload":[1]}]}`)
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionChanges(t, h, "sess", "docB", `{"changes":[{"id":"x","payload":{"k":1}},{"id":"y","payload":[1]}]}`)
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}
