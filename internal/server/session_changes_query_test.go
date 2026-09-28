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

// sessionQueryPath is the session-scoped change-log batch lookup path.
func sessionQueryPath(session, doc string) string {
	return sessionChangesPath(session, doc) + "/query"
}

// querySessionChanges sends a raw POST to the session batch lookup and returns
// the recorder.
func querySessionChanges(t *testing.T, h http.Handler, session, doc, rawBody string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, sessionQueryPath(session, doc), strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestSessionQueryChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postRawChanges(t, h, "doc1", "dev", `[
		{"id":"a","payload":{"k":"v"}},
		{"id":"b","payload":42},
		{"id":"c","payload":null}
	]`)

	// The body carries only ids: the calling device is the session's owning
	// device. Hits are answered in the order asked with the exact same body as
	// the document-level lookup.
	w := querySessionChanges(t, h, "sess", "doc1", `{"ids":["c","a","nope","b"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	want := `{"results":[` +
		`{"id":"c","status":"found","deviceId":"dev","payload":null,"cursor":3},` +
		`{"id":"a","status":"found","deviceId":"dev","payload":{"k":"v"},"cursor":1},` +
		`{"id":"nope","status":"missing"},` +
		`{"id":"b","status":"found","deviceId":"dev","payload":42,"cursor":2}` +
		`],"count":4}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// A stray deviceId is decoded and ignored: it can never override the session
// owner, even when it names another registered device.
func TestSessionQueryChangesHTTPIgnoresStrayDevice(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	registerDevice(t, h, "other")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	w := querySessionChanges(t, h, "sess", "doc1", `{"deviceId":"other","ids":["a"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[{"id":"a","status":"found","deviceId":"dev","payload":1,"cursor":1}],"count":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// A stray unregistered device is ignored the same way rather than 404.
	w = querySessionChanges(t, h, "sess", "doc1", `{"deviceId":"ghost","ids":["a"]}`)
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("stray ghost = %d %q want %q", w.Code, w.Body.String(), want)
	}
}

// A payload is presented exactly as the JSON content saved at commit time.
func TestSessionQueryChangesHTTPPayloadVerbatim(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"x","payload":{"arr":[1,true,null,"s"],"nested":{"z":1,"a":[{}]}}}]`)

	w := querySessionChanges(t, h, "sess", "doc1", `{"ids":["x"]}`)
	want := `{"results":[{"id":"x","status":"found","deviceId":"dev","payload":{"arr":[1,true,null,"s"],"nested":{"z":1,"a":[{}]}},"cursor":1}],"count":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every queried id appears in the answer, including unknown ones; count
// matches the array length even when the document has never existed.
func TestSessionQueryChangesHTTPMissingAndUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	for _, doc := range []string{"doc1", "never-heard-of-it"} {
		w := querySessionChanges(t, h, "sess", doc, `{"ids":["x","y"]}`)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d body = %s", doc, w.Code, w.Body.String())
		}
		want := `{"results":[{"id":"x","status":"missing"},{"id":"y","status":"missing"}],"count":2}` + "\n"
		if w.Body.String() != want {
			t.Fatalf("%s body = %q want %q", doc, w.Body.String(), want)
		}
	}
}

// A compacted-away id reports only its first cursor and trimmed status: no
// device and no payload are restored, while the online tail is still found.
func TestSessionQueryChangesHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postDocChanges(t, h, "doc", "dev-1", 4)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w := querySessionChanges(t, h, "sess", "doc", `{"ids":["c1","c2","c3","gone-never"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"c1","status":"compacted","cursor":1},` +
		`{"id":"c2","status":"compacted","cursor":2},` +
		`{"id":"c3","status":"found","deviceId":"dev-1","payload":{"n":3},"cursor":3},` +
		`{"id":"gone-never","status":"missing"}` +
		`],"count":4}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every request-shape violation is a 400 JSON error with zero writes, checked
// before the session even exists.
func TestSessionQueryChangesHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	url := sessionQueryPath("sess", "doc1")

	send := func(contentType, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, url, strings.NewReader(raw))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"ids":["a"]}`},
		{"missing content type", "", `{"ids":["a"]}`},
		{"content type with json suffix", "application/vnd.api+json", `{"ids":["a"]}`},
		{"invalid json", "application/json", `{"ids":[`},
		{"trailing content", "application/json", `{"ids":["a"]} junk`},
		{"second json value", "application/json", `{"ids":["a"]}{}`},
		{"missing ids", "application/json", `{}`},
		{"null ids", "application/json", `{"ids":null}`},
		{"empty ids", "application/json", `{"ids":[]}`},
		{"empty id element", "application/json", `{"ids":["a",""]}`},
		{"non-string id element", "application/json", `{"ids":["a",1]}`},
		{"ids not an array", "application/json", `{"ids":"a"}`},
		{"duplicate ids", "application/json", `{"ids":["a","a"]}`},
		{"non-object body", "application/json", `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := send(tc.contentType, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// A malformed body against an unknown session is still a 400: shape
	// validation precedes session existence.
	r := httptest.NewRequest(http.MethodPost, sessionQueryPath("ghost", "doc1"), strings.NewReader(`{"ids":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-existence = %d body = %s", w.Code, w.Body.String())
	}
}

// Session existence and permission are enforced after shape validation, and a
// failure exposes no change content.
func TestSessionQueryChangesHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	// Unknown session: 404 with an error only.
	w := querySessionChanges(t, h, "ghost", "doc1", `{"ids":["a"]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("404 leaked change content: %s", w.Body.String())
	}

	// A deleted session is a 404 too.
	gw, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "gone"})
	if gw.Code != http.StatusOK {
		t.Fatalf("create gone session: %d %s", gw.Code, gw.Body.String())
	}
	if dw, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/gone"); dw.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", dw.Code, dw.Body.String())
	}
	if w := querySessionChanges(t, h, "gone", "doc1", `{"ids":["a"]}`); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with an error only.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = querySessionChanges(t, h, "sess", "doc1", `{"ids":["a"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("403 leaked change content: %s", w.Body.String())
	}
}

// The lookup is read-only: it allocates no cursor, so a later commit receives
// the cursor it would have received without the queries, and it neither wakes
// a parked session long poll nor a push subscriber.
func TestSessionQueryChangesHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	// A parked session long poll with a short wait must time out despite
	// queries hitting the same session document.
	done := make(chan struct {
		changes  int
		timedOut bool
		err      error
	}, 1)
	go func() {
		changes, _, timedOut, err := s.WaitForChanges(t.Context(), "doc1", 1, 100, 250*time.Millisecond)
		done <- struct {
			changes  int
			timedOut bool
			err      error
		}{len(changes), timedOut, err}
	}()

	time.Sleep(30 * time.Millisecond) // let the poll park
	for i := 0; i < 3; i++ {
		if w := querySessionChanges(t, h, "sess", "doc1", `{"ids":["a","missing"]}`); w.Code != http.StatusOK {
			t.Fatalf("query %d = %d %s", i, w.Code, w.Body.String())
		}
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("poll: %v", got.err)
	}
	if !got.timedOut || got.changes != 0 {
		t.Fatalf("poll woken by a read-only query: %+v", got)
	}

	// The queries consumed no cursor: the next commit gets cursor 2.
	postRawChanges(t, h, "doc1", "dev", `[{"id":"b","payload":2}]`)
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 || rows[1].Cursor != 2 {
		t.Fatalf("query moved the cursor: rows=%+v next=%d", rows, next)
	}
}

// Method and path shape: only POST at the exact session .../changes/query
// location is accepted; every other verb or shape is a JSON 400, never a
// redirect.
func TestSessionQueryChangesHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/changes/query"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc1/changes/query"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc1/changes/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes/query/"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes/query/extra"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes/queryextra"},
		{http.MethodPost, "/v1/sessions//documents/doc1/changes/query"},
		{http.MethodPost, "/v1/sessions/sess/documents//changes/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changesx/query"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"ids":["a"]}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("content type = %q, want JSON", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("failure redirected to %q", loc)
			}
		})
	}
}

// A session or document literally named "query" keeps its ordinary routes;
// the query keyword is only recognized in the terminal subresource position.
func TestSessionQueryChangesHTTPIdentifiersNamedQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "query")
	postRawChanges(t, h, "query", "dev", `[{"id":"z","payload":1}]`)

	// The session named "query" answers its lookup subresource normally.
	w := querySessionChanges(t, h, "query", "query", `{"ids":["z"]}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("query with identifiers named query = %d %s", w.Code, w.Body.String())
	}
}

// The query verdict derives solely from durable data: after a process restart
// the same request yields the byte-identical body, including found, missing
// and compacted answers.
func TestSessionQueryChangesHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postDocChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := querySessionChanges(t, h, "sess", "doc", `{"ids":["c1","c2","nope"]}`).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	// The durable session row backs the same identity after restart.
	r := httptest.NewRequest(http.MethodPost, sessionQueryPath("sess", "doc"), strings.NewReader(`{"ids":["c1","c2","nope"]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h2.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w.Body.String())
	}
}
