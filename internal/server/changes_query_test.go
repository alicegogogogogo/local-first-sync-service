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

// queryChanges sends a raw POST to the batch lookup and returns the recorder.
func queryChanges(t *testing.T, h http.Handler, doc, rawBody string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/"+doc+"/changes/query", strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestQueryChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "doc1", "dev", `[
		{"id":"a","payload":{"k":"v"}},
		{"id":"b","payload":42},
		{"id":"c","payload":null}
	]`)

	// Hits are answered in the order asked, each carrying source device,
	// verbatim saved payload and cursor. The body is one compact line with
	// top-level keys results,count and item keys id,status,deviceId,payload,cursor.
	w := queryChanges(t, h, "doc1", `{"deviceId":"dev","ids":["c","a","nope","b"]}`)
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

// A payload is presented exactly as the JSON content saved at commit time:
// key reordering and whitespace are normalized by the store, but the value is
// otherwise byte-faithful.
func TestQueryChangesHTTPPayloadVerbatim(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"x","payload":{"arr":[1,true,null,"s"],"nested":{"z":1,"a":[{}]}}}]`)

	w := queryChanges(t, h, "doc1", `{"deviceId":"dev","ids":["x"]}`)
	want := `{"results":[{"id":"x","status":"found","deviceId":"dev","payload":{"arr":[1,true,null,"s"],"nested":{"z":1,"a":[{}]}},"cursor":1}],"count":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every queried id appears in the answer, including unknown ones: missing is
// a normal answer with no content, not an error, and count matches the array
// length even when the document has never existed.
func TestQueryChangesHTTPMissingAndUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	for _, doc := range []string{"doc1", "never-heard-of-it"} {
		w := queryChanges(t, h, doc, `{"deviceId":"dev","ids":["x","y"]}`)
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
// device and no payload are restored, while the surviving online tail is
// still found with full content.
func TestQueryChangesHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = queryChanges(t, h, "doc", `{"deviceId":"dev-1","ids":["c1","c2","c3","gone-never"]}`)
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
// before the device even exists.
func TestQueryChangesHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	url := "/v1/documents/doc1/changes/query"

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
		{"wrong content type", "text/plain", `{"deviceId":"dev","ids":["a"]}`},
		{"missing content type", "", `{"deviceId":"dev","ids":["a"]}`},
		{"content type with json suffix", "application/vnd.api+json", `{"deviceId":"dev","ids":["a"]}`},
		{"invalid json", "application/json", `{"deviceId":"dev","ids":[`},
		{"trailing content", "application/json", `{"deviceId":"dev","ids":["a"]} junk`},
		{"second json value", "application/json", `{"deviceId":"dev","ids":["a"]}{}`},
		{"missing ids", "application/json", `{"deviceId":"dev"}`},
		{"null ids", "application/json", `{"deviceId":"dev","ids":null}`},
		{"empty ids", "application/json", `{"deviceId":"dev","ids":[]}`},
		{"empty id element", "application/json", `{"deviceId":"dev","ids":["a",""]}`},
		{"non-string id element", "application/json", `{"deviceId":"dev","ids":["a",1]}`},
		{"ids not an array", "application/json", `{"deviceId":"dev","ids":"a"}`},
		{"duplicate ids", "application/json", `{"deviceId":"dev","ids":["a","a"]}`},
		{"missing deviceId", "application/json", `{"ids":["a"]}`},
		{"empty deviceId", "application/json", `{"deviceId":"","ids":["a"]}`},
		{"non-string deviceId", "application/json", `{"deviceId":7,"ids":["a"]}`},
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

	// A malformed body against an unregistered device is still a 400: shape
	// validation precedes device existence.
	w := send("application/json", `{"deviceId":"ghost","ids":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-existence = %d body = %s", w.Code, w.Body.String())
	}
}

// Device existence and permission are enforced after shape validation, and a
// failure exposes no change content.
func TestQueryChangesHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	// Unregistered device: 404 with an error only.
	w := queryChanges(t, h, "doc1", `{"deviceId":"ghost","ids":["a"]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("404 leaked change content: %s", w.Body.String())
	}

	// Revoked permission: 403 with an error only.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = queryChanges(t, h, "doc1", `{"deviceId":"dev","ids":["a"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("403 leaked change content: %s", w.Body.String())
	}
}

// The lookup is read-only: it allocates no cursor, so a later commit receives
// the cursor it would have received without the queries, and it neither wakes
// a parked long poll nor a push subscriber.
func TestQueryChangesHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	// A parked long poll with a short wait must time out despite queries
	// hitting the same document.
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
		if w := queryChanges(t, h, "doc1", `{"deviceId":"dev","ids":["a","missing"]}`); w.Code != http.StatusOK {
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

// Method and path shape: only POST at the exact .../changes/query location is
// accepted; every other verb or shape is a JSON 400, never a redirect.
func TestQueryChangesHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/documents/doc1/changes/query"},
		{http.MethodPut, "/v1/documents/doc1/changes/query"},
		{http.MethodDelete, "/v1/documents/doc1/changes/query"},
		{http.MethodPost, "/v1/documents/doc1/changes/query/"},
		{http.MethodPost, "/v1/documents/doc1/changes/query/extra"},
		{http.MethodPost, "/v1/documents/doc1/changes/queryextra"},
		{http.MethodPost, "/v1/documents//changes/query"},
		{http.MethodPost, "/v1/documents/doc1/changesx/query"},
		// The batch lookup is document-level only; the same segment under a
		// session change collection is not an endpoint there.
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes/query"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"deviceId":"dev","ids":["a"]}`))
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

// A document literally named "query" keeps its ordinary change routes; the
// query keyword is only recognized in the terminal subresource position.
func TestQueryChangesHTTPDocumentNamedQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "query", "dev", `[{"id":"z","payload":1}]`)

	// Ordinary paged read on the document named "query".
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/query/changes")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("paged read on doc named query = %d %s", w.Code, w.Body.String())
	}
	// The lookup subresource of that document answers normally too.
	w = queryChanges(t, h, "query", `{"deviceId":"dev","ids":["z"]}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("query on doc named query = %d %s", w.Code, w.Body.String())
	}
}

// The query verdict derives solely from durable data: after a process
// restart the same request yields the byte-identical body, including found,
// missing and compacted answers.
func TestQueryChangesHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := queryChanges(t, h, "doc", `{"deviceId":"dev-1","ids":["c1","c2","nope"]}`).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/changes/query", strings.NewReader(`{"deviceId":"dev-1","ids":["c1","c2","nope"]}`))
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
