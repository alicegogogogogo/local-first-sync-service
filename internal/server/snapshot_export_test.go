package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// rawGet issues a GET and returns the raw recorder so byte-level body and
// header assertions can be made.
func rawGet(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

// putSnapshotHTTP creates a snapshot directly through the documented POST
// surface, failing the test on anything but a 200.
func putSnapshotHTTP(t *testing.T, h http.Handler, doc string, cursor int, state string) {
	t.Helper()
	body := `{"cursor":` + strconv.Itoa(cursor) + `,"state":` + state + `}`
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots", body)
	if w.Code != http.StatusOK {
		t.Fatalf("put snapshot cursor %d: status = %d body = %s", cursor, w.Code, w.Body.String())
	}
}

func TestSnapshotExportHTTPShapes(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 5)
	putSnapshotHTTP(t, h, "doc1", 1, `null`)
	putSnapshotHTTP(t, h, "doc1", 2, `12.50`)
	putSnapshotHTTP(t, h, "doc1", 3, `"str"`)
	putSnapshotHTTP(t, h, "doc1", 4, `[1,2]`)
	putSnapshotHTTP(t, h, "doc1", 5, `{"b":2,"a":1}`)

	w := rawGet(t, h, "/v1/documents/doc1/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := "{\"snapshots\":[" +
		"{\"cursor\":1,\"state\":null}," +
		"{\"cursor\":2,\"state\":12.50}," +
		"{\"cursor\":3,\"state\":\"str\"}," +
		"{\"cursor\":4,\"state\":[1,2]}," +
		"{\"cursor\":5,\"state\":{\"b\":2,\"a\":1}}" +
		"],\"count\":5}\n"
	if w.Body.String() != want {
		t.Fatalf("export body = %q, want %q", w.Body.String(), want)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}

	// Closed interval [2,4]: endpoints inclusive, ascending, count matches.
	w = rawGet(t, h, "/v1/documents/doc1/snapshots?from=2&to=4")
	if got := w.Body.String(); got != "{\"snapshots\":[{\"cursor\":2,\"state\":12.50},{\"cursor\":3,\"state\":\"str\"},{\"cursor\":4,\"state\":[1,2]}],\"count\":3}\n" {
		t.Fatalf("2..4 body = %q", got)
	}

	// from alone runs to the end.
	w = rawGet(t, h, "/v1/documents/doc1/snapshots?from=5")
	if got := w.Body.String(); got != "{\"snapshots\":[{\"cursor\":5,\"state\":{\"b\":2,\"a\":1}}],\"count\":1}\n" {
		t.Fatalf("from=5 body = %q", got)
	}

	// A gap in snapshots is skipped silently.
	w = rawGet(t, h, "/v1/documents/doc1/snapshots?to=1")
	if got := w.Body.String(); got != "{\"snapshots\":[{\"cursor\":1,\"state\":null}],\"count\":1}\n" {
		t.Fatalf("to=1 body = %q", got)
	}
}

func TestSnapshotExportHTTPEmptySuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)
	putSnapshotHTTP(t, h, "doc1", 1, `{}`)

	for _, url := range []string{
		"/v1/documents/ghost/snapshots",       // unknown document
		"/v1/documents/doc1/snapshots?from=9", // interval past every snapshot
		"/v1/documents/doc1/snapshots?from=2", // known doc, no snapshot at 2
		"/v1/documents/doc1/snapshots?to=0",   // snapshots start at cursor 1
		"/v1/documents/doc1/snapshots?from=0&to=0",
	} {
		w := rawGet(t, h, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", url, w.Code)
		}
		if got := w.Body.String(); got != "{\"snapshots\":[],\"count\":0}\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}
}

func TestSnapshotExportHTTPRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)
	putSnapshotHTTP(t, h, "doc1", 1, `{}`)

	for _, q := range []string{
		"?from=abc",
		"?from=-1",
		"?from=1.5",
		"?from=1e2",
		"?to=abc",
		"?to=-2",
		"?from=5&to=4", // to smaller than from
	} {
		w := rawGet(t, h, "/v1/documents/doc1/snapshots"+q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %s", q, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s content type = %q", q, ct)
		}
		if !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("%s body = %s", q, w.Body.String())
		}
	}

	// An empty value is treated as omitted (the same convention after/limit
	// follow elsewhere): from defaults to 0, to stays open.
	w := rawGet(t, h, "/v1/documents/doc1/snapshots?from=&to=")
	if w.Code != http.StatusOK {
		t.Fatalf("empty-value query status = %d, want 200, body = %s", w.Code, w.Body.String())
	}

	// Zero-write: the snapshot at cursor 1 is untouched and no new rows exist.
	w = rawGet(t, h, "/v1/documents/doc1/snapshots")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"count":1`) {
		t.Fatalf("zero-write violated: %s", w.Body.String())
	}
}

func TestSnapshotExportHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 1)

	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/documents/doc1/snapshots"},
		{http.MethodPut, "/v1/documents/doc1/snapshots"},
		{http.MethodDelete, "/v1/documents/doc1/snapshots"},
		{http.MethodGet, "/v1/documents/doc1/snapshots/"},        // trailing slash
		{http.MethodGet, "/v1/documents/doc1/snapshots/1/extra"}, // extra segment
		{http.MethodGet, "/v1/documents//snapshots"},             // empty documentID
		{http.MethodPost, "/v1/documents//snapshots"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("body = %s", w.Body.String())
			}
		})
	}

	// No redirects and no HTML, ever.
	for _, path := range []string{
		"/v1/documents/doc1/snapshots/",
		"/v1/documents//snapshots",
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") ||
			strings.Contains(w.Header().Get("Location"), "/") {
			t.Fatalf("%s redirected or returned HTML: %q location=%q", path, w.Body.String(), w.Header().Get("Location"))
		}
	}
}

func TestSnapshotExportHTTPMatchesSingleRead(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 3)
	putSnapshotHTTP(t, h, "doc1", 2, `{"z":[1,{"y":2}],"a":null}`)

	single := rawGet(t, h, "/v1/documents/doc1/snapshots/2")
	if single.Code != http.StatusOK {
		t.Fatalf("single read status = %d", single.Code)
	}
	bulk := rawGet(t, h, "/v1/documents/doc1/snapshots?from=2&to=2")
	if bulk.Code != http.StatusOK {
		t.Fatalf("export status = %d", bulk.Code)
	}
	// The single read is {"cursor":2,"state":...}; the export element carries
	// exactly the same cursor/state content.
	const prefix = "{\"snapshots\":["
	const suffix = "],\"count\":1}\n"
	got := bulk.Body.String()
	if !strings.HasPrefix(got, prefix) || !strings.HasSuffix(got, suffix) {
		t.Fatalf("export shape = %q", got)
	}
	if element := got[len(prefix) : len(got)-len(suffix)]; element != strings.TrimSuffix(single.Body.String(), "\n") {
		t.Fatalf("element %q != single read %q", element, single.Body.String())
	}
}

func TestSnapshotExportHTTPPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedDoc(t, h, "doc1", 3)
	putSnapshotHTTP(t, h, "doc1", 1, `{"v":1}`)
	putSnapshotHTTP(t, h, "doc1", 3, `{"v":3}`)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })

	w := rawGet(t, NewHandler(s2), "/v1/documents/doc1/snapshots")
	want := "{\"snapshots\":[{\"cursor\":1,\"state\":{\"v\":1}},{\"cursor\":3,\"state\":{\"v\":3}}],\"count\":2}\n"
	if w.Body.String() != want {
		t.Fatalf("after restart body = %q, want %q", w.Body.String(), want)
	}
}
