package server

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// seedSnapshots stores snapshots at the given cursors with verbatim raw
// states, after first seeding enough changes for the cursors to exist.
func seedSnapshots(t *testing.T, h http.Handler, doc string, states map[int]string) {
	t.Helper()
	max := 0
	for c := range states {
		if c > max {
			max = c
		}
	}
	seedDoc(t, h, doc, max)
	for cursor, state := range states {
		w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots",
			`{"cursor":`+strconv.Itoa(cursor)+`,"state":`+state+`}`)
		if w.Code != http.StatusOK {
			t.Fatalf("seed snapshot %d status = %d body = %s", cursor, w.Code, w.Body.String())
		}
	}
}

func TestExportSnapshotsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{
		1: "null",
		2: `42`,
		3: `"str"`,
		4: `[1,2]`,
		5: `{"a":1}`,
	})

	// No parameters: from defaults to 0, no upper bound. The body is one
	// compact line with keys ordered snapshots,count and item keys ordered
	// cursor,state; exactly one trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	want := `{"snapshots":[` +
		`{"cursor":1,"state":null},` +
		`{"cursor":2,"state":42},` +
		`{"cursor":3,"state":"str"},` +
		`{"cursor":4,"state":[1,2]},` +
		`{"cursor":5,"state":{"a":1}}` +
		`],"count":5}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// Closed interval includes both endpoints.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?from=2&to=4")
	if w.Code != http.StatusOK {
		t.Fatalf("range status = %d", w.Code)
	}
	want = `{"snapshots":[` +
		`{"cursor":2,"state":42},` +
		`{"cursor":3,"state":"str"},` +
		`{"cursor":4,"state":[1,2]}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("range body = %q\nwant %q", w.Body.String(), want)
	}

	// Degenerate interval matches the single cursor.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?from=3&to=3")
	if got := strings.TrimSpace(w.Body.String()); got != `{"snapshots":[{"cursor":3,"state":"str"}],"count":1}` {
		t.Fatalf("point body = %q", got)
	}

	// Lower bound only (to absent).
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?from=5")
	if got := strings.TrimSpace(w.Body.String()); got != `{"snapshots":[{"cursor":5,"state":{"a":1}}],"count":1}` {
		t.Fatalf("from-only body = %q", got)
	}

	// Explicit empty parameters behave like defaults.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?from=&to=")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 5 {
		t.Fatalf("empty params = %d %s", w.Code, w.Body.String())
	}
}

func TestExportSnapshotsHTTPEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{2: `{"v":1}`, 4: `{"v":2}`})

	// An interval with no snapshot (gap, but within the document) is a
	// successful empty list.
	for _, url := range []string{
		"/v1/documents/doc1/snapshots?from=3&to=3",
		"/v1/documents/doc1/snapshots?from=5",
		"/v1/documents/doc1/snapshots?from=99&to=200",
	} {
		w, _ := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d", url, w.Code)
		}
		if got := w.Body.String(); got != `{"snapshots":[],"count":0}`+"\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}

	// An unknown document is also a successful empty list, not a 404.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/nope/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("unknown doc status = %d", w.Code)
	}
	if got := w.Body.String(); got != `{"snapshots":[],"count":0}`+"\n" {
		t.Fatalf("unknown doc body = %q", got)
	}
}

func TestExportSnapshotsHTTPStateMatchesSingleRead(t *testing.T) {
	h, _ := newTestHandler(t)
	states := map[int]string{1: `{"z":1,"a":2}`, 3: `[true,null,{"n":0.5}]`}
	seedSnapshots(t, h, "doc1", states)

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("export status = %d", w.Code)
	}
	for cursor := range states {
		single, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/"+strconv.Itoa(cursor))
		if single.Code != http.StatusOK {
			t.Fatalf("single %d status = %d", cursor, single.Code)
		}
		// The state text of the single read must appear verbatim in the
		// export body.
		singleState := strings.TrimSpace(single.Body.String())
		// single: {"cursor":N,"state":...}; extract the state suffix.
		prefix := `{"cursor":` + strconv.Itoa(cursor) + `,"state":`
		if !strings.HasPrefix(singleState, prefix) {
			t.Fatalf("single body shape = %q", singleState)
		}
		stateJSON := strings.TrimSuffix(strings.TrimPrefix(singleState, prefix), "}")
		if !strings.Contains(w.Body.String(), `"cursor":`+strconv.Itoa(cursor)+`,"state":`+stateJSON) {
			t.Fatalf("export %q does not contain single-read state %q", w.Body.String(), stateJSON)
		}
	}
}

func TestExportSnapshotsHTTPRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	for _, q := range []string{
		"from=x",
		"from=-1",
		"from=1.5",
		"from=1e3",
		"from=+1",
		"to=x",
		"to=-1",
		"to=1.5",
		"from=3&to=2",
		"from=99999999999999999999",
		"to=99999999999999999999",
	} {
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?"+q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s status = %d, want 400, body = %s", q, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("?%s content type = %q", q, ct)
		}
		if !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("?%s body = %s", q, w.Body.String())
		}
	}

	// Empty documentID is a 400 JSON error, not a redirect.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents//snapshots")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty doc status = %d", w.Code)
	}

	// Zero writes: the rejected requests neither created snapshots nor moved
	// the change cursor.
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}
}

func TestExportSnapshotsHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	for _, c := range []struct{ method, path string }{
		// Non-GET verbs on the collection path.
		{http.MethodPut, "/v1/documents/doc1/snapshots"},
		{http.MethodDelete, "/v1/documents/doc1/snapshots"},
		{http.MethodPatch, "/v1/documents/doc1/snapshots"},
		// Non-GET verbs on the item path.
		{http.MethodPut, "/v1/documents/doc1/snapshots/1"},
		{http.MethodDelete, "/v1/documents/doc1/snapshots/1"},
		{http.MethodPost, "/v1/documents/doc1/snapshots/1"},
		// Missing/extra path segments.
		{http.MethodGet, "/v1/documents/doc1/snapshots/"},
		{http.MethodGet, "/v1/documents/doc1/snapshots/1/"},
		{http.MethodGet, "/v1/documents/doc1/snapshots/1/extra"},
	} {
		w, _ := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400, body = %q", c.method, c.path, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content type = %q, want JSON; body = %q", c.method, c.path, ct, w.Body.String())
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") || strings.Contains(w.Body.String(), "Method Not Allowed") {
			t.Fatalf("%s %s leaked non-JSON/plain body: %q", c.method, c.path, w.Body.String())
		}
	}

	// POST on the collection remains the existing create endpoint.
	w, _ := doRequest(t, h, http.MethodPost, "/v1/documents/doc1/snapshots")
	if w.Code == http.StatusBadRequest && strings.Contains(w.Body.String(), "method is not allowed") {
		t.Fatalf("POST on collection must remain the create endpoint, got %s", w.Body.String())
	}
}

func TestExportSnapshotsHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"v":1}`, 2: `{"v":2}`})

	_, before := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if before["nextCursor"].(float64) != 2 {
		t.Fatalf("seed nextCursor = %v", before["nextCursor"])
	}

	// Repeated exports change nothing.
	for i := 0; i < 3; i++ {
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots")
		if w.Code != http.StatusOK {
			t.Fatalf("export %d = %d", i, w.Code)
		}
	}

	_, after := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if after["nextCursor"].(float64) != 2 {
		t.Fatalf("nextCursor moved to %v after exports", after["nextCursor"])
	}
	// The snapshot set still contains exactly the two seeded snapshots.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots")
	if strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("snapshot set changed after exports: %s", w.Body.String())
	}
}

func TestExportSnapshotsHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"a":1}`, 2: `[1,2]`, 3: `null`})
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots?from=1&to=3")
	if w.Code != http.StatusOK {
		t.Fatalf("export before restart = %d", w.Code)
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

	w2, _ := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/snapshots?from=1&to=3")
	if w2.Code != http.StatusOK {
		t.Fatalf("export after restart = %d", w2.Code)
	}
	if string(w2.Body.Bytes()) != string(before) {
		t.Fatalf("export body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}
}
