package server

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func TestExportChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 5)

	// No parameters: from defaults to 0, no upper bound. The body is one
	// compact line with keys ordered changes,count and record keys ordered
	// id,deviceId,payload,cursor; exactly one trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	want := `{"changes":[` +
		`{"id":"c1","deviceId":"dev","payload":{"n":1},"cursor":1},` +
		`{"id":"c2","deviceId":"dev","payload":{"n":2},"cursor":2},` +
		`{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3},` +
		`{"id":"c4","deviceId":"dev","payload":{"n":4},"cursor":4},` +
		`{"id":"c5","deviceId":"dev","payload":{"n":5},"cursor":5}` +
		`],"count":5}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// Closed interval includes both endpoints.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=2&to=4")
	if w.Code != http.StatusOK {
		t.Fatalf("range status = %d", w.Code)
	}
	want = `{"changes":[` +
		`{"id":"c2","deviceId":"dev","payload":{"n":2},"cursor":2},` +
		`{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3},` +
		`{"id":"c4","deviceId":"dev","payload":{"n":4},"cursor":4}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("range body = %q\nwant %q", w.Body.String(), want)
	}

	// Degenerate interval matches the single cursor.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=3&to=3")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3}],"count":1}` {
		t.Fatalf("point body = %q", got)
	}

	// Lower bound only (to absent).
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=4")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c4","deviceId":"dev","payload":{"n":4},"cursor":4},{"id":"c5","deviceId":"dev","payload":{"n":5},"cursor":5}],"count":2}` {
		t.Fatalf("from-only body = %q", got)
	}

	// Explicit empty parameters behave like defaults.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=&to=")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 5 {
		t.Fatalf("empty params = %d %s", w.Code, w.Body.String())
	}
}

func TestExportChangesHTTPEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)

	// An interval with no change (beyond the document's cursor) is a
	// successful empty list.
	for _, url := range []string{
		"/v1/documents/doc1/changes/export?from=3&to=3",
		"/v1/documents/doc1/changes/export?from=5",
		"/v1/documents/doc1/changes/export?from=99&to=200",
	} {
		w, _ := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d", url, w.Code)
		}
		if got := w.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}

	// An unknown document is also a successful empty list, not a 404.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/nope/changes/export")
	if w.Code != http.StatusOK {
		t.Fatalf("unknown doc status = %d", w.Code)
	}
	if got := w.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
		t.Fatalf("unknown doc body = %q", got)
	}
}

// The records the export emits at each cursor are byte-identical to the ones
// the paged read emits at the same cursors.
func TestExportChangesHTTPMatchesPagedRead(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 4)

	paged, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?after=0&limit=100")
	pagedLine := strings.TrimSpace(paged.Body.String())
	if !strings.HasPrefix(pagedLine, `{"changes":[`) || !strings.HasSuffix(pagedLine, `],"nextCursor":4}`) {
		t.Fatalf("paged body shape = %q", pagedLine)
	}
	pagedRecords := strings.TrimSuffix(strings.TrimPrefix(pagedLine, `{"changes":[`), `],"nextCursor":4}`)

	exported, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export")
	exportLine := strings.TrimSpace(exported.Body.String())
	if !strings.HasPrefix(exportLine, `{"changes":[`) || !strings.HasSuffix(exportLine, `],"count":4}`) {
		t.Fatalf("export body shape = %q", exportLine)
	}
	exportRecords := strings.TrimSuffix(strings.TrimPrefix(exportLine, `{"changes":[`), `],"count":4}`)

	if exportRecords != pagedRecords {
		t.Fatalf("records differ:\npaged  %q\nexport %q", pagedRecords, exportRecords)
	}
}

func TestExportChangesHTTPRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 1)

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
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?"+q)
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
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents//changes/export")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty doc status = %d", w.Code)
	}

	// Zero writes: the rejected requests neither created changes nor moved
	// the change cursor.
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}
}

func TestExportChangesHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 1)

	for _, c := range []struct{ method, path string }{
		// Non-GET verbs on the export path.
		{http.MethodPost, "/v1/documents/doc1/changes/export"},
		{http.MethodPut, "/v1/documents/doc1/changes/export"},
		{http.MethodDelete, "/v1/documents/doc1/changes/export"},
		{http.MethodPatch, "/v1/documents/doc1/changes/export"},
		// Missing/extra path segments.
		{http.MethodGet, "/v1/documents/doc1/changes/export/"},
		{http.MethodGet, "/v1/documents/doc1/changes/export/x"},
		{http.MethodGet, "/v1/documents/doc1/export"},
		{http.MethodGet, "/v1/documents/doc1/export/x"},
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

	// A document literally named "export" keeps its ordinary routes.
	w, _ := postJSON(t, h, "/v1/documents/export/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("changes on document named export = %d %s", w.Code, w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/export/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("document named export changes read = %d %v", w.Code, body)
	}
}

func TestExportChangesHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)

	_, before := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if before["nextCursor"].(float64) != 2 {
		t.Fatalf("seed nextCursor = %v", before["nextCursor"])
	}

	// Repeated exports change nothing.
	for i := 0; i < 3; i++ {
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export")
		if w.Code != http.StatusOK {
			t.Fatalf("export %d = %d", i, w.Code)
		}
	}

	_, after := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if after["nextCursor"].(float64) != 2 {
		t.Fatalf("nextCursor moved to %v after exports", after["nextCursor"])
	}
	if got := len(after["changes"].([]any)); got != 2 {
		t.Fatalf("change log grew after exports: %d records", got)
	}
}

// Changes that compaction moved out of the online log are absent from the
// export; an interval fully inside the trimmed range exports empty.
func TestExportChangesHTTPCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	seedDoc(t, h, "doc1", 4)

	w, _ := postJSON(t, h, "/v1/documents/doc1/snapshots", `{"cursor":2,"state":{"s":1}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d body = %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes/compact", `{"deviceId":"dev"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("compact status = %d body = %s", w.Code, w.Body.String())
	}

	// The trimmed cursors 1..2 are gone from the export; the online 3..4
	// remain, in order.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export")
	want := `{"changes":[` +
		`{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3},` +
		`{"id":"c4","deviceId":"dev","payload":{"n":4},"cursor":4}` +
		`],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("post-compact export = %q\nwant %q", w.Body.String(), want)
	}

	// An interval fully inside the trimmed range is a successful empty list.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=1&to=2")
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed range status = %d", w.Code)
	}
	if got := w.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
		t.Fatalf("trimmed range body = %q", got)
	}
}

func TestExportChangesHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedDoc(t, h, "doc1", 3)
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/export?from=1&to=3")
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

	w2, _ := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes/export?from=1&to=3")
	if w2.Code != http.StatusOK {
		t.Fatalf("export after restart = %d", w2.Code)
	}
	if string(w2.Body.Bytes()) != string(before) {
		t.Fatalf("export body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}
}
