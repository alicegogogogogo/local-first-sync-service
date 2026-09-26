package server

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// postRawChanges posts one batch from device with the given raw change bodies
// of the form {"id":...,"payload":...}.
func postRawChanges(t *testing.T, h http.Handler, doc, device string, rawChanges string) {
	t.Helper()
	body := `{"deviceId":"` + device + `","changes":` + rawChanges + `}`
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", body)
	if w.Code != http.StatusOK {
		t.Fatalf("post changes = %d %s", w.Code, w.Body.String())
	}
}

func TestExportChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 3)

	// No parameters: from defaults to 0, no upper bound. The body is one
	// compact line with top-level keys ordered changes,count and item keys
	// ordered id,deviceId,payload,cursor; exactly one trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	want := `{"changes":[` +
		`{"id":"c1","deviceId":"dev","payload":{"n":1},"cursor":1},` +
		`{"id":"c2","deviceId":"dev","payload":{"n":2},"cursor":2},` +
		`{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// No export parameters at all is still the paged read shape.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("paged read status = %d", w.Code)
	}
	if body["nextCursor"] == nil || body["count"] != nil {
		t.Fatalf("paged read shape changed: %s", w.Body.String())
	}

	// Closed interval includes both endpoints.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=2&to=2")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c2","deviceId":"dev","payload":{"n":2},"cursor":2}],"count":1}` {
		t.Fatalf("point body = %q", got)
	}

	// Lower bound only (to absent).
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=3")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3}],"count":1}` {
		t.Fatalf("from-only body = %q", got)
	}

	// to alone applies the lower default of 0.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?to=1")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c1","deviceId":"dev","payload":{"n":1},"cursor":1}],"count":1}` {
		t.Fatalf("to-only body = %q", got)
	}

	// Explicit empty parameters behave like defaults.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=&to=")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 3 {
		t.Fatalf("empty params = %d %s", w.Code, w.Body.String())
	}
}

func TestExportChangesHTTPPayloadShapes(t *testing.T) {
	h, _ := newTestHandler(t)
	postRawChanges(t, h, "doc1", "dev-9", `[
		{"id":"x1","payload":null},
		{"id":"x2","payload":42},
		{"id":"x3","payload":"str"},
		{"id":"x4","payload":[1,true,null]}
	]`)

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
	want := `{"changes":[` +
		`{"id":"x1","deviceId":"dev-9","payload":null,"cursor":1},` +
		`{"id":"x2","deviceId":"dev-9","payload":42,"cursor":2},` +
		`{"id":"x3","deviceId":"dev-9","payload":"str","cursor":3},` +
		`{"id":"x4","deviceId":"dev-9","payload":[1,true,null],"cursor":4}` +
		`],"count":4}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

func TestExportChangesHTTPMultipleDevices(t *testing.T) {
	h, _ := newTestHandler(t)
	postRawChanges(t, h, "doc1", "dev-a", `[{"id":"a1","payload":{"who":"a"}}]`)
	postRawChanges(t, h, "doc1", "dev-b", `[{"id":"b1","payload":{"who":"b"}},{"id":"b2","payload":{"who":"b2"}}]`)

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
	want := `{"changes":[` +
		`{"id":"a1","deviceId":"dev-a","payload":{"who":"a"},"cursor":1},` +
		`{"id":"b1","deviceId":"dev-b","payload":{"who":"b"},"cursor":2},` +
		`{"id":"b2","deviceId":"dev-b","payload":{"who":"b2"},"cursor":3}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

func TestExportChangesHTTPEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)

	for _, url := range []string{
		"/v1/documents/doc1/changes?from=3&to=3",
		"/v1/documents/doc1/changes?from=9",
		"/v1/documents/doc1/changes?from=99&to=200",
		"/v1/documents/nope/changes?from=0",
	} {
		w, _ := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d", url, w.Code)
		}
		if got := w.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}
}

func TestExportChangesHTTPCompacted(t *testing.T) {
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

	// Unbounded: online tail only.
	w2, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?from=0")
	want := `{"changes":[` +
		`{"id":"c3","deviceId":"dev-1","payload":{"n":3},"cursor":3},` +
		`{"id":"c4","deviceId":"dev-1","payload":{"n":4},"cursor":4}` +
		`],"count":2}` + "\n"
	if w2.Body.String() != want {
		t.Fatalf("tail body = %q\nwant %q", w2.Body.String(), want)
	}

	// Interval entirely inside the trimmed region: empty success.
	w2, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?from=0&to=2")
	if got := w2.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
		t.Fatalf("trimmed interval body = %q", got)
	}

	// Interval straddling the boundary: online rows only.
	w2, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?from=1&to=3")
	if got := strings.TrimSpace(w2.Body.String()); got != `{"changes":[{"id":"c3","deviceId":"dev-1","payload":{"n":3},"cursor":3}],"count":1}` {
		t.Fatalf("straddling body = %q", got)
	}
}

func TestExportChangesHTTPMatchesPagedRead(t *testing.T) {
	h, _ := newTestHandler(t)
	postRawChanges(t, h, "doc1", "dev-a", `[
		{"id":"a1","payload":{"z":1,"a":2}},
		{"id":"a2","payload":[true,null,0.5]}
	]`)

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
	if w.Code != http.StatusOK {
		t.Fatalf("export = %d", w.Code)
	}
	for cursor, id := range map[int]string{1: "a1", 2: "a2"} {
		rec, page := doRequest(t, h, http.MethodGet,
			"/v1/documents/doc1/changes?after="+strconv.Itoa(cursor-1)+"&limit=1")
		if rec.Code != http.StatusOK {
			t.Fatalf("paged after=%d = %d", cursor-1, rec.Code)
		}
		rows := page["changes"].([]any)
		if len(rows) != 1 {
			t.Fatalf("paged rows = %v", rows)
		}
		paged := rows[0].(map[string]any)
		export := w.Body.String()
		needle := `"id":"` + id + `"`
		if !strings.Contains(export, needle) {
			t.Fatalf("export missing %s: %s", id, export)
		}
		if paged["id"] != id || int(paged["cursor"].(float64)) != cursor {
			t.Fatalf("paged row mismatch: %+v", paged)
		}
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
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?"+q)
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
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents//changes?from=0")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("empty doc status = %d", w.Code)
	}

	// Zero writes: the rejected requests moved the change cursor nowhere.
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}
}

func TestExportChangesHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 1)

	for _, c := range []struct{ method, path string }{
		{http.MethodPut, "/v1/documents/doc1/changes?from=0"},
		{http.MethodDelete, "/v1/documents/doc1/changes?from=0"},
		{http.MethodPatch, "/v1/documents/doc1/changes?from=0"},
		{http.MethodOptions, "/v1/documents/doc1/changes?from=0"},
		{http.MethodGet, "/v1/documents/doc1/changes/?from=0"},
		{http.MethodGet, "/v1/documents/doc1/changes/extra?from=0"},
		{http.MethodGet, "/v1/documents/doc1/changes/subscribe?from=0"},
		{http.MethodPost, "/v1/documents/doc1/changes/extra"},
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

	// POST on the collection remains the existing commit endpoint.
	w, _ := postJSON(t, h, "/v1/documents/doc1/changes?from=0",
		map[string]any{"deviceId": "dev", "changes": []any{
			map[string]any{"id": "c2", "payload": map[string]any{"n": 2}},
		}})
	if w.Code != http.StatusOK {
		t.Fatalf("POST collection with export query = %d %s", w.Code, w.Body.String())
	}
}

func TestExportChangesHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDoc(t, h, "doc1", 2)

	for i := 0; i < 3; i++ {
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
		if w.Code != http.StatusOK {
			t.Fatalf("export %d = %d", i, w.Code)
		}
	}

	_, after := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if after["nextCursor"].(float64) != 2 {
		t.Fatalf("nextCursor moved to %v after exports", after["nextCursor"])
	}
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=0")
	if strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("change set changed after exports: %s", w.Body.String())
	}
}

func TestExportChangesHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export-changes-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedDoc(t, h, "doc1", 3)
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?from=1&to=3")
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

	w2, _ := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes?from=1&to=3")
	if w2.Code != http.StatusOK {
		t.Fatalf("export after restart = %d", w2.Code)
	}
	if string(w2.Body.Bytes()) != string(before) {
		t.Fatalf("export body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}
}

// A document literally named "permissions" gets the same permission path
// shape treatment as any other document; the missing-segment and trailing or
// extra segment shapes are all a JSON 400, never an unknown-document 404.
func TestPermissionPathShapedLikeDocumentNamedPermissions(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	for _, c := range []struct{ method, path string }{
		// Resource with a missing documentID segment.
		{http.MethodGet, "/v1/documents/permissions"},
		{http.MethodGet, "/v1/documents/permissions/"},
		// The document is itself named "permissions": the resource is then
		// /v1/documents/permissions/permissions, and its malformed shapes must
		// answer 400 JSON rather than the old unknown-document 404.
		{http.MethodGet, "/v1/documents/permissions/permissions/"},
		{http.MethodGet, "/v1/documents/permissions/permissions/extra"},
		{http.MethodGet, "/v1/documents/permissions/permissions/extra/more"},
		{http.MethodPost, "/v1/documents/permissions/permissions/extra"},
		{http.MethodDelete, "/v1/documents/permissions/permissions"},
		{http.MethodPut, "/v1/documents/permissions/permissions"},
	} {
		w, _ := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, body = %q", c.method, c.path, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content type = %q", c.method, c.path, ct)
		}
		if !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("%s %s body = %q", c.method, c.path, w.Body.String())
		}
	}

	// The well-formed resource for the document named "permissions" still
	// answers 200 (unknown document is not an error for the ledger).
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/permissions/permissions")
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"permissions":[{"deviceId":"dev-1","authorized":true}],"count":1}` {
		t.Fatalf("ledger for document named permissions = %d %s", w.Code, w.Body.String())
	}
}
