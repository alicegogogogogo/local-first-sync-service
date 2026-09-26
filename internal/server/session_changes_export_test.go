package server

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionChangesPath is the session-scoped change collection path shared by
// the paged read and the interval export.
func sessionChangesPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/changes"
}

func TestSessionExportChangesHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 3)

	// from present: the export answers with the changes,count shape — one
	// compact line, item keys ordered id,deviceId,payload,cursor, exactly one
	// trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
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
	w, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1"))
	if w.Code != http.StatusOK {
		t.Fatalf("paged read status = %d", w.Code)
	}
	if body["nextCursor"] == nil || body["count"] != nil {
		t.Fatalf("paged read shape changed: %s", w.Body.String())
	}

	// Closed interval includes both endpoints.
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=2&to=2")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c2","deviceId":"dev","payload":{"n":2},"cursor":2}],"count":1}` {
		t.Fatalf("point body = %q", got)
	}

	// Lower bound only (to absent).
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=3")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c3","deviceId":"dev","payload":{"n":3},"cursor":3}],"count":1}` {
		t.Fatalf("from-only body = %q", got)
	}

	// to alone applies the lower default of 0.
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?to=1")
	if got := strings.TrimSpace(w.Body.String()); got != `{"changes":[{"id":"c1","deviceId":"dev","payload":{"n":1},"cursor":1}],"count":1}` {
		t.Fatalf("to-only body = %q", got)
	}

	// Explicit empty parameters behave like defaults.
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=&to=")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 3 {
		t.Fatalf("empty params = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportChangesHTTPPayloadShapes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-9", "sess")
	postRawChanges(t, h, "doc1", "dev-9", `[
		{"id":"x1","payload":null},
		{"id":"x2","payload":42},
		{"id":"x3","payload":"str"},
		{"id":"x4","payload":[1,true,null]}
	]`)

	w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
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

func TestSessionExportChangesHTTPEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2)

	// An interval without records — and a document unknown to the log — are
	// successful empty exports, not errors.
	for _, url := range []string{
		sessionChangesPath("sess", "doc1") + "?from=3&to=3",
		sessionChangesPath("sess", "doc1") + "?from=9",
		sessionChangesPath("sess", "doc1") + "?from=99&to=200",
		sessionChangesPath("sess", "ghost") + "?from=0",
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

func TestSessionExportChangesHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	postDocChanges(t, h, "doc", "dev-1", 4)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// Unbounded: online tail only.
	w2, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?from=0")
	want := `{"changes":[` +
		`{"id":"c3","deviceId":"dev-1","payload":{"n":3},"cursor":3},` +
		`{"id":"c4","deviceId":"dev-1","payload":{"n":4},"cursor":4}` +
		`],"count":2}` + "\n"
	if w2.Body.String() != want {
		t.Fatalf("tail body = %q\nwant %q", w2.Body.String(), want)
	}

	// Interval entirely inside the trimmed region: empty success.
	w2, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?from=0&to=2")
	if got := w2.Body.String(); got != `{"changes":[],"count":0}`+"\n" {
		t.Fatalf("trimmed interval body = %q", got)
	}

	// Interval straddling the boundary: online rows only.
	w2, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?from=1&to=3")
	if got := strings.TrimSpace(w2.Body.String()); got != `{"changes":[{"id":"c3","deviceId":"dev-1","payload":{"n":3},"cursor":3}],"count":1}` {
		t.Fatalf("straddling body = %q", got)
	}
}

func TestSessionExportChangesHTTPMatchesPagedRead(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-a", "sess")
	postRawChanges(t, h, "doc1", "dev-a", `[
		{"id":"a1","payload":{"z":1,"a":2}},
		{"id":"a2","payload":[true,null,0.5]}
	]`)

	w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusOK {
		t.Fatalf("export = %d", w.Code)
	}
	for cursor, id := range map[int]string{1: "a1", 2: "a2"} {
		rec, page := doRequest(t, h, http.MethodGet,
			sessionChangesPath("sess", "doc1")+"?after="+strconv.Itoa(cursor-1)+"&limit=1")
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

func TestSessionExportChangesHTTPRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
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
		w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?"+q)
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

	// Shape errors take precedence over the session lookup: an illegal
	// interval against a never-created session is still a 400.
	w, body := doRequest(t, h, http.MethodGet, sessionChangesPath("ghost", "doc1")+"?from=x")
	if w.Code != http.StatusBadRequest || body["error"] == nil {
		t.Fatalf("bad param + missing session = %d %v", w.Code, body)
	}

	// Empty identifiers are a 400 JSON error, not a redirect.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/changes?from=0",
		"/v1/sessions/sess/documents//changes?from=0",
	} {
		w, _ := doRequest(t, h, http.MethodGet, p)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", p, w.Code)
		}
		assertJSONError(t, w)
	}

	// Zero writes: the rejected requests moved the change cursor nowhere.
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}
}

func TestSessionExportChangesHTTPSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	seedDoc(t, h, "doc1", 2)

	// Never-created session -> 404 with no change content.
	w, body := doRequest(t, h, http.MethodGet, sessionChangesPath("ghost", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["changes"] != nil || body["count"] != nil {
		t.Fatalf("404 leaked export content: %s", w.Body.String())
	}

	// Delete the session -> exports now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}
}

func TestSessionExportChangesHTTPPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	seedDoc(t, h, "doc1", 2)

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 with no change content.
	w, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["changes"] != nil || body["count"] != nil {
		t.Fatalf("403 leaked export content: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("ghost", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Re-grant restores the export.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("re-granted export = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportChangesHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 1)

	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes?from=0"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc1/changes?from=0"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc1/changes?from=0"},
		{http.MethodPatch, "/v1/sessions/sess/documents/doc1/changes?from=0"},
		{http.MethodOptions, "/v1/sessions/sess/documents/doc1/changes?from=0"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/changes/?from=0"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/changes/extra?from=0"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/changes/extra"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/changes/extra"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/changes/extra/more?from=0"},
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

	// Identifiers named like the keyword keep their ordinary routes: a
	// document literally named "changes" exports through the same shape.
	postRawChanges(t, h, "changes", "dev", `[{"id":"k1","payload":{"k":1}}]`)
	w, _ := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/changes/changes?from=0")
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"changes":[{"id":"k1","deviceId":"dev","payload":{"k":1},"cursor":1}],"count":1}` {
		t.Fatalf("document named changes export = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportChangesHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2)

	for i := 0; i < 3; i++ {
		w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
		if w.Code != http.StatusOK {
			t.Fatalf("export %d = %d", i, w.Code)
		}
	}

	_, after := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if after["nextCursor"].(float64) != 2 {
		t.Fatalf("nextCursor moved to %v after exports", after["nextCursor"])
	}
	w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=0")
	if strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("change set changed after exports: %s", w.Body.String())
	}
}

func TestSessionExportChangesHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-export-changes-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 3)
	w, _ := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=1&to=3")
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

	w2, _ := doRequest(t, h2, http.MethodGet, sessionChangesPath("sess", "doc1")+"?from=1&to=3")
	if w2.Code != http.StatusOK {
		t.Fatalf("export after restart = %d", w2.Code)
	}
	if string(w2.Body.Bytes()) != string(before) {
		t.Fatalf("export body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}
}
