package server

import (
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionSnapshotsPath is the session-scoped snapshot collection path: the
// session document-read prefix with the resource segment swapped to the
// snapshot collection.
func sessionSnapshotsPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshots"
}

func TestSessionExportSnapshotsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{
		1: "null",
		2: `42`,
		3: `"str"`,
		4: `[1,2]`,
		5: `{"a":1}`,
	})

	// No parameters: from defaults to 0, no upper bound — the path offers no
	// other read form, so a bare GET is the export. The body is one compact
	// line with keys ordered snapshots,count and item keys ordered
	// cursor,state; exactly one trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1"))
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
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=2&to=4")
	want = `{"snapshots":[` +
		`{"cursor":2,"state":42},` +
		`{"cursor":3,"state":"str"},` +
		`{"cursor":4,"state":[1,2]}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("range body = %q\nwant %q", w.Body.String(), want)
	}

	// Degenerate interval matches the single cursor.
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=3&to=3")
	if got := strings.TrimSpace(w.Body.String()); got != `{"snapshots":[{"cursor":3,"state":"str"}],"count":1}` {
		t.Fatalf("point body = %q", got)
	}

	// Lower bound only (to absent).
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=5")
	if got := strings.TrimSpace(w.Body.String()); got != `{"snapshots":[{"cursor":5,"state":{"a":1}}],"count":1}` {
		t.Fatalf("from-only body = %q", got)
	}

	// to alone applies the lower default of 0.
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?to=1")
	if got := strings.TrimSpace(w.Body.String()); got != `{"snapshots":[{"cursor":1,"state":null}],"count":1}` {
		t.Fatalf("to-only body = %q", got)
	}

	// Explicit empty parameters behave like defaults.
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=&to=")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 5 {
		t.Fatalf("empty params = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportSnapshotsHTTPMatchesDocumentExport(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"z":1,"a":2}`, 3: `[true,null,{"n":0.5}]`})

	// The session view's body is byte-for-byte the document-level export's.
	for _, q := range []string{"", "?from=0", "?from=1&to=3", "?to=2", "?from=9"} {
		wSess, _ := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+q)
		wDoc, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots"+q)
		if wSess.Code != wDoc.Code || wSess.Body.String() != wDoc.Body.String() {
			t.Fatalf("q=%q session = %d %s, document = %d %s",
				q, wSess.Code, wSess.Body.String(), wDoc.Code, wDoc.Body.String())
		}
	}
}

func TestSessionExportSnapshotsHTTPEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{2: `{"v":1}`, 4: `{"v":2}`})

	// An interval without a snapshot — and a document unknown to the log —
	// are successful empty exports, not errors.
	for _, url := range []string{
		sessionSnapshotsPath("sess", "doc1") + "?from=3&to=3",
		sessionSnapshotsPath("sess", "doc1") + "?from=5",
		sessionSnapshotsPath("sess", "doc1") + "?from=99&to=200",
		sessionSnapshotsPath("sess", "ghost") + "?from=0",
		sessionSnapshotsPath("sess", "ghost"),
	} {
		w, _ := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d", url, w.Code)
		}
		if got := w.Body.String(); got != `{"snapshots":[],"count":0}`+"\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}
}

func TestSessionExportSnapshotsHTTPRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
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
		w, _ := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?"+q)
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
	w, body := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("ghost", "doc1")+"?from=x")
	if w.Code != http.StatusBadRequest || body["error"] == nil {
		t.Fatalf("bad param + missing session = %d %v", w.Code, body)
	}

	// Empty identifiers are a 400 JSON error, not a redirect.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/snapshots",
		"/v1/sessions/sess/documents//snapshots",
		"/v1/sessions//documents/doc1/snapshots?from=0",
	} {
		w, _ := doRequest(t, h, http.MethodGet, p)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", p, w.Code)
		}
		assertJSONError(t, w)
	}

	// Zero writes: the rejected requests neither created snapshots nor moved
	// the change cursor.
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots")
	if strings.Count(w.Body.String(), `"cursor"`) != 1 {
		t.Fatalf("snapshot set changed: %s", w.Body.String())
	}
}

func TestSessionExportSnapshotsHTTPSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"v":1}`})

	// Never-created session -> 404 with no snapshot content.
	w, body := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("ghost", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["snapshots"] != nil || body["count"] != nil {
		t.Fatalf("404 leaked export content: %s", w.Body.String())
	}

	// Delete the session -> exports now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}
	if body["snapshots"] != nil || body["count"] != nil {
		t.Fatalf("404 leaked export content: %s", w.Body.String())
	}
}

func TestSessionExportSnapshotsHTTPPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"v":1}`, 2: `{"v":2}`})

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 with no snapshot content.
	w, body := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["snapshots"] != nil || body["count"] != nil {
		t.Fatalf("403 leaked export content: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("ghost", "doc1")+"?from=0")
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Re-grant restores the export.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=0")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("re-granted export = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportSnapshotsHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	for _, c := range []struct{ method, path string }{
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/snapshots"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc1/snapshots"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc1/snapshots"},
		{http.MethodPatch, "/v1/sessions/sess/documents/doc1/snapshots"},
		{http.MethodOptions, "/v1/sessions/sess/documents/doc1/snapshots"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/snapshots?from=0"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/snapshots/"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/snapshots/1"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/snapshots/1?from=0"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/snapshots/extra"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/snapshots/extra"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/snapshots/extra/more"},
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
	// document literally named "snapshots" exports through the same shape.
	seedSnapshots(t, h, "snapshots", map[int]string{1: `{"k":1}`})
	w, _ := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/snapshots/snapshots?from=0")
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"snapshots":[{"cursor":1,"state":{"k":1}}],"count":1}` {
		t.Fatalf("document named snapshots export = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionExportSnapshotsHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"v":1}`, 2: `{"v":2}`})

	// Repeated exports change nothing.
	for i := 0; i < 3; i++ {
		w, _ := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=0")
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

func TestSessionExportSnapshotsHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-export-snapshots-http.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"a":1}`, 2: `[1,2]`, 3: `null`})
	w, _ := doRequest(t, h, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=1&to=3")
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

	w2, _ := doRequest(t, h2, http.MethodGet, sessionSnapshotsPath("sess", "doc1")+"?from=1&to=3")
	if w2.Code != http.StatusOK {
		t.Fatalf("export after restart = %d", w2.Code)
	}
	if string(w2.Body.Bytes()) != string(before) {
		t.Fatalf("export body changed across restart:\nbefore %q\nafter  %q", before, w2.Body.Bytes())
	}
}
