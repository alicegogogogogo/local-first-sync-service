package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCRDTQueryPath is the session-scoped CRDT batch lookup path.
func sessionCRDTQueryPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/query"
}

// querySessionCRDTOps sends a raw POST to the session-scoped CRDT batch
// lookup and returns the recorder.
func querySessionCRDTOps(t *testing.T, h http.Handler, session, doc, rawBody string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, sessionCRDTQueryPath(session, doc), strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// The session lookup's found, missing and compacted answers are byte-for-byte
// the document-level lookup's for the same document and ids, and the calling
// device is the session's owning device: a stray deviceId in the body is
// ignored and cannot override it.
func TestSessionQueryCRDTOpsHTTPSuccessMatchesDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", map[string]any{
		"deviceId": "dev-1", "type": "register", "ops": []map[string]any{
			{"id": "r1", "version": 1, "value": "one"},
			{"id": "r2", "version": 2, "value": "two"},
		},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	body := `{"ids":["r2","ghost","r1"]}`
	wSess := querySessionCRDTOps(t, h, "sess", "doc", body)
	if wSess.Code != http.StatusOK {
		t.Fatalf("session status = %d %s", wSess.Code, wSess.Body.String())
	}
	wDoc := queryCRDTOps(t, h, "doc", `{"deviceId":"dev-1","ids":["r2","ghost","r1"]}`)
	if wSess.Body.String() != wDoc.Body.String() {
		t.Fatalf("session and document bodies differ:\nsession  %q\ndocument %q", wSess.Body.String(), wDoc.Body.String())
	}
	want := `{"results":[` +
		`{"id":"r2","status":"found","deviceId":"dev-1","version":2,"value":"two"},` +
		`{"id":"ghost","status":"missing"},` +
		`{"id":"r1","status":"found","deviceId":"dev-1","version":1,"value":"one"}` +
		`],"count":3}` + "\n"
	if wSess.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", wSess.Body.String(), want)
	}

	// A stray deviceId naming another device is decoded and ignored: the
	// answer still comes from the session owner's identity.
	wStray := querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["r1"],"deviceId":"dev-2"}`)
	if wStray.Code != http.StatusOK {
		t.Fatalf("stray deviceId = %d %s", wStray.Code, wStray.Body.String())
	}
	if !strings.Contains(wStray.Body.String(), `"deviceId":"dev-1"`) {
		t.Fatalf("stray deviceId overrode the session owner: %s", wStray.Body.String())
	}
}

// Compacted-away ids answered through the session view report the bare
// trimmed status exactly like the document-level lookup.
func TestSessionQueryCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	postSession := func(typ string, raw string) {
		t.Helper()
		if w, _ := postJSON(t, h, sessionCRDTOpsPath("sess", "doc"), raw); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	postSession("counter", `{"type":"counter","ops":[{"id":"a1","value":5},{"id":"a2","value":8}]}`)
	// Compaction through the session entry too.
	if w, _ := postJSON(t, h, "/v1/sessions/sess/documents/doc/crdt/compact", "{}"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w := querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["a1","a2","never"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"a1","status":"compacted"},` +
		`{"id":"a2","status":"found","deviceId":"dev-1","value":8},` +
		`{"id":"never","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Every request-shape violation is a 400 JSON error, settled before the
// session existence verdict: a malformed batch against an unknown session is
// still a 400.
func TestSessionQueryCRDTOpsHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	if w, _ := postJSON(t, h, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a1", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	url := sessionCRDTQueryPath("sess", "doc")

	send := func(target, contentType, raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(raw))
		if contentType != "" {
			r.Header.Set("Content-Type", contentType)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	cases := []struct {
		name string
		body string
	}{
		{"invalid json", `{"ids":[`},
		{"trailing content", `{"ids":["a"]} junk`},
		{"second json value", `{"ids":["a"]}{}`},
		{"missing ids", `{}`},
		{"null ids", `{"ids":null}`},
		{"empty ids", `{"ids":[]}`},
		{"empty id element", `{"ids":["a",""]}`},
		{"non-string id element", `{"ids":["a",1]}`},
		{"ids not an array", `{"ids":"a"}`},
		{"duplicate ids", `{"ids":["a","a"]}`},
		{"non-object body", `[1,2]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := send(url, "application/json", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// Content-type failures are 400 as well.
	for _, ct := range []string{"", "text/plain", "application/vnd.api+json"} {
		if w := send(url, ct, `{"ids":["a"]}`); w.Code != http.StatusBadRequest {
			t.Fatalf("content type %q = %d, want 400", ct, w.Code)
		}
	}

	// Shape precedes session existence.
	w := send(sessionCRDTQueryPath("ghost", "doc"), "application/json", `{"ids":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-session = %d, want 400", w.Code)
	}
}

// Session existence, document permission and CRDT state existence are enforced
// in that order after shape validation, and a failure exposes no content.
func TestSessionQueryCRDTOpsHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	if w, _ := postJSON(t, h, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a1", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unknown session: 404 with an error only.
	w := querySessionCRDTOps(t, h, "ghost", "doc", `{"ids":["a1"]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// Revoked permission: 403 with an error only.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["a1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked session device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked content: %s", w.Body.String())
	}

	// Permission precedes the CRDT-state verdict: revoke on a document with no
	// CRDT ops and the answer is 403, not 404.
	w, _ = postJSON(t, h, "/v1/documents/no-state/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = querySessionCRDTOps(t, h, "sess", "no-state", `{"ids":["a1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked on no-state doc = %d, want 403", w.Code)
	}

	// Re-grant: a document with no committed CRDT operation is 404, matching
	// the session state read.
	w, _ = postJSON(t, h, "/v1/documents/no-state/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = querySessionCRDTOps(t, h, "sess", "no-state", `{"ids":["a1"]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("no-state doc = %d, want 404 body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
}

// Method and path shape: only POST at the exact session .../crdt/query
// location is accepted; every other verb or shape is a JSON 400, never a
// redirect.
func TestSessionQueryCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	base := "/v1/sessions/sess/documents/doc1/crdt/query"

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, base},
		{http.MethodPut, base},
		{http.MethodDelete, base},
		{http.MethodPost, base + "/"},
		{http.MethodPost, base + "/extra"},
		{http.MethodPost, base + "extra"},
		{http.MethodPost, "/v1/sessions//documents/doc1/crdt/query"},
		{http.MethodPost, "/v1/sessions/sess/documents//crdt/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/crdtx/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/crdt"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/crdt/state"},
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

// Identifiers literally named "query" keep ordinary behavior: the keyword is
// an endpoint word only in its terminal segment position.
func TestSessionQueryCRDTOpsHTTPKeywordIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	if w, _ := postJSON(t, h, sessionCRDTOpsPath("sess", "query"),
		sessionCRDTBody("counter", crdtCounterOp("z", 1))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w := querySessionCRDTOps(t, h, "sess", "query", `{"ids":["z"]}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("query on doc named query = %d %s", w.Code, w.Body.String())
	}
}

// The session query verdict is byte-stable across a restart.
func TestSessionQueryCRDTOpsHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	if w, _ := postJSON(t, h, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("gset", crdtGSetOp("g1", "a"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["g1","x"]}`).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	r := httptest.NewRequest(http.MethodPost, sessionCRDTQueryPath("sess", "doc"), strings.NewReader(`{"ids":["g1","x"]}`))
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
