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

// The session lookup answers in request order with a body byte-identical to
// the document-level lookup; the calling device is the session owner and a
// stray deviceId in the body is ignored.
func TestSessionQueryCRDTOpsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A document-level batch from another device exercises device sourcing.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("c2", 7)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The stray deviceId is ignored: the session owner is the caller.
	w = querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["c2","c1","nope"],"deviceId":"someone-else"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"c2","status":"found","deviceId":"dev-2","value":7},` +
		`{"id":"c1","status":"found","deviceId":"dev-1","value":5},` +
		`{"id":"nope","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// The session lookup renders the same per-type shapes the document-level
// lookup renders.
func TestSessionQueryCRDTOpsHTTPAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "g", sessionCRDTBody("gset", crdtGSetOp("g1", "b", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "r", sessionCRDTBody("register", crdtRegisterOp("r1", 2, "v")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "o", sessionCRDTBody("orset", crdtORSetOp("o1", "add", "x")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		doc, body, want string
	}{
		{"g", `{"ids":["g1"]}`,
			`{"results":[{"id":"g1","status":"found","deviceId":"dev","elements":["b","a"]}],"count":1}` + "\n"},
		{"r", `{"ids":["r1"]}`,
			`{"results":[{"id":"r1","status":"found","deviceId":"dev","version":2,"value":"v"}],"count":1}` + "\n"},
		{"o", `{"ids":["o1"]}`,
			`{"results":[{"id":"o1","status":"found","deviceId":"dev","action":"add","element":"x"}],"count":1}` + "\n"},
	}
	for _, tc := range cases {
		w := querySessionCRDTOps(t, h, "sess", tc.doc, tc.body)
		if w.Code != http.StatusOK || w.Body.String() != tc.want {
			t.Fatalf("%s = %d %q\nwant %q", tc.doc, w.Code, w.Body.String(), tc.want)
		}
	}
}

// A compacted id reports the trimmed status through the session view too,
// with no device or content, and compaction plus query can arrive on
// different entries of the same session.
func TestSessionQueryCRDTOpsHTTPCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["a1","a2","nope"]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"a1","status":"compacted"},` +
		`{"id":"a2","status":"found","deviceId":"dev-1","value":8},` +
		`{"id":"nope","status":"missing"}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Shape violations are 400 and precede the session existence verdict.
func TestSessionQueryCRDTOpsHTTPRejectsBadShape(t *testing.T) {
	h, _ := newTestHandler(t)
	url := sessionCRDTQueryPath("sess", "doc")

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
		name, contentType, body string
	}{
		{"wrong content type", "text/plain", `{"ids":["a"]}`},
		{"missing content type", "", `{"ids":["a"]}`},
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
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}

	// A malformed body against an unknown session is still 400: shape
	// precedes session existence.
	if w := send("application/json", `{"ids":[]}`); w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-session = %d body = %s", w.Code, w.Body.String())
	}
}

// The fixed verdict order: shape (400), session existence (404), permission
// (403), CRDT state existence (404); none of the failures exposes content.
func TestSessionQueryCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unknown session: 404.
	if w := querySessionCRDTOps(t, h, "ghost", "doc", `{"ids":["c1"]}`); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d body = %s", w.Code, w.Body.String())
	}
	// A document with no committed CRDT operation is the same 404 the state
	// read gives.
	if w := querySessionCRDTOps(t, h, "sess", "empty-doc", `{"ids":["c1"]}`); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with no content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["c1"]}`)
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"results"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// Only POST at the exact session .../crdt/query location is accepted; other
// verbs, missing/extra segments and empty identifiers are JSON 400.
func TestSessionQueryCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, path string
	}{
		{http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/query"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc/crdt/query"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc/crdt/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/query/"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/query/extra"},
		{http.MethodPost, "/v1/sessions//documents/doc/crdt/query"},
		{http.MethodPost, "/v1/sessions/sess/documents//crdt/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdt"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdtx/query"},
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/queryx"},
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
// the keyword counts only in the terminal subresource position.
func TestSessionQueryCRDTOpsIdentifierNamedQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "query")

	w, _ := postSessionCRDTOps(t, h, "query", "doc", sessionCRDTBody("counter", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := querySessionCRDTOps(t, h, "query", "doc", `{"ids":["z"]}`); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("session named query = %d %s", w.Code, w.Body.String())
	}

	createSessionViaHTTP(t, h, "dev-2", "sess2")
	w, _ = postSessionCRDTOps(t, h, "sess2", "query", sessionCRDTBody("counter", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := querySessionCRDTOps(t, h, "sess2", "query", `{"ids":["z"]}`); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"status":"found"`) {
		t.Fatalf("document named query = %d %s", w.Code, w.Body.String())
	}
}

// After a process restart the same session query yields the byte-identical
// body and verdicts.
func TestSessionQueryCRDTOpsHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := querySessionCRDTOps(t, h, "sess", "doc", `{"ids":["a1","a2","nope"]}`).Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w2 := querySessionCRDTOps(t, h2, "sess", "doc", `{"ids":["a1","a2","nope"]}`)
	if w2.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w2.Body.String())
	}
}
