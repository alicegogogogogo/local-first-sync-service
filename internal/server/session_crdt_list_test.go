package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCRDTListPath is the session-scoped CRDT operation listing path.
func sessionCRDTListPath(session, doc, query string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/list" + query
}

// listSessionCRDTOps sends a raw GET to the session-scoped CRDT operation
// listing and returns the recorder.
func listSessionCRDTOps(t *testing.T, h http.Handler, session, doc, query string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, sessionCRDTListPath(session, doc, query), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// The session listing renders the stable id order and the same body the
// document-level listing renders; the calling device is the session owner and
// a stray deviceId query parameter is ignored.
func TestSessionListCRDTOpsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A document-level batch from another device exercises device sourcing.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("c3", 7)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1", crdtCounterOp("c2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w = listSessionCRDTOps(t, h, "sess", "doc", "?deviceId=someone-else")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"ops":[` +
		`{"id":"c1","deviceId":"dev-1","type":"counter","value":5},` +
		`{"id":"c2","deviceId":"dev-1","type":"counter","value":8},` +
		`{"id":"c3","deviceId":"dev-2","type":"counter","value":7}` +
		`],"count":3,"compacted":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// The session listing renders the gset, register and orset item shapes the
// document-level listing renders.
func TestSessionListCRDTOpsHTTPAllTypes(t *testing.T) {
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
		doc, want string
	}{
		{"g", `{"ops":[{"id":"g1","deviceId":"dev","type":"gset","elements":["b","a"]}],"count":1,"compacted":0}` + "\n"},
		{"r", `{"ops":[{"id":"r1","deviceId":"dev","type":"register","version":2,"value":"v"}],"count":1,"compacted":0}` + "\n"},
		{"o", `{"ops":[{"id":"o1","deviceId":"dev","type":"orset","action":"add","element":"x"}],"count":1,"compacted":0}` + "\n"},
	}
	for _, tc := range cases {
		w := listSessionCRDTOps(t, h, "sess", tc.doc, "")
		if w.Code != http.StatusOK || w.Body.String() != tc.want {
			t.Fatalf("%s = %d %q\nwant %q", tc.doc, w.Code, w.Body.String(), tc.want)
		}
	}
}

// limit/offset page the session listing on the stable order; an offset past
// the end is an empty page with count 0.
func TestSessionListCRDTOpsHTTPPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a", 1), crdtCounterOp("b", 2), crdtCounterOp("c", 3)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w1 := listSessionCRDTOps(t, h, "sess", "doc", "?limit=2&offset=0")
	if w1.Body.String() != `{"ops":[{"id":"a","deviceId":"dev","type":"counter","value":1},{"id":"b","deviceId":"dev","type":"counter","value":2}],"count":2,"compacted":0}`+"\n" {
		t.Fatalf("page1 = %q", w1.Body.String())
	}
	w2 := listSessionCRDTOps(t, h, "sess", "doc", "?limit=2&offset=2")
	if w2.Body.String() != `{"ops":[{"id":"c","deviceId":"dev","type":"counter","value":3}],"count":1,"compacted":0}`+"\n" {
		t.Fatalf("page2 = %q", w2.Body.String())
	}
	w3 := listSessionCRDTOps(t, h, "sess", "doc", "?offset=3")
	if w3.Body.String() != `{"ops":[],"count":0,"compacted":0}`+"\n" {
		t.Fatalf("page3 = %q", w3.Body.String())
	}
}

// Illegal pagination values are 400 and precede the session existence verdict.
func TestSessionListCRDTOpsHTTPRejectsBadPagination(t *testing.T) {
	h, _ := newTestHandler(t)

	bad := []string{
		"?limit=0", "?limit=1001", "?limit=-1", "?limit=1.5", "?limit=abc",
		"?offset=-1", "?offset=1.5", "?offset=abc",
	}
	for _, q := range bad {
		w := listSessionCRDTOps(t, h, "sess", "doc", q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d want 400 body = %s", q, w.Code, w.Body.String())
		}
		if !strings.HasPrefix(w.Body.String(), `{"error":`) {
			t.Fatalf("%s body = %q, want a JSON error", q, w.Body.String())
		}
	}

	// A malformed listing against an unknown session is still a 400: shape
	// precedes session existence.
	if w := listSessionCRDTOps(t, h, "ghost", "doc", "?limit=0"); w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-session = %d body = %s", w.Code, w.Body.String())
	}
}

// The fixed verdict order: shape (400), session existence (404), permission
// (403), CRDT state existence (404); none of the failures exposes content.
func TestSessionListCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unknown session: 404, even with a stray deviceId claiming someone else.
	if w := listSessionCRDTOps(t, h, "ghost", "doc", "?deviceId=dev"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d body = %s", w.Code, w.Body.String())
	}
	// A document with no committed CRDT operation is the same 404 the state
	// read gives.
	if w := listSessionCRDTOps(t, h, "sess", "empty-doc", ""); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoked permission: 403 with no content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = listSessionCRDTOps(t, h, "sess", "doc", "")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"ops"`) || strings.Contains(w.Body.String(), `"value"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// A compacted id leaves the session listing and compacted reports the total.
func TestSessionListCRDTOpsHTTPCompacted(t *testing.T) {
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

	w = listSessionCRDTOps(t, h, "sess", "doc", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"ops":[{"id":"a2","deviceId":"dev-1","type":"counter","value":8}],"count":1,"compacted":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// Only GET at the exact session .../crdt/list location is accepted; other
// verbs, missing/extra segments and empty identifiers are JSON 400.
func TestSessionListCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, path string
	}{
		{http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/list"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc/crdt/list"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc/crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/list/"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/list/extra"},
		{http.MethodGet, "/v1/sessions//documents/doc/crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents//crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc/crdt"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/listx"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
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

// A session or document literally named "list" keeps its ordinary routes; the
// keyword counts only in the terminal subresource position.
func TestSessionListCRDTOpsIdentifierNamedList(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "list")

	w, _ := postSessionCRDTOps(t, h, "list", "doc", sessionCRDTBody("counter", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := listSessionCRDTOps(t, h, "list", "doc", ""); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("session named list = %d %s", w.Code, w.Body.String())
	}

	createSessionViaHTTP(t, h, "dev-2", "sess2")
	w, _ = postSessionCRDTOps(t, h, "sess2", "list", sessionCRDTBody("counter", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := listSessionCRDTOps(t, h, "sess2", "list", ""); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("document named list = %d %s", w.Code, w.Body.String())
	}
}

// After a process restart the same session listing yields the byte-identical
// body and verdicts.
func TestSessionListCRDTOpsHTTPRestartStable(t *testing.T) {
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
	body := listSessionCRDTOps(t, h, "sess", "doc", "?limit=10&offset=0").Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w2 := listSessionCRDTOps(t, h2, "sess", "doc", "?limit=10&offset=0")
	if w2.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w2.Code, w2.Body.String())
	}
	if w2.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w2.Body.String())
	}
}
