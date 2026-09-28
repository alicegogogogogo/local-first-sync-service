package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// sessionCRDTListPath is the session-scoped CRDT operation listing path.
func sessionCRDTListPath(session, doc, rawQuery string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/list" + rawQuery
}

// listSessionCRDTOps sends a raw GET to the session-scoped CRDT operation
// listing and returns the recorder.
func listSessionCRDTOps(h http.Handler, session, doc, rawQuery string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, sessionCRDTListPath(session, doc, rawQuery), nil))
	return w
}

// The session listing is byte-for-byte the document-level listing's body:
// ascending ids, the fixed per-item key order, the page count and the trimmed
// total. The caller is the session owner; a stray deviceId query parameter is
// ignored and can never override it.
func TestSessionListCRDTOpsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("c1", 3), crdtCounterOp("c2", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("c3", 7)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w = listSessionCRDTOps(h, "sess", "doc", "?deviceId=someone-else")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"ops":[` +
		`{"id":"c1","deviceId":"dev-1","type":"counter","value":3},` +
		`{"id":"c2","deviceId":"dev-1","type":"counter","value":5},` +
		`{"id":"c3","deviceId":"dev-2","type":"counter","value":7}` +
		`],"count":3,"compacted":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// limit/offset page the session listing exactly like the document listing.
func TestSessionListCRDTOpsHTTPPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	for _, id := range []string{"op-02", "op-01"} {
		w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp(id, 1)))
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	w := listSessionCRDTOps(h, "sess", "doc", "?limit=1&offset=0")
	if w.Body.String() != `{"ops":[{"id":"op-01","deviceId":"dev","type":"counter","value":1}],"count":1,"compacted":0}`+"\n" {
		t.Fatalf("page 0 = %q", w.Body.String())
	}
	w = listSessionCRDTOps(h, "sess", "doc", "?limit=1&offset=1")
	if w.Body.String() != `{"ops":[{"id":"op-02","deviceId":"dev","type":"counter","value":1}],"count":1,"compacted":0}`+"\n" {
		t.Fatalf("page 1 = %q", w.Body.String())
	}
	w = listSessionCRDTOps(h, "sess", "doc", "?offset=2")
	if w.Body.String() != `{"ops":[],"count":0,"compacted":0}`+"\n" {
		t.Fatalf("past-end = %q", w.Body.String())
	}
}

// Pagination shape is judged before the session existence verdict.
func TestSessionListCRDTOpsHTTPRejectsBadPaginationBeforeSession(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, q := range []string{"?limit=0", "?limit=1001", "?limit=x", "?offset=-1", "?offset=1.5"} {
		w := listSessionCRDTOps(h, "ghost-session", "doc", q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s against unknown session = %d body = %s", q, w.Code, w.Body.String())
		}
		if !strings.HasPrefix(w.Body.String(), `{"error":`) {
			t.Fatalf("%s body = %q, want a JSON error", q, w.Body.String())
		}
	}
}

// The fixed verdict order: shape (400), session existence (404), permission
// (403), CRDT state existence (404); the latter failures expose no content.
func TestSessionListCRDTOpsHTTPGateOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// An unknown session is a 404, even naming a document with CRDT data.
	if w := listSessionCRDTOps(h, "ghost", "doc", ""); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d body = %s", w.Code, w.Body.String())
	}
	// A document with no committed CRDT operation is the same 404 the session
	// state read gives.
	if w := listSessionCRDTOps(h, "sess", "empty-doc", ""); w.Code != http.StatusNotFound {
		t.Fatalf("no-crdt document = %d body = %s", w.Code, w.Body.String())
	}

	// Revoke the session owner's permission for the populated document: 403
	// with no listing content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = listSessionCRDTOps(h, "sess", "doc", "")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked session device = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"ops"`) || strings.Contains(w.Body.String(), `"c1"`) {
		t.Fatalf("403 leaked crdt content: %s", w.Body.String())
	}
}

// Only GET at the exact session .../crdt/list location is accepted; every
// other verb or shape is a JSON 400, never a redirect.
func TestSessionListCRDTOpsHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, path string
	}{
		{http.MethodPost, "/v1/sessions/sess/documents/doc1/crdt/list"},
		{http.MethodPut, "/v1/sessions/sess/documents/doc1/crdt/list"},
		{http.MethodDelete, "/v1/sessions/sess/documents/doc1/crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/crdt/list/"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/crdt/list/extra"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/crdt/listextra"},
		{http.MethodGet, "/v1/sessions//documents/doc1/crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents//crdt/list"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/crdt"},
		{http.MethodGet, "/v1/sessions/sess/documents/doc1/crdt/query/list"},
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
// list keyword is only recognized in the terminal subresource position.
func TestSessionListCRDTOpsHTTPIdentifierNamedList(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "list")

	w, _ := postSessionCRDTOps(t, h, "list", "doc", sessionCRDTBody("counter", crdtCounterOp("z", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := listSessionCRDTOps(h, "list", "doc", ""); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("list with session named list = %d %s", w.Code, w.Body.String())
	}

	// A document literally named "list" under an ordinary session works too.
	w, _ = postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "list", sessionCRDTBody("counter", crdtCounterOp("q", 2)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := listSessionCRDTOps(h, "sess", "list", ""); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"id":"q"`) {
		t.Fatalf("list with document named list = %d %s", w.Code, w.Body.String())
	}
}

// After a session device is deregistered (which cascades the session away),
// the listing answers 404 "session not found", never a content leak.
func TestSessionListCRDTOpsHTTPDeregisteredOwner(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	req := httptest.NewRequest(http.MethodDelete, "/v1/devices/dev", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("deregister = %d %s", rw.Code, rw.Body.String())
	}
	if w := listSessionCRDTOps(h, "sess", "doc", ""); w.Code != http.StatusNotFound {
		t.Fatalf("after deregister = %d body = %s", w.Code, w.Body.String())
	}
}
