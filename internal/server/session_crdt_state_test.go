package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionCRDTStatePath is the session-scoped CRDT state read path.
func sessionCRDTStatePath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/state"
}

// createSessionViaHTTP registers device and creates session through the
// public HTTP surface.
func createSessionViaHTTP(t *testing.T, h http.Handler, device, session string) {
	t.Helper()
	registerDevice(t, h, device)
	w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": session})
	if w.Code != http.StatusOK {
		t.Fatalf("create session: %d %s", w.Code, w.Body.String())
	}
}

// assertJSONErrorStatus asserts a JSON {"error": "..."} body with the given
// status, no redirect and no HTML.
func assertJSONErrorStatus(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int) {
	t.Helper()
	if rec.Code != wantStatus {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	assertJSONError(t, rec)
	if loc := rec.Header().Get("Location"); loc != "" {
		t.Fatalf("unexpected redirect to %q", loc)
	}
}

// A successful counter read reports the sum of per-device maxima and its body
// is byte-for-byte the document-level state read's body.
func TestSessionCRDTStateCounter(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	submit := func(device, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device, crdtCounterOp(id, value)))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %s=%d: %d %s", device, value, w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 5)
	submit("dev-2", "b1", 3)
	submit("dev-1", "a2", 8) // advance dev-1 max 5 -> 8

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	// Compact single-line JSON, keys type then value, exactly one trailing
	// newline.
	wantBody := []byte(`{"type":"counter","value":11}` + "\n")
	if !bytes.Equal(rec.Body.Bytes(), wantBody) {
		t.Fatalf("body = %q, want %q", rec.Body.Bytes(), wantBody)
	}

	// Byte-for-byte identical to the document-level read.
	docRec := httptest.NewRecorder()
	h.ServeHTTP(docRec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	if !bytes.Equal(rec.Body.Bytes(), docRec.Body.Bytes()) {
		t.Fatalf("session body %q != document body %q", rec.Body.Bytes(), docRec.Body.Bytes())
	}
}

// A successful gset read reports the ascending union, again byte-identical to
// the document-level read.
func TestSessionCRDTStateGSet(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		crdtGSetOp("g1", "banana", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		crdtGSetOp("g2", "cherry", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	wantBody := []byte(`{"type":"gset","value":["apple","banana","cherry"]}` + "\n")
	if !bytes.Equal(rec.Body.Bytes(), wantBody) {
		t.Fatalf("body = %q, want %q", rec.Body.Bytes(), wantBody)
	}
}

// A document with no committed CRDT operation is a 404 JSON error even when
// the session exists and is authorized — the same judgment as the
// document-level read, regardless of change-log content.
func TestSessionCRDTState404BeforeAnyOp(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	// A change log for the document must not change the CRDT judgment.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)
}

// A session that never existed and a session that existed but was deleted are
// both a 404 JSON error.
func TestSessionCRDTStateSessionMissingOrDeleted(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	read := func(session string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath(session, "doc"), nil))
		return rec
	}

	assertJSONErrorStatus(t, read("ghost"), http.StatusNotFound)

	// Delete via the DELETE session route; a subsequent read is the same 404.
	del := httptest.NewRecorder()
	h.ServeHTTP(del, httptest.NewRequest(http.MethodDelete, "/v1/devices/dev-1/sessions/sess", nil))
	if del.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", del.Code, del.Body.String())
	}
	assertJSONErrorStatus(t, read("sess"), http.StatusNotFound)
}

// A revoked permission for the session's device is a 403 JSON error carrying
// no state content, even when the document has state.
func TestSessionCRDTStateRevokedPermission(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 7)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	assertJSONErrorStatus(t, rec, http.StatusForbidden)
	var body map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if strings.Contains(body["error"], "counter") || strings.Contains(body["error"], "value") {
		t.Fatalf("403 body leaks state: %q", rec.Body.String())
	}

	// Re-granting restores the read.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("after grant = %d %s", rec.Code, rec.Body.String())
	}
}

// Deleted session, unknown document-state and revoked permission are three
// independent answers, never merged into one error.
func TestSessionCRDTStateThreeOutcomesStayDistinct(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	createSessionViaHTTP(t, h, "dev-2", "sess-rev")

	// sess-rev's device is revoked on doc-rev, which does have state.
	w, _ := postJSON(t, h, "/v1/documents/doc-rev/crdt/ops", crdtCounterBody("dev-2",
		crdtCounterOp("a1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc-rev/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		name       string
		session    string
		doc        string
		wantStatus int
	}{
		{"deleted/missing session", "ghost", "doc-rev", http.StatusNotFound},
		{"authorized session, no state", "sess", "doc-empty", http.StatusNotFound},
		{"revoked permission", "sess-rev", "doc-rev", http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath(tc.session, tc.doc), nil))
			assertJSONErrorStatus(t, rec, tc.wantStatus)
		})
	}
}

// Shape validation always precedes the session lookup: a wrong method or a
// malformed path is a 400 JSON error even when the session does not exist, and
// the endpoint never redirects or emits HTML.
func TestSessionCRDTStateShapeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"wrong method POST", http.MethodPost, sessionCRDTStatePath("sess", "doc")},
		{"wrong method DELETE", http.MethodDelete, sessionCRDTStatePath("sess", "doc")},
		{"empty session segment", http.MethodGet, "/v1/sessions//documents/doc/crdt/state"},
		{"empty document segment", http.MethodGet, "/v1/sessions/sess/documents//crdt/state"},
		{"trailing slash", http.MethodGet, sessionCRDTStatePath("sess", "doc") + "/"},
		{"extra segment", http.MethodGet, sessionCRDTStatePath("sess", "doc") + "/extra"},
		{"bare crdt namespace", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt"},
		{"crdt namespace with slash", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/"},
		{"missing state word", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			assertJSONErrorStatus(t, rec, http.StatusBadRequest)
		})
	}

	// Shape beats every later judgment: the same malformed shapes against an
	// unknown session and a revoked document are still 400, not 404/403.
	order := []struct {
		name   string
		method string
		path   string
	}{
		{"wrong method, unknown session", http.MethodPost, sessionCRDTStatePath("ghost", "doc")},
		{"extra segment, unknown session", http.MethodGet, sessionCRDTStatePath("ghost", "doc") + "/extra"},
		{"empty session before lookup", http.MethodGet, "/v1/sessions//documents/doc/crdt/state"},
		{"wrong method despite revocation", http.MethodPost, sessionCRDTStatePath("sess", "doc2")},
	}
	for _, tc := range order {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tc.method, tc.path, nil))
			assertJSONErrorStatus(t, rec, http.StatusBadRequest)
		})
	}
}

// The read is read-only: it creates no CRDT state, advances no change cursor
// and leaves no change record.
func TestSessionCRDTStateIsReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
		assertJSONErrorStatus(t, rec, http.StatusNotFound)
	}
	// The document-level read agrees that the reads created no state.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)

	// Seed CRDT state and a change, then read repeatedly; the change page and
	// its cursor are untouched.
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 2)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for i := 0; i < 3; i++ {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
		if r.Code != http.StatusOK || r.Body.String() != `{"type":"counter","value":2}`+"\n" {
			t.Fatalf("read %d = %d %q", i, r.Code, r.Body.String())
		}
	}
	page := httptest.NewRecorder()
	h.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/changes", nil))
	var body struct {
		Changes    []json.RawMessage `json:"changes"`
		NextCursor int64             `json:"nextCursor"`
	}
	if err := json.Unmarshal(page.Body.Bytes(), &body); err != nil {
		t.Fatal(page.Body.String())
	}
	if len(body.Changes) != 1 || body.NextCursor != 1 {
		t.Fatalf("changes page mutated by reads: %s", page.Body.String())
	}
}

// Over real connections the session read, the document read and the
// subscription push all carry byte-identical state at the same instant.
func TestSessionCRDTStateMatchesDocumentReadAndPush(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 9))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %d", hs.StatusCode)
	}
	defer conn.close()
	frame := conn.readCRDTRaw()

	sessionBody := mustReadBody(t, srv, sessionCRDTStatePath("sess", "doc"))
	documentBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")
	if string(frame) != sessionBody || sessionBody != documentBody {
		t.Fatalf("frame=%q session=%q document=%q", frame, sessionBody, documentBody)
	}
}

// Identifiers literally named "state"/"crdt" stay ordinary identifiers on the
// session state read path.
func TestSessionCRDTStateKeywordIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "state")
	w, _ := postJSON(t, h, "/v1/documents/crdt/crdt/ops", crdtCounterBody("dev-1",
		crdtCounterOp("a1", 4)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit to doc crdt: %d %s", w.Code, w.Body.String())
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/sessions/state/documents/crdt/crdt/state", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d %s", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != `{"type":"counter","value":4}`+"\n" {
		t.Fatalf("body = %q", rec.Body.String())
	}
}

// After a process restart the session read answers the same state, 404s and
// 403s as before; the read itself persists nothing.
func TestSessionCRDTStateConsistentAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-crdt.db")

	s1, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	createSessionViaHTTP(t, h1, "dev-1", "sess")
	createSessionViaHTTP(t, h1, "dev-2", "sess-rev")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		crdtGSetOp("g1", "b", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h1, "/v1/documents/doc-rev/crdt/ops", crdtCounterBody("dev-2",
		crdtCounterOp("a1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h1, "/v1/documents/doc-rev/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Read everything once before the restart; the reads must leave no trace.
	for _, p := range []string{
		sessionCRDTStatePath("sess", "doc"),
		sessionCRDTStatePath("sess", "doc-empty"),
		sessionCRDTStatePath("sess-rev", "doc-rev"),
	} {
		rec := httptest.NewRecorder()
		h1.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		_, _ = io.Copy(io.Discard, rec.Body)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	read := func(p string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		return rec
	}
	if rec := read(sessionCRDTStatePath("sess", "doc")); rec.Code != http.StatusOK ||
		rec.Body.String() != `{"type":"gset","value":["a","b"]}`+"\n" {
		t.Fatalf("state after restart = %d %q", rec.Code, rec.Body.String())
	}
	assertJSONErrorStatus(t, read(sessionCRDTStatePath("sess", "doc-empty")), http.StatusNotFound)
	assertJSONErrorStatus(t, read(sessionCRDTStatePath("ghost", "doc")), http.StatusNotFound)
	assertJSONErrorStatus(t, read(sessionCRDTStatePath("sess-rev", "doc-rev")), http.StatusForbidden)
}
