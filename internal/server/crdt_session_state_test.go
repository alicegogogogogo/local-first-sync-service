package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// sessionCRDTStateURL builds the session-scoped CRDT state path.
func sessionCRDTStateURL(session, document string) string {
	return "/v1/sessions/" + session + "/documents/" + document + "/crdt/state"
}

func setupCRDTSession(t *testing.T, h http.Handler, device, session string) {
	t.Helper()
	registerDevice(t, h, device)
	w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": session})
	if w.Code != http.StatusOK {
		t.Fatalf("create session: %d %s", w.Code, w.Body.String())
	}
}

// A successful read renders the document's current type and merged result.
func TestSessionCRDTStateCounterSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	submit := func(device, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device,
			map[string]any{"id": id, "value": value}))
		if w.Code != http.StatusOK {
			t.Fatalf("submit: %d %s", w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 5)
	submit("dev-2", "b1", 3)
	submit("dev-1", "a2", 8) // dev-1 max advances 5 -> 8

	w, body := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
	if body["type"] != "counter" {
		t.Fatalf("type = %v", body["type"])
	}
	if body["value"].(float64) != 11 { // 8 + 3
		t.Fatalf("value = %v, want 11", body["value"])
	}
}

func TestSessionCRDTStateGSetSortedSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"banana", "apple"}},
		map[string]any{"id": "g2", "elements": []string{"apple", "cherry"}},
	))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	rec := rawRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if rec.Code != http.StatusOK {
		t.Fatalf("state = %d %s", rec.Code, rec.Body.String())
	}
	// Compact single-line JSON, keys in type/value order, one trailing newline.
	if got, want := rec.Body.String(), `{"type":"gset","value":["apple","banana","cherry"]}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// The session read body is byte-for-byte the document-level read body (and
// therefore a pushed state frame, per the subscription's encoder twin).
func TestSessionCRDTStateMatchesDocumentReadAndFrame(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 42}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	doc := rawRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	sess := rawRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if doc.Code != http.StatusOK || sess.Code != http.StatusOK {
		t.Fatalf("doc=%d sess=%d", doc.Code, sess.Code)
	}
	if doc.Body.String() != sess.Body.String() {
		t.Fatalf("session body %q != document body %q", sess.Body.String(), doc.Body.String())
	}
	want := `{"type":"counter","value":42}` + "\n"
	if sess.Body.String() != want {
		t.Fatalf("body = %q, want %q", sess.Body.String(), want)
	}

	var st store.CRDTState
	if err := json.Unmarshal([]byte(sess.Body.String()), &st); err != nil {
		t.Fatal(err)
	}
	if frame := string(encodeCRDTStateFrame(st)); frame != sess.Body.String() {
		t.Fatalf("subscription frame %q != state body %q", frame, sess.Body.String())
	}
}

// A document with no CRDT operation is the same 404 the document-level read
// returns — even when a change log exists.
func TestSessionCRDTState404WithoutOps(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")

	// Unknown document: 404.
	w, body := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "never"))
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("unknown doc = %d %s, want 404 JSON", w.Code, w.Body.String())
	}

	// A document with change-log rows but no CRDT operation is still 404.
	w, _ = postJSON(t, h, "/v1/documents/logdoc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c1", "payload": map[string]string{"k": "v"}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "logdoc"))
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("changes-only doc = %d %s, want 404 JSON", w.Code, w.Body.String())
	}
}

// A missing or deleted session is 404; once recreated it reads again.
func TestSessionCRDTStateSessionNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 1}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w, _ := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("ghost", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d, want 404", w.Code)
	}

	if w := rawRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess"); w.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session = %d, want 404", w.Code)
	}

	// Recreate the session as a brand-new one: the state is readable again.
	w, _ = postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatalf("recreate session: %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc")); w.Code != http.StatusOK {
		t.Fatalf("recreated session = %d, want 200", w.Code)
	}
}

// Revoked permission is 403 with no state content, distinct from the two 404s.
func TestSessionCRDTStatePermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 7}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	rec := rawRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d, want 403", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Fatalf("body = %q, want a JSON error", rec.Body.String())
	}
	if _, leaksType := body["type"]; leaksType {
		t.Fatalf("403 body must not carry state: %v", body)
	}
	if _, leaksValue := body["value"]; leaksValue {
		t.Fatalf("403 body must not carry state: %v", body)
	}

	// Permission is checked before state existence: a document that has never
	// held a CRDT operation is also 403 (not 404) once this device is revoked
	// for it.
	w, _ = postJSON(t, h, "/v1/documents/nostate/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess", "nostate")); w.Code != http.StatusForbidden {
		t.Fatalf("revoked + no state = %d, want 403 (permission precedes state lookup)", w.Code)
	}

	// Another device's session is unaffected by dev-1's revocation.
	setupCRDTSession(t, h, "dev-2", "sess2")
	if w, _ := doRequest(t, h, http.MethodGet, sessionCRDTStateURL("sess2", "doc")); w.Code != http.StatusOK {
		t.Fatalf("other device = %d, want 200", w.Code)
	}
}

// Every request-shape failure is a 400 JSON error: empty identifiers, missing
// or extra path segments, wrong method — never a redirect or HTML. Shape is
// validated before session existence, so a ghost session on a malformed path
// is still a 400.
func TestSessionCRDTStateRequestShape(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "sess")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 1}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		name   string
		method string
		target string
	}{
		{"empty session segment", http.MethodGet, "/v1/sessions//documents/doc/crdt/state"},
		{"empty document segment", http.MethodGet, "/v1/sessions/sess/documents//crdt/state"},
		{"trailing slash", http.MethodGet, sessionCRDTStateURL("sess", "doc") + "/"},
		{"extra segment", http.MethodGet, sessionCRDTStateURL("sess", "doc") + "/extra"},
		{"missing state segment", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt"},
		{"missing state keyword", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/ops"},
		{"wrong terminal word", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/states"},
		{"state segment doubled", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state/state"},
		{"POST not allowed", http.MethodPost, sessionCRDTStateURL("sess", "doc")},
		{"PUT not allowed", http.MethodPut, sessionCRDTStateURL("sess", "doc")},
		{"DELETE not allowed", http.MethodDelete, sessionCRDTStateURL("sess", "doc")},
		// Shape precedes the session lookup: a malformed path stays 400 even
		// though the session does not exist.
		{"malformed path with ghost session", http.MethodGet, "/v1/sessions/ghost/documents//crdt/state"},
		{"wrong method with ghost session", http.MethodPost, sessionCRDTStateURL("ghost", "doc")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := rawRequest(t, h, tc.method, tc.target)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want application/json", ct)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("body = %q, want a JSON error", rec.Body.String())
			}
		})
	}
}

// Identifiers literally named "state"/"crdt" occupy identifier positions and
// keep the endpoint working like any other id.
func TestSessionCRDTStateKeywordsAsIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	setupCRDTSession(t, h, "dev-1", "state")
	w, _ := postJSON(t, h, "/v1/documents/crdt/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 9}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rec := rawRequest(t, h, http.MethodGet, "/v1/sessions/state/documents/crdt/crdt/state")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d %s, want 200", rec.Code, rec.Body.String())
	}
	if got, want := rec.Body.String(), `{"type":"counter","value":9}`+"\n"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

// The read is side-effect free: it creates no cursor or change, and the state,
// error codes and every decision are identical after a process restart.
func TestSessionCRDTStateReadOnlyAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-crdt.db")
	s1, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	setupCRDTSession(t, h1, "dev-1", "sess")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"b", "a"}}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	before := rawRequest(t, h1, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if before.Code != http.StatusOK {
		t.Fatalf("read = %d", before.Code)
	}
	// Reading leaves the change log untouched: the document cursor space is 0.
	_, changes := doRequest(t, h1, http.MethodGet, "/v1/documents/doc/changes")
	if len(changes["changes"].([]any)) != 0 {
		t.Fatalf("CRDT read must not create changes: %v", changes["changes"])
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

	after := rawRequest(t, h2, http.MethodGet, sessionCRDTStateURL("sess", "doc"))
	if after.Code != http.StatusOK || after.Body.String() != before.Body.String() {
		t.Fatalf("after restart: %d %q, want %d %q", after.Code, after.Body.String(), before.Code, before.Body.String())
	}
	// Error outcomes survive the restart unchanged and stay distinct.
	if w, _ := doRequest(t, h2, http.MethodGet, sessionCRDTStateURL("ghost", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session after restart = %d, want 404", w.Code)
	}
	w2, _ := postJSON(t, h2, "/v1/documents/doc2/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w2.Code != http.StatusOK {
		t.Fatal(w2.Body.String())
	}
	if w, _ := doRequest(t, h2, http.MethodGet, sessionCRDTStateURL("sess", "doc2")); w.Code != http.StatusForbidden {
		t.Fatalf("revoked after restart = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h2, http.MethodGet, sessionCRDTStateURL("sess", "never")); w.Code != http.StatusNotFound {
		t.Fatalf("no-state after restart = %d, want 404", w.Code)
	}
}

// rawRequest dispatches a request with no body and returns the raw recorder so
// byte-level body assertions are possible.
func rawRequest(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}
