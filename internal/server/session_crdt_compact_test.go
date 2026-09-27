package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCRDTCompactPath is the session-scoped CRDT compaction path.
func sessionCRDTCompactPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/compact"
}

// postSessionCRDTCompact posts one raw compaction body to the session-scoped
// CRDT compaction entry and returns the recorder so tests can assert the
// exact single-line body.
func postSessionCRDTCompact(t *testing.T, h http.Handler, session, doc string, body any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, sessionCRDTCompactPath(session, doc), body)
	return w
}

func TestSessionCRDTCompactEndToEnd(t *testing.T) {
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
	submit("dev-1", "a2", 8) // dev-1's max advances 5 -> 8; a1 is trimmable

	// The body only has to be one legal JSON value; a stray deviceId is
	// ignored and never overrides the session's owning device.
	w := postSessionCRDTCompact(t, h, "sess", "doc", `{"deviceId":"someone-else"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	want := compactBody("counter", "11", "2", "0")
	if got := w.Body.String(); got != want {
		t.Fatalf("compact body = %q, want %q", got, want)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("compact content type = %q", ct)
	}

	// The compact response is byte-for-byte the document-level snapshot read.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != want {
		t.Fatalf("snapshot after compact = %q, want %q", got, want)
	}
	// The merged result is unchanged and readable through the session view.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if got := rec.Body.String(); got != "{\"type\":\"counter\",\"value\":11}\n" {
		t.Fatalf("state after compact = %q", got)
	}

	// Repeating the compaction is not an error and returns the same body.
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != want {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}
}

// Every legal JSON value is an acceptable compaction body; its content never
// affects the result.
func TestSessionCRDTCompactAcceptsAnyJSONBody(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, raw := range []string{
		`{}`,
		`{"deviceId":"dev"}`,
		`{"unexpected":["nested", 1, true, null]}`,
		`[]`,
		`[1, {"a":2}]`,
		`"a string"`,
		`123`,
		`true`,
		`null`,
	} {
		w := postSessionCRDTCompact(t, h, "sess", "doc", raw)
		if w.Code != http.StatusOK {
			t.Fatalf("body %s = %d %s", raw, w.Code, w.Body.String())
		}
		// The first legal body trims the superseded contribution; every later
		// legal body is an idempotent repeat returning the same snapshot.
		want := compactBody("counter", "8", "1", "0")
		if got := w.Body.String(); got != want {
			t.Fatalf("body %s -> %q, want %q", raw, got, want)
		}
	}
}

func TestSessionCRDTCompactBodyValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	before := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String()

	// Shape failures precede the session lookup: a malformed body against an
	// unknown session is still a 400.
	for _, tc := range []struct {
		name        string
		session     string
		body        string
		contentType string
	}{
		{"invalid JSON", "sess", `{not json`, "application/json"},
		{"trailing content", "sess", `{} {}`, "application/json"},
		{"empty body", "sess", ``, "application/json"},
		{"wrong content type", "sess", `{}`, "text/plain"},
		{"malformed body unknown session", "ghost", `{`, "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, sessionCRDTCompactPath(tc.session, "doc"), strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}

	// Every rejection wrote nothing.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != before {
		t.Fatalf("snapshot changed by rejected compacts: %q -> %q", before, got)
	}
}

func TestSessionCRDTCompactPathAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"empty session id", http.MethodPost, "/v1/sessions//documents/doc/crdt/compact"},
		{"empty document id", http.MethodPost, "/v1/sessions/sess/documents//crdt/compact"},
		{"trailing slash", http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/compact/"},
		{"extra segment", http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/compact/x"},
		{"missing compact segment", http.MethodPost, "/v1/sessions/sess/documents/doc/crdt"},
		{"wrong method GET", http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/compact"},
		{"wrong method DELETE", http.MethodDelete, "/v1/sessions/sess/documents/doc/crdt/compact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}

	// A session or document literally named "compact" is an ordinary
	// identifier, not the endpoint keyword.
	createSessionViaHTTP(t, h, "dev2", "compact")
	w, _ = postSessionCRDTOps(t, h, "compact", "compact", sessionCRDTBody("counter", crdtCounterOp("c1", 4)))
	if w.Code != http.StatusOK {
		t.Fatalf("ops on compact-named ids = %d %s", w.Code, w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "compact", "compact", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "4", "1", "0") {
		t.Fatalf("compact-named ids = %d %q", w.Code, w.Body.String())
	}
}

func TestSessionCRDTCompactGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Revoke the session owner's permission on a second document that also
	// has CRDT state.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc2", sessionCRDTBody("counter", crdtCounterOp("b1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A deleted session is a 404, even with an otherwise valid body.
	gw, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "gone"})
	if gw.Code != http.StatusOK {
		t.Fatalf("create gone session: %d %s", gw.Code, gw.Body.String())
	}
	dw, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/gone")
	if dw.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", dw.Code, dw.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "gone", "doc", `{}`); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session = %d %s", w.Code, w.Body.String())
	}

	// Revoked permission is a 403 after the shape and session checks pass.
	if w := postSessionCRDTCompact(t, h, "sess", "doc2", `{}`); w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d %s", w.Code, w.Body.String())
	}

	// A document with no CRDT operation is a 404.
	if w := postSessionCRDTCompact(t, h, "sess", "never", `{}`); w.Code != http.StatusNotFound {
		t.Fatalf("no crdt state = %d %s", w.Code, w.Body.String())
	}

	// The rejections trimmed nothing: doc still holds both contributions and
	// doc2 still holds its one.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compactBody("counter", "8", "2", "0") {
		t.Fatalf("doc snapshot after rejections = %q", got)
	}
	if got := getRaw(t, h, "/v1/documents/doc2/crdt/snapshot").Body.String(); got != compactBody("counter", "1", "1", "0") {
		t.Fatalf("doc2 snapshot after rejections = %q", got)
	}
}

func TestSessionCRDTCompactTrimmedIdentity(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	submit := func(id string, value int) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp(id, value)))
		return w
	}
	if w := submit("a1", 5); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submit("a2", 8); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// The trimmed a1 still replays idempotently through the session commit:
	// created=false and no new merge effect.
	w := submit("a1", 5)
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"results":[{"id":"a1","created":false}]}` {
		t.Fatalf("trimmed replay body = %q", got)
	}

	// Same id, changed comparison content: 409 JSON, state unchanged.
	if w := submit("a1", 9); w.Code != http.StatusConflict {
		t.Fatalf("trimmed id changed content = %d, want 409", w.Code)
	}
	// Same id, changed origin device: 409 as well, even though the op row is
	// gone.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusConflict {
		t.Fatalf("trimmed id changed device = %d, want 409", w.Code)
	}

	// The failed replays moved nothing: the merge still merges from one op.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compactBody("counter", "8", "1", "0") {
		t.Fatalf("snapshot after replays = %q", got)
	}
}

// The per-type trimming rule is shared with the document-level compaction:
// gset keeps a minimal cover, orset drops tombstoned tags with their
// tombstones and leaves its operation log out of the trim.
func TestSessionCRDTCompactSetTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	post := func(doc string, body map[string]any) {
		t.Helper()
		w, _ := postSessionCRDTOps(t, h, "sess", doc, body)
		if w.Code != http.StatusOK {
			t.Fatalf("ops on %s: %d %s", doc, w.Code, w.Body.String())
		}
	}

	// gset: g3 is fully covered by g1 and g2.
	post("g", sessionCRDTBody("gset", map[string]any{"id": "g1", "elements": []string{"apple", "banana"}}))
	post("g", sessionCRDTBody("gset", map[string]any{"id": "g2", "elements": []string{"banana", "cherry"}}))
	post("g", sessionCRDTBody("gset", map[string]any{"id": "g3", "elements": []string{"apple"}}))
	if w := postSessionCRDTCompact(t, h, "sess", "g", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("gset", `["apple","banana","cherry"]`, "2", "0") {
		t.Fatalf("gset compact = %d %q", w.Code, w.Body.String())
	}

	// orset: the tombstoned tag and its tombstone cancel out; the operation
	// log does not participate in the trim.
	post("o", sessionCRDTBody("orset", map[string]any{"id": "o1", "action": "add", "element": "apple"}))
	post("o", sessionCRDTBody("orset", map[string]any{"id": "o2", "action": "add", "element": "banana"}))
	post("o", sessionCRDTBody("orset", map[string]any{"id": "o3", "action": "remove", "element": "apple"}))
	if got := getRaw(t, h, "/v1/documents/o/crdt/snapshot").Body.String(); got != compactBody("orset", `["banana"]`, "2", "1") {
		t.Fatalf("orset snapshot = %q", got)
	}
	if w := postSessionCRDTCompact(t, h, "sess", "o", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("orset", `["banana"]`, "1", "0") {
		t.Fatalf("orset compact = %d %q", w.Code, w.Body.String())
	}
	// The merged result is byte-identical before and after compaction.
	if got := getRaw(t, h, "/v1/documents/o/crdt/state").Body.String(); got != "{\"type\":\"orset\",\"value\":[\"banana\"]}\n" {
		t.Fatalf("orset state after compact = %q", got)
	}
}

// Compaction is not a change: it allocates no cursor and writes no change
// record, so the session's change log is untouched.
func TestSessionCRDTCompactLeavesChangeLogAlone(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	_, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	changes := body["changes"].([]any)
	if len(changes) != 1 || changes[0].(map[string]any)["id"] != "c1" {
		t.Fatalf("changes after compaction = %v", changes)
	}
	if body["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor after compaction = %v, want 1", body["nextCursor"])
	}
}

func TestSessionCRDTCompactRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "8", "1", "0") {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()

	// The session must be re-established through HTTP after restart (the
	// device and session rows are durable; this is the same create call,
	// reporting created=false).
	w, _ = postJSON(t, h2, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatalf("re-create session after restart: %d %s", w.Code, w.Body.String())
	}

	// Counts, state and idempotency decisions survive the restart.
	if w := postSessionCRDTCompact(t, h2, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "8", "1", "0") {
		t.Fatalf("re-compact after restart = %d %q", w.Code, w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay after restart = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"results":[{"id":"a1","created":false}]}` {
		t.Fatalf("trimmed replay body after restart = %q", got)
	}
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 9)))
	if w.Code != http.StatusConflict {
		t.Fatalf("changed content after restart = %d, want 409", w.Code)
	}
}
