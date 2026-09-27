package server

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

// A counter compacted through the session view trims to each device's maximum
// contribution; the success body is byte-for-byte the document-level snapshot
// read, the merge is untouched and a repeat is a no-op returning the same
// body.
func TestSessionCRDTCompactCounterEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatalf("session submit: %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("b1", 3)))
	if w.Code != http.StatusOK {
		t.Fatalf("document submit: %d %s", w.Code, w.Body.String())
	}

	stateBefore := getRaw(t, h, "/v1/documents/doc/crdt/state").Body.String()
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compactBody("counter", "11", "3", "0") {
		t.Fatalf("snapshot before compact = %q", got)
	}

	// The body only has to be one legal JSON value; a stray deviceId is
	// ignored and never overrides the session's owning device.
	w = postSessionCRDTCompact(t, h, "sess", "doc", `{"deviceId":"someone-else"}`)
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

	// The compact response, the document-level snapshot read and the merge
	// are byte-for-byte stable across the compaction.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != want {
		t.Fatalf("snapshot after compact = %q, want %q", got, want)
	}
	if got := getRaw(t, h, "/v1/documents/doc/crdt/state").Body.String(); got != stateBefore {
		t.Fatalf("state changed by compaction: %q -> %q", stateBefore, got)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if !bytes.Equal(rec.Body.Bytes(), []byte(stateBefore)) {
		t.Fatalf("session state after compact = %q, want %q", rec.Body.Bytes(), stateBefore)
	}

	// Repeating the compaction is not an error and reports the same body.
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != want {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}
}

// The gset, register and orset trim rules are the document-level
// compaction's, driven here through the session entry.
func TestSessionCRDTCompactGSetRegisterORSet(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	postOps := func(doc string, body any) {
		t.Helper()
		w, _ := postSessionCRDTOps(t, h, "sess", doc, body)
		if w.Code != http.StatusOK {
			t.Fatalf("submit to %s: %d %s", doc, w.Code, w.Body.String())
		}
	}

	// gset: g3 is fully covered by g1 and g2.
	postOps("g", sessionCRDTBody("gset", crdtGSetOp("g1", "apple", "banana")))
	postOps("g", sessionCRDTBody("gset", crdtGSetOp("g2", "banana", "cherry")))
	postOps("g", sessionCRDTBody("gset", crdtGSetOp("g3", "apple")))
	if got := postSessionCRDTCompact(t, h, "sess", "g", `{}`).Body.String(); got != compactBody("gset", `["apple","banana","cherry"]`, "2", "0") {
		t.Fatalf("gset compact = %q", got)
	}

	// register: r1 is superseded by r2 from the same device.
	postOps("r", sessionCRDTBody("register", crdtRegisterOp("r1", 1, "first")))
	postOps("r", sessionCRDTBody("register", crdtRegisterOp("r2", 2, "second")))
	if got := postSessionCRDTCompact(t, h, "sess", "r", `{}`).Body.String(); got != compactBody("register", `"second"`, "1", "0") {
		t.Fatalf("register compact = %q", got)
	}

	// orset: the tombstoned tag and its tombstone cancel out; the operation
	// log is untouched, so replaying either op stays idempotent.
	postOps("o", sessionCRDTBody("orset", crdtORSetOp("o1", "add", "apple")))
	postOps("o", sessionCRDTBody("orset", crdtORSetOp("o2", "add", "banana")))
	postOps("o", sessionCRDTBody("orset", crdtORSetOp("o3", "remove", "apple")))
	if got := getRaw(t, h, "/v1/documents/o/crdt/snapshot").Body.String(); got != compactBody("orset", `["banana"]`, "2", "1") {
		t.Fatalf("orset snapshot = %q", got)
	}
	if got := postSessionCRDTCompact(t, h, "sess", "o", `{}`).Body.String(); got != compactBody("orset", `["banana"]`, "1", "0") {
		t.Fatalf("orset compact = %q", got)
	}
	w, body := postSessionCRDTOps(t, h, "sess", "o", sessionCRDTBody("orset", crdtORSetOp("o1", "add", "apple")))
	if w.Code != http.StatusOK {
		t.Fatalf("orset replay after compact = %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != false {
		t.Fatalf("orset replay result = %v, want created=false", result)
	}
	if got := getRaw(t, h, "/v1/documents/o/crdt/state").Body.String(); got != "{\"type\":\"orset\",\"value\":[\"banana\"]}\n" {
		t.Fatalf("orset state after replay = %q", got)
	}
}

// Every legal JSON value is an acceptable compaction body; its content never
// affects the result.
func TestSessionCRDTCompactAcceptsAnyJSONBody(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The first legal body trims the superseded contribution; every later
	// legal body is an idempotent repeat against the same trimmed state.
	want := compactBody("counter", "8", "1", "0")
	for _, raw := range []string{
		`{}`,
		`{"deviceId":"dev-1"}`,
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
		if got := w.Body.String(); got != want {
			t.Fatalf("body %s -> %q, want %q", raw, got, want)
		}
	}
}

// Shape failures are 400 JSON errors with zero writes and precede the
// session lookup: a malformed body against an unknown session is still a
// 400.
func TestSessionCRDTCompactBodyValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	before := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String()

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
			assertJSONErrorStatus(t, w, http.StatusBadRequest)
		})
	}

	// Every rejection wrote nothing.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != before {
		t.Fatalf("snapshot changed by rejected compacts: %q -> %q", before, got)
	}
}

// Wrong methods and malformed paths are 400 JSON errors — never a redirect
// or HTML — and identifiers literally named "compact" stay ordinary
// identifiers.
func TestSessionCRDTCompactPathAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"wrong method GET", http.MethodGet, sessionCRDTCompactPath("sess", "doc")},
		{"wrong method DELETE", http.MethodDelete, sessionCRDTCompactPath("sess", "doc")},
		{"wrong method PUT", http.MethodPut, sessionCRDTCompactPath("sess", "doc")},
		{"empty session id", http.MethodPost, "/v1/sessions//documents/doc/crdt/compact"},
		{"empty document id", http.MethodPost, "/v1/sessions/sess/documents//crdt/compact"},
		{"trailing slash", http.MethodPost, sessionCRDTCompactPath("sess", "doc") + "/"},
		{"extra segment", http.MethodPost, sessionCRDTCompactPath("sess", "doc") + "/x"},
		{"bare crdt namespace", http.MethodPost, "/v1/sessions/sess/documents/doc/crdt"},
		{"unknown crdt verb", http.MethodPost, "/v1/sessions/sess/documents/doc/crdt/compacts"},
		{"wrong method, unknown session", http.MethodGet, sessionCRDTCompactPath("ghost", "doc")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			assertJSONErrorStatus(t, w, http.StatusBadRequest)
		})
	}

	// A session literally named "compact" is an ordinary identifier.
	createSessionViaHTTP(t, h, "dev-2", "compact")
	w, _ := postSessionCRDTOps(t, h, "compact", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 4)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit via session compact: %d %s", w.Code, w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "compact", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "4", "1", "0") {
		t.Fatalf("session named compact = %d %q", w.Code, w.Body.String())
	}

	// A document literally named "compact" is an ordinary identifier.
	w, _ = postSessionCRDTOps(t, h, "sess", "compact", sessionCRDTBody("counter", crdtCounterOp("a1", 2)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit to doc compact: %d %s", w.Code, w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "sess", "compact", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "2", "1", "0") {
		t.Fatalf("document named compact = %d %q", w.Code, w.Body.String())
	}
}

// The checks run in the fixed order request shape, session existence,
// document permission, CRDT state existence; every rejection writes nothing.
func TestSessionCRDTCompactGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A second document the session's device is revoked on, with state.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc-rev", sessionCRDTBody("counter", crdtCounterOp("b1", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc-rev/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A session that never existed and one that was deleted are both 404.
	gw, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "gone"})
	if gw.Code != http.StatusOK {
		t.Fatalf("create gone session: %d %s", gw.Code, gw.Body.String())
	}
	dw, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/gone")
	if dw.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", dw.Code, dw.Body.String())
	}
	assertJSONErrorStatus(t, postSessionCRDTCompact(t, h, "ghost", "doc", `{}`), http.StatusNotFound)
	assertJSONErrorStatus(t, postSessionCRDTCompact(t, h, "gone", "doc", `{}`), http.StatusNotFound)

	// Revoked permission is a 403 after the shape and session checks pass.
	assertJSONErrorStatus(t, postSessionCRDTCompact(t, h, "sess", "doc-rev", `{}`), http.StatusForbidden)

	// A document with no committed CRDT operation is a 404, even when it has
	// ordinary change-log content.
	w, _ = postJSON(t, h, "/v1/documents/doc-empty/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	assertJSONErrorStatus(t, postSessionCRDTCompact(t, h, "sess", "doc-empty", `{}`), http.StatusNotFound)

	// The rejections wrote nothing: doc still compacts as the first trim and
	// doc-rev's state is untouched.
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compactBody("counter", "8", "1", "0") {
		t.Fatalf("doc compact after rejections = %d %q", w.Code, w.Body.String())
	}
	if got := getRaw(t, h, "/v1/documents/doc-rev/crdt/snapshot").Body.String(); got != compactBody("counter", "1", "1", "0") {
		t.Fatalf("doc-rev snapshot after rejections = %q", got)
	}
}

// A trimmed id keeps its source device and comparison-content digest: a
// matching replay through the session commit is idempotent (created=false),
// while a changed content or source device is a 409 with zero writes.
func TestSessionCRDTCompactTrimmedIDReplayAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// The trimmed a1 replays idempotently through the session commit: 200
	// created=false and no new merge effect.
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay = %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["id"] != "a1" || result["created"] != false {
		t.Fatalf("trimmed replay result = %v, want created=false", result)
	}

	// Same id, changed comparison content: 409 JSON, zero writes — the new op
	// batched with it is rejected too.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 9), crdtCounterOp("c1", 10)))
	if w.Code != http.StatusConflict {
		t.Fatalf("changed content = %d, want 409", w.Code)
	}
	// Same id, changed source device: 409 as well, even though the op row is
	// gone.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusConflict {
		t.Fatalf("changed device = %d, want 409", w.Code)
	}

	// The failed batches moved nothing: the merge still derives from one
	// stored operation, and the batched c1 was never written.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compactBody("counter", "8", "1", "0") {
		t.Fatalf("snapshot after replays = %q", got)
	}
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 10)))
	if w.Code != http.StatusOK {
		t.Fatalf("c1 resubmit = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("c1 was written by the rejected batch: %v", body)
	}
}

// Compaction touches only the CRDT state layer: it allocates no change
// cursor and writes no change record.
func TestSessionCRDTCompactLeavesChangeLogAlone(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c1", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w := postSessionCRDTCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	changes := body["changes"].([]any)
	if len(changes) != 1 || changes[0].(map[string]any)["id"] != "c1" {
		t.Fatalf("changes after compaction = %v", changes)
	}
	if body["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor after compaction = %v, want 1", body["nextCursor"])
	}
}

// Compaction is not a state change: a live CRDT state subscription receives
// no frame for it, and the next frame after a real submit is that submit's
// merge.
func TestSessionCRDTCompactPushesNothing(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/crdt/ops",
		sessionCRDTBody("counter", crdtCounterOp("a1", 5), crdtCounterOp("a2", 8))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if hs.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %d", hs.StatusCode)
	}
	defer conn.close()
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 8 {
		t.Fatalf("initial frame = %d, want 8", n)
	}

	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/crdt/compact", map[string]any{}); code != http.StatusOK {
		t.Fatalf("compact = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("the compaction pushed a frame")
	}
	conn.clearReadDeadline()

	// The next frame belongs to the next real merge change.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a3", 9))); code != http.StatusOK {
		t.Fatalf("submit after compact = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 9 {
		t.Fatalf("frame after compact = %d, want 9", n)
	}
}

// After a process restart the compacted counts, the merge and the trimmed-id
// idempotency decisions are unchanged.
func TestSessionCRDTCompactRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-crdt-compact.db")

	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	createSessionViaHTTP(t, h1, "dev-1", "sess")
	w, _ := postSessionCRDTOps(t, h1, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = postSessionCRDTCompact(t, h1, "sess", "doc", `{}`)
	if w.Code != http.StatusOK || w.Body.String() != compactBody("counter", "8", "1", "0") {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}
	compacted := w.Body.String()
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	// The session row is durable; re-creating it reports created=false.
	w, body := postJSON(t, h2, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK || body["created"] != false {
		t.Fatalf("re-create session after restart = %d %v", w.Code, body)
	}

	// Counts, the merge and the repeat-compaction body survive the restart.
	if got := getRaw(t, h2, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compacted {
		t.Fatalf("snapshot after restart = %q, want %q", got, compacted)
	}
	if w := postSessionCRDTCompact(t, h2, "sess", "doc", `{}`); w.Code != http.StatusOK ||
		w.Body.String() != compacted {
		t.Fatalf("re-compact after restart = %d %q", w.Code, w.Body.String())
	}
	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Body.String() != `{"type":"counter","value":8}`+"\n" {
		t.Fatalf("state after restart = %q", rec.Body.String())
	}

	// The trimmed id still replays idempotently, and a regression is still a
	// 409.
	w, body = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("trimmed replay after restart = %d %v", w.Code, body)
	}
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a3", 7)))
	if w.Code != http.StatusConflict {
		t.Fatalf("regression after restart = %d, want 409", w.Code)
	}
}
