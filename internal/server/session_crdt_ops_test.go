package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCRDTOpsPath is the session-scoped CRDT batch commit path.
func sessionCRDTOpsPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/ops"
}

// sessionCRDTBody builds a session-scoped CRDT submission body: the declared
// type and the ops array, no device id.
func sessionCRDTBody(typ string, ops ...map[string]any) map[string]any {
	return map[string]any{"type": typ, "ops": ops}
}

// postSessionCRDTOps posts a batch to the session-scoped CRDT ops collection.
func postSessionCRDTOps(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionCRDTOpsPath(session, doc), body)
}

// A first commit on a document with no operations succeeds and fixes the
// type; the merged state is the same as a document-level submission's, with
// the session's owning device as the op's origin.
func TestSessionCRDTOpsHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	// First commit on an unknown document: 200, results in request order, the
	// document-level success shape byte-for-byte.
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[{"id":"a1","created":true},{"id":"a2","created":true}]}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The calling device is the session's owning device: a document-level
	// batch from dev-2 adds to the same per-device sum.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("b1", 3)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Body.String() != `{"type":"counter","value":11}`+"\n" {
		t.Fatalf("state = %q", rec.Body.String())
	}

	// The session device is stamped as the origin: the same id and value
	// resubmitted by dev-1 at the document level is idempotent, while dev-2
	// conflicts — the stored op belongs to dev-1.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("same-device resubmit = %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusConflict {
		t.Fatalf("other-device resubmit = %d, want 409", w.Code)
	}
}

// The session commit's success body is byte-identical to the document-level
// commit's for the same batch.
func TestSessionCRDTOpsHTTPSuccessShapeMatchesDocumentOps(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := postJSON(t, h, "/v1/documents/docA/crdt/ops",
		`{"deviceId":"dev","type":"gset","ops":[{"id":"g1","elements":["b","a"]},{"id":"g2","elements":["c"]}]}`)
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionCRDTOps(t, h, "sess", "docB",
		`{"type":"gset","ops":[{"id":"g1","elements":["b","a"]},{"id":"g2","elements":["c"]}]}`)
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}

// The first accepted batch fixes the document type; a later batch declaring
// another type is a 409 that changes nothing, through the session path just
// as through the document-level one.
func TestSessionCRDTOpsHTTPTypeFixationAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Declaring a different type: 409 JSON, no results, state untouched.
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("gset", crdtGSetOp("g1", "x")))
	if w.Code != http.StatusConflict || body["error"] == nil {
		t.Fatalf("type conflict = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("409 leaked results: %s", w.Body.String())
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	if rec.Body.String() != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after type conflict = %q", rec.Body.String())
	}

	// The same-type batch still commits after the rejected one.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a2", 9)))
	if w.Code != http.StatusOK {
		t.Fatalf("post-conflict commit = %d %s", w.Code, w.Body.String())
	}
}

// The four merge rules behave exactly as at the document-level entry:
// contributions only advance, the union is ascending, the greatest version
// wins and a remove deletes only the adds it observed.
func TestSessionCRDTOpsHTTPMergeRules(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	state := func(doc string) string {
		t.Helper()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", doc), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("state %s = %d %s", doc, rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}

	// Counter: a regressed contribution is a 409 and the state does not move.
	postSessionCRDTOps(t, h, "sess", "doc-c", sessionCRDTBody("counter", crdtCounterOp("c1", 5)))
	w, _ := postSessionCRDTOps(t, h, "sess", "doc-c", sessionCRDTBody("counter", crdtCounterOp("c2", 3)))
	if w.Code != http.StatusConflict {
		t.Fatalf("regressed contribution = %d %s", w.Code, w.Body.String())
	}
	if got := state("doc-c"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("counter after regression = %q", got)
	}

	// GSet: the merge is the ascending union.
	postSessionCRDTOps(t, h, "sess", "doc-g", sessionCRDTBody("gset", crdtGSetOp("g1", "banana", "apple")))
	postSessionCRDTOps(t, h, "sess", "doc-g", sessionCRDTBody("gset", crdtGSetOp("g2", "cherry", "apple")))
	if got := state("doc-g"); got != `{"type":"gset","value":["apple","banana","cherry"]}`+"\n" {
		t.Fatalf("gset = %q", got)
	}

	// Register: the greatest version wins; a stalled version is a 409.
	postSessionCRDTOps(t, h, "sess", "doc-r", sessionCRDTBody("register", crdtRegisterOp("r1", 3, "old")))
	postSessionCRDTOps(t, h, "sess", "doc-r", sessionCRDTBody("register", crdtRegisterOp("r2", 4, "new")))
	if got := state("doc-r"); got != `{"type":"register","value":"new"}`+"\n" {
		t.Fatalf("register = %q", got)
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "doc-r", sessionCRDTBody("register", crdtRegisterOp("r3", 4, "stall")))
	if w.Code != http.StatusConflict {
		t.Fatalf("stalled version = %d %s", w.Code, w.Body.String())
	}

	// ORSet: a remove deletes only the adds accepted before it.
	postSessionCRDTOps(t, h, "sess", "doc-o", sessionCRDTBody("orset", crdtORSetOp("o1", "add", "apple")))
	postSessionCRDTOps(t, h, "sess", "doc-o", sessionCRDTBody("orset", crdtORSetOp("o2", "remove", "apple")))
	if got := state("doc-o"); got != `{"type":"orset","value":[]}`+"\n" {
		t.Fatalf("orset after observed remove = %q", got)
	}
	postSessionCRDTOps(t, h, "sess", "doc-o", sessionCRDTBody("orset", crdtORSetOp("o3", "add", "apple")))
	if got := state("doc-o"); got != `{"type":"orset","value":["apple"]}`+"\n" {
		t.Fatalf("orset after concurrent add = %q", got)
	}
}

// A repeated id is idempotent only when the session device and the content
// both match; any other repeat is a 409 that leaves the state untouched.
func TestSessionCRDTOpsHTTPIdempotentAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	createSessionViaHTTP(t, h, "dev-2", "sess-2")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("x", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical resubmission: created=false, nothing new written.
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("x", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("repost = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["id"] != "x" || r["created"] != false {
		t.Fatalf("repost result = %v", r)
	}

	// Same id, different content: 409 and the batch's other (valid, new)
	// element is not written either — no half batch.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("fresh", 6), crdtCounterOp("x", 7)))
	if w.Code != http.StatusConflict {
		t.Fatalf("content conflict = %d %s", w.Code, w.Body.String())
	}

	// Same id and content but a different session device is a conflict too.
	w, _ = postSessionCRDTOps(t, h, "sess-2", "doc", sessionCRDTBody("counter", crdtCounterOp("x", 5)))
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict = %d %s", w.Code, w.Body.String())
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	if rec.Body.String() != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after conflicts = %q", rec.Body.String())
	}
}

// Malformed bodies are a 400 with zero writes; the document stays without
// any CRDT state afterwards.
func TestSessionCRDTOpsHTTPRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	url := sessionCRDTOpsPath("sess", "doc")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"type":"counter","ops":[{"id":"a","value":1}]}`},
		{"missing content type", "", `{"type":"counter","ops":[{"id":"a","value":1}]}`},
		{"malformed json", "application/json", `{"type":"counter","ops":[`},
		{"trailing content", "application/json", `{"type":"counter","ops":[{"id":"a","value":1}]}garbage`},
		{"second json value", "application/json", `{"type":"counter","ops":[{"id":"a","value":1}]}{"type":"gset","ops":[{"id":"b","elements":["x"]}]}`},
		{"missing type", "application/json", `{"ops":[{"id":"a","value":1}]}`},
		{"unknown type", "application/json", `{"type":"map","ops":[{"id":"a","value":1}]}`},
		{"numeric type", "application/json", `{"type":1,"ops":[{"id":"a","value":1}]}`},
		{"missing ops", "application/json", `{"type":"counter"}`},
		{"null ops", "application/json", `{"type":"counter","ops":null}`},
		{"empty ops", "application/json", `{"type":"counter","ops":[]}`},
		{"ops not an array", "application/json", `{"type":"counter","ops":{"id":"a"}}`},
		{"element not an object", "application/json", `{"type":"counter","ops":[1]}`},
		{"missing id", "application/json", `{"type":"counter","ops":[{"value":1}]}`},
		{"empty id", "application/json", `{"type":"counter","ops":[{"id":"","value":1}]}`},
		{"numeric id", "application/json", `{"type":"counter","ops":[{"id":1,"value":1}]}`},
		{"duplicate ids in batch", "application/json", `{"type":"counter","ops":[{"id":"a","value":1},{"id":"a","value":2}]}`},
		{"counter missing value", "application/json", `{"type":"counter","ops":[{"id":"a"}]}`},
		{"counter negative value", "application/json", `{"type":"counter","ops":[{"id":"a","value":-1}]}`},
		{"counter fractional value", "application/json", `{"type":"counter","ops":[{"id":"a","value":1.5}]}`},
		{"counter string value", "application/json", `{"type":"counter","ops":[{"id":"a","value":"1"}]}`},
		{"gset missing elements", "application/json", `{"type":"gset","ops":[{"id":"a"}]}`},
		{"gset empty elements", "application/json", `{"type":"gset","ops":[{"id":"a","elements":[]}]}`},
		{"gset empty string element", "application/json", `{"type":"gset","ops":[{"id":"a","elements":[""]}]}`},
		{"register missing value", "application/json", `{"type":"register","ops":[{"id":"a","version":1}]}`},
		{"register missing version", "application/json", `{"type":"register","ops":[{"id":"a","value":1}]}`},
		{"register negative version", "application/json", `{"type":"register","ops":[{"id":"a","version":-1,"value":1}]}`},
		{"orset bad action", "application/json", `{"type":"orset","ops":[{"id":"a","action":"del","element":"x"}]}`},
		{"orset missing element", "application/json", `{"type":"orset","ops":[{"id":"a","action":"add"}]}`},
		{"second element invalid", "application/json", `{"type":"counter","ops":[{"id":"ok","value":1},{"id":"bad"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, url, tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: every rejected request left the document without CRDT
	// state, including the batch whose first element was valid.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)
}

// A session that never existed and a session that was deleted are both a 404
// with no results; request shape is judged before the session lookup.
func TestSessionCRDTOpsHTTPSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, body := postSessionCRDTOps(t, h, "ghost", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("404 leaked results: %s", w.Body.String())
	}

	// A malformed batch against an unknown session is still a 400.
	w, _ = postSessionCRDTOps(t, h, "ghost", "doc", `{"type":"counter","ops":[{"id":"a","value":1}]}trailing`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> commits through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	// Zero writes: the document still has no CRDT state.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)
}

// A revoked permission for the session's device is a 403 with zero writes;
// session existence precedes the permission check and re-granting restores
// the commit path.
func TestSessionCRDTOpsHTTPPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("403 leaked results: %s", w.Body.String())
	}

	// An unknown session against a revoked document is a 404, not a 403.
	w, _ = postSessionCRDTOps(t, h, "ghost", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Zero writes: the document still has no CRDT state.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)

	// Re-grant restores the commit path.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted commit = %d %s", w.Code, w.Body.String())
	}
	if r := body["results"].([]any)[0].(map[string]any); r["created"] != true {
		t.Fatalf("re-granted result = %v", r)
	}
}

// Verbs other than POST on the ops path, empty identifiers, a trailing slash
// and extra segments are a JSON 400 — never a redirect or an HTML page.
func TestSessionCRDTOpsHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		w, _ := doRequest(t, h, method, sessionCRDTOpsPath("sess", "doc"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	for _, p := range []string{
		"/v1/sessions//documents/doc/crdt/ops",
		"/v1/sessions/sess/documents//crdt/ops",
		"/v1/sessions/sess/documents/doc/crdt/ops/",
		"/v1/sessions/sess/documents/doc/crdt/ops/extra",
		"/v1/sessions/sess/documents/doc/crdt/ops/extra/more",
		"/v1/sessions/sess/documents/doc/crdt",
		"/v1/sessions/sess/documents/doc/crdt/",
		"/v1/sessions/sess/documents/doc/crdt/other",
	} {
		r := newJSONRequest(http.MethodPost, p, `{"type":"counter","ops":[{"id":"a","value":1}]}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("POST %s status = %d, want 400, body = %q", p, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("POST %s redirected to %q", p, loc)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
			t.Fatalf("POST %s leaked HTML: %q", p, w.Body.String())
		}
	}

	// Zero writes: the document still has no CRDT state.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	assertJSONErrorStatus(t, rec, http.StatusNotFound)
}

// A batch committed through the session endpoint pushes the new merged state
// to the document's subscribers like any other write path; an idempotent
// repeat and a rejected batch push nothing.
func TestSessionCRDTOpsHTTPPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	// The first state-changing commit pushes the new merged state.
	if code := postHTTP(t, srv, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a1", 9))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 9 {
		t.Fatalf("frame = %d, want 9", n)
	}

	// An idempotent repeat changes nothing and pushes nothing.
	if code := postHTTP(t, srv, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a1", 9))); code != http.StatusOK {
		t.Fatalf("idempotent resubmit = %d", code)
	}
	// A rejected batch (regressed contribution) pushes nothing either.
	if code := postHTTP(t, srv, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a2", 1))); code != http.StatusConflict {
		t.Fatalf("regressed submit = %d, want 409", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("idempotent or rejected batch pushed a frame")
	}
	conn.clearReadDeadline()

	// A genuine advance pushes again.
	if code := postHTTP(t, srv, sessionCRDTOpsPath("sess", "doc"),
		sessionCRDTBody("counter", crdtCounterOp("a3", 12))); code != http.StatusOK {
		t.Fatalf("advancing submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 12 {
		t.Fatalf("frame = %d, want 12", n)
	}
}

// Concurrent batches through the session endpoint serialize: every batch
// commits exactly once and the merged state reflects all of them.
func TestSessionCRDTOpsHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("gset",
				crdtGSetOp(fmt.Sprintf("op-%02d", i), fmt.Sprintf("el-%02d", i))))
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("post %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every element landed exactly once in the union.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	var state struct {
		Type  string   `json:"type"`
		Value []string `json:"value"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
		t.Fatal(rec.Body.String())
	}
	if state.Type != "gset" || len(state.Value) != n {
		t.Fatalf("merged %d elements, want %d: %s", len(state.Value), n, rec.Body.String())
	}
}

// Type fixation, idempotency and conflict decisions and the merged state all
// survive a process restart.
func TestSessionCRDTOpsHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-crdt-ops.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Idempotency survives the restart: same session device and value.
	w, body := postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if r := body["results"].([]any)[0].(map[string]any); r["created"] != false {
		t.Fatalf("idempotent after restart = %v", r)
	}

	// So do the conflict decision and the type fixation.
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 6)))
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("gset", crdtGSetOp("g1", "x")))
	if w.Code != http.StatusConflict {
		t.Fatalf("type conflict after restart = %d %s", w.Code, w.Body.String())
	}

	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Body.String() != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after restart = %q", rec.Body.String())
	}
}

// An extra deviceId field in the body is ignored: the calling device is
// always the session's owning device, never a body field.
func TestSessionCRDTOpsHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", map[string]any{
		"deviceId": "intruder",
		"type":     "counter",
		"ops":      []any{crdtCounterOp("a1", 5)},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	// The stored op belongs to the session device: resubmitting the same id
	// and value as dev is idempotent, as "intruder" it would conflict.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("same-device resubmit = %d %s", w.Code, w.Body.String())
	}
}

// Identifiers literally named "crdt"/"ops" stay ordinary identifiers on the
// session ops path.
func TestSessionCRDTOpsHTTPKeywordIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "crdt")

	w, _ := postSessionCRDTOps(t, h, "crdt", "ops", sessionCRDTBody("counter", crdtCounterOp("a1", 4)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit to doc ops = %d %s", w.Code, w.Body.String())
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/v1/sessions/crdt/documents/ops/crdt/state", nil))
	if rec.Body.String() != `{"type":"counter","value":4}`+"\n" {
		t.Fatalf("state = %q", rec.Body.String())
	}
}

// The document-level commit is untouched by the new entry: it still requires
// its body deviceId and answers with its own shapes.
func TestSessionCRDTOpsHTTPDocumentOpsUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", `{"type":"counter","ops":[{"id":"a","value":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("document ops without deviceId = %d, want 400", w.Code)
	}

	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("a", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if r := body["results"].([]any)[0].(map[string]any); r["created"] != true {
		t.Fatalf("document ops result = %v", r)
	}

	// The session path reads back exactly what the document path wrote.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	if rec.Body.String() != `{"type":"counter","value":1}`+"\n" {
		t.Fatalf("session read = %q", rec.Body.String())
	}
}
