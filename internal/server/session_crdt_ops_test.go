package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCRDTOpsPath is the session-scoped CRDT ops collection path.
func sessionCRDTOpsPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/crdt/ops"
}

// sessionCRDTBody builds a session-scoped CRDT batch body: only the declared
// type and the ops array — no device field travels in the body.
func sessionCRDTBody(typ string, ops ...map[string]any) map[string]any {
	return map[string]any{"type": typ, "ops": ops}
}

// postSessionCRDTOps posts a batch to the session-scoped CRDT ops collection.
func postSessionCRDTOps(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionCRDTOpsPath(session, doc), body)
}

// The first session-scoped commit on an unknown document fixes its type and
// answers with the document-level success shape byte-for-byte; the stored
// operations name the session's owning device.
func TestSessionCRDTOpsSuccessCounter(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("a1", 5), crdtCounterOp("a2", 8)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[{"id":"a1","created":true},{"id":"a2","created":true}]}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The merged state reads back through both the session and the document
	// path, byte-identical.
	if got := crdtStateBody(t, h, "doc"); got != `{"type":"counter","value":8}`+"\n" {
		t.Fatalf("document state = %q", got)
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionCRDTStatePath("sess", "doc"))
	if w.Body.String() != `{"type":"counter","value":8}`+"\n" {
		t.Fatalf("session state = %q", w.Body.String())
	}

	// The calling device is the session's owning device: an identical repost
	// through the same session is idempotent, while another session's device
	// carrying the same id and content conflicts.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("repost = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("identical repost = %s", w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess-2", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusConflict {
		t.Fatalf("cross-device repost = %d, want 409, body = %s", w.Code, w.Body.String())
	}
}

// The session commit's success body is byte-identical to the document-level
// commit's for the same batch.
func TestSessionCRDTOpsSuccessShapeMatchesDocumentPost(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := postJSON(t, h, "/v1/documents/docA/crdt/ops",
		`{"deviceId":"dev","type":"gset","ops":[{"id":"g1","elements":["b","a"]}]}`)
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionCRDTOps(t, h, "sess", "docB",
		`{"type":"gset","ops":[{"id":"g1","elements":["b","a"]}]}`)
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}

// Every CRDT type can be driven through the session path with the
// document-level merge rules.
func TestSessionCRDTOpsAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// gset: union rendered ascending.
	w, _ := postSessionCRDTOps(t, h, "sess", "doc-gset", sessionCRDTBody("gset",
		crdtGSetOp("g1", "banana", "apple"), crdtGSetOp("g2", "cherry", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateBody(t, h, "doc-gset"); got != `{"type":"gset","value":["apple","banana","cherry"]}`+"\n" {
		t.Fatalf("gset state = %q", got)
	}

	// register: greatest version wins.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc-register", sessionCRDTBody("register",
		map[string]any{"id": "r1", "version": 1, "value": "first"},
		map[string]any{"id": "r2", "version": 2, "value": map[string]any{"k": true}}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateBody(t, h, "doc-register"); got != `{"type":"register","value":{"k":true}}`+"\n" {
		t.Fatalf("register state = %q", got)
	}

	// orset: a remove tombstones only the adds it observed.
	w, _ = postSessionCRDTOps(t, h, "sess", "doc-orset", sessionCRDTBody("orset",
		crdtORSetOp("a1", "add", "apple"),
		crdtORSetOp("a2", "add", "banana"),
		crdtORSetOp("r1", "remove", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateBody(t, h, "doc-orset"); got != `{"type":"orset","value":["banana"]}`+"\n" {
		t.Fatalf("orset state = %q", got)
	}
}

// The first accepted batch fixes the document type through the session path
// too; a later batch declaring another type is a 409 that moves nothing.
func TestSessionCRDTOpsTypeFixation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("c1", 2)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for _, body := range []map[string]any{
		sessionCRDTBody("gset", crdtGSetOp("g1", "x")),
		sessionCRDTBody("register", map[string]any{"id": "r1", "version": 1, "value": 1}),
		sessionCRDTBody("orset", crdtORSetOp("o1", "add", "x")),
	} {
		w, body := postSessionCRDTOps(t, h, "sess", "doc", body)
		if w.Code != http.StatusConflict || body["error"] == nil {
			t.Fatalf("type conflict = %d %s", w.Code, w.Body.String())
		}
	}
	if got := crdtStateBody(t, h, "doc"); got != `{"type":"counter","value":2}`+"\n" {
		t.Fatalf("state after type conflicts = %q", got)
	}
}

// Idempotency and conflict judgments match the document-level entry: an
// identical repost is created=false, any content or origin difference is a
// 409, and rejected batches leave no half batch behind.
func TestSessionCRDTOpsIdempotentAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical repost: created=false, nothing new written.
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatalf("repost = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("repost result = %s", w.Body.String())
	}

	// Same id, different value: 409; the batch's other valid op is not
	// written either.
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter",
		crdtCounterOp("fresh", 6), crdtCounterOp("a1", 9)))
	if w.Code != http.StatusConflict || body["error"] == nil {
		t.Fatalf("value conflict = %d %s", w.Code, w.Body.String())
	}

	// The conflicting batch wrote nothing: "fresh" is still a new id.
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("fresh", 6)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("fresh after rejected batch = %s", w.Body.String())
	}
	if got := crdtStateBody(t, h, "doc"); got != `{"type":"counter","value":6}`+"\n" {
		t.Fatalf("state = %q", got)
	}
}

// Counter regressions and register version stalls are 409 through the session
// path, exactly as at the document-level entry.
func TestSessionCRDTOpsRegressionAndStall(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc-counter", sessionCRDTBody("counter", crdtCounterOp("c1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionCRDTOps(t, h, "sess", "doc-counter", sessionCRDTBody("counter", crdtCounterOp("c2", 4)))
	if w.Code != http.StatusConflict {
		t.Fatalf("counter regression = %d, want 409", w.Code)
	}
	if got := crdtStateBody(t, h, "doc-counter"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("counter state = %q", got)
	}

	w, _ = postSessionCRDTOps(t, h, "sess", "doc-register", sessionCRDTBody("register",
		map[string]any{"id": "r1", "version": 5, "value": "five"}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for _, op := range []map[string]any{
		{"id": "r2", "version": 4, "value": "four"},
		{"id": "r3", "version": 5, "value": "stall"},
	} {
		w, _ = postSessionCRDTOps(t, h, "sess", "doc-register", sessionCRDTBody("register", op))
		if w.Code != http.StatusConflict {
			t.Fatalf("register %v = %d, want 409", op["version"], w.Code)
		}
	}
	if got := crdtStateBody(t, h, "doc-register"); got != `{"type":"register","value":"five"}`+"\n" {
		t.Fatalf("register state = %q", got)
	}
}

func TestSessionCRDTOpsRejectsBadInput(t *testing.T) {
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
		{"content type with json suffix", "application/vnd.api+json", `{"type":"counter","ops":[{"id":"a","value":1}]}`},
		{"malformed json", "application/json", `{"type":"counter","ops":[`},
		{"trailing content", "application/json", `{"type":"counter","ops":[{"id":"a","value":1}]}garbage`},
		{"second json value", "application/json", `{"type":"counter","ops":[{"id":"a","value":1}]}{"type":"gset","ops":[{"id":"b","elements":["x"]}]}`},
		{"missing type", "application/json", `{"ops":[{"id":"a","value":1}]}`},
		{"bad type", "application/json", `{"type":"list","ops":[{"id":"a","value":1}]}`},
		{"numeric type", "application/json", `{"type":7,"ops":[{"id":"a","value":1}]}`},
		{"missing ops", "application/json", `{"type":"counter"}`},
		{"null ops", "application/json", `{"type":"counter","ops":null}`},
		{"empty ops", "application/json", `{"type":"counter","ops":[]}`},
		{"ops not an array", "application/json", `{"type":"counter","ops":{"id":"a"}}`},
		{"element not an object", "application/json", `{"type":"counter","ops":[1]}`},
		{"missing id", "application/json", `{"type":"counter","ops":[{"value":1}]}`},
		{"empty id", "application/json", `{"type":"counter","ops":[{"id":"","value":1}]}`},
		{"numeric id", "application/json", `{"type":"counter","ops":[{"id":123,"value":1}]}`},
		{"duplicate ids in batch", "application/json", `{"type":"counter","ops":[{"id":"a","value":1},{"id":"a","value":2}]}`},
		{"counter missing value", "application/json", `{"type":"counter","ops":[{"id":"a"}]}`},
		{"counter fractional value", "application/json", `{"type":"counter","ops":[{"id":"a","value":1.5}]}`},
		{"counter negative value", "application/json", `{"type":"counter","ops":[{"id":"a","value":-1}]}`},
		{"counter string value", "application/json", `{"type":"counter","ops":[{"id":"a","value":"1"}]}`},
		{"counter null value", "application/json", `{"type":"counter","ops":[{"id":"a","value":null}]}`},
		{"gset missing elements", "application/json", `{"type":"gset","ops":[{"id":"a"}]}`},
		{"gset empty elements", "application/json", `{"type":"gset","ops":[{"id":"a","elements":[]}]}`},
		{"gset empty element string", "application/json", `{"type":"gset","ops":[{"id":"a","elements":["x",""]}]}`},
		{"gset non-string element", "application/json", `{"type":"gset","ops":[{"id":"a","elements":["x",1]}]}`},
		{"register missing value", "application/json", `{"type":"register","ops":[{"id":"a","version":1}]}`},
		{"register missing version", "application/json", `{"type":"register","ops":[{"id":"a","value":1}]}`},
		{"register null version", "application/json", `{"type":"register","ops":[{"id":"a","value":1,"version":null}]}`},
		{"register fractional version", "application/json", `{"type":"register","ops":[{"id":"a","value":1,"version":1.5}]}`},
		{"register negative version", "application/json", `{"type":"register","ops":[{"id":"a","value":1,"version":-1}]}`},
		{"orset missing action", "application/json", `{"type":"orset","ops":[{"id":"a","element":"x"}]}`},
		{"orset bad action", "application/json", `{"type":"orset","ops":[{"id":"a","action":"reset","element":"x"}]}`},
		{"orset missing element", "application/json", `{"type":"orset","ops":[{"id":"a","action":"add"}]}`},
		{"orset empty element", "application/json", `{"type":"orset","ops":[{"id":"a","action":"add","element":""}]}`},
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

	// Zero writes: every rejected request left the document without CRDT state.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("zero-write violated: state = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCRDTOpsSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Never-created session -> 404 JSON with no results.
	w, body := postSessionCRDTOps(t, h, "ghost", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("404 leaked results: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed batch against an
	// unknown session is still a 400.
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

	// Zero writes: nothing reached the CRDT state.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("zero-write violated: state = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCRDTOpsPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no results and zero writes.
	w, body := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("403 leaked results: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionCRDTOps(t, h, "ghost", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("zero-write violated: state = %d %s", w.Code, w.Body.String())
	}

	// Re-grant restores the commit path.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a", 1)))
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted commit = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("re-granted result = %s", w.Body.String())
	}
}

func TestSessionCRDTOpsRejectsBadMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Verbs other than POST on the collection path are a JSON 400.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		w, _ := doRequest(t, h, method, sessionCRDTOpsPath("sess", "doc"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash and extra segments are a JSON 400,
	// never a redirect or an HTML page.
	for _, p := range []string{
		"/v1/sessions//documents/doc/crdt/ops",
		"/v1/sessions/sess/documents//crdt/ops",
		"/v1/sessions/sess/documents/doc/crdt/ops/",
		"/v1/sessions/sess/documents/doc/crdt/ops/extra",
		"/v1/sessions/sess/documents/doc/crdt/ops/extra/more",
		"/v1/sessions/sess/documents/doc/crdt",
		"/v1/sessions/sess/documents/doc/crdt/",
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
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("zero-write violated: state = %d %s", w.Code, w.Body.String())
	}
}

// Identifiers literally named "ops"/"crdt" stay ordinary identifiers on the
// session CRDT ops path.
func TestSessionCRDTOpsKeywordIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "ops")

	w, _ := postSessionCRDTOps(t, h, "ops", "crdt", sessionCRDTBody("counter", crdtCounterOp("a1", 4)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit to doc crdt via session ops: %d %s", w.Code, w.Body.String())
	}
	if got := crdtStateBody(t, h, "crdt"); got != `{"type":"counter","value":4}`+"\n" {
		t.Fatalf("state = %q", got)
	}

	w, _ = postSessionCRDTOps(t, h, "ops", "ops", sessionCRDTBody("counter", crdtCounterOp("a1", 7)))
	if w.Code != http.StatusOK {
		t.Fatalf("submit to doc ops: %d %s", w.Code, w.Body.String())
	}
	if got := crdtStateBody(t, h, "ops"); got != `{"type":"counter","value":7}`+"\n" {
		t.Fatalf("state = %q", got)
	}
}

// A batch committed through the session-scoped endpoint pushes the new merged
// state to the document's CRDT subscribers; idempotent repeats and rejected
// batches push nothing.
func TestSessionCRDTOpsPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	opsPath := "/v1/sessions/sess/documents/doc/crdt/ops"

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	// The first commit fixes the type and pushes the first state.
	if code := postHTTP(t, srv, opsPath, sessionCRDTBody("counter", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 5 {
		t.Fatalf("frame = %d, want 5", n)
	}

	// An idempotent repeat pushes nothing.
	if code := postHTTP(t, srv, opsPath, sessionCRDTBody("counter", crdtCounterOp("a1", 5))); code != http.StatusOK {
		t.Fatalf("idempotent resubmit = %d", code)
	}
	// A rejected regression pushes nothing either.
	if code := postHTTP(t, srv, opsPath, sessionCRDTBody("counter", crdtCounterOp("a2", 4))); code != http.StatusConflict {
		t.Fatalf("regression = %d, want 409", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("an idempotent or rejected commit pushed a frame")
	}
	conn.clearReadDeadline()

	// An advancing contribution pushes the new sum.
	if code := postHTTP(t, srv, opsPath, sessionCRDTBody("counter", crdtCounterOp("a3", 8))); code != http.StatusOK {
		t.Fatalf("advance = %d", code)
	}
	if n := crdtCounterValue(t, conn.readCRDTState()); n != 8 {
		t.Fatalf("frame = %d, want 8", n)
	}
}

// CRDT commits through the session path produce no change-log records and
// allocate no cursors.
func TestSessionCRDTOpsLeavesChangeLogAlone(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionCRDTOps(t, h, "sess", "doc", sessionCRDTBody("gset", crdtGSetOp("g1", "a", "b")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("change log touched: %s", w.Body.String())
	}
}

func TestSessionCRDTOpsPersistenceAcrossRestart(t *testing.T) {
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

	// Idempotency survives the restart.
	w, body := postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("idempotent after restart = %s", w.Body.String())
	}

	// So do the conflict judgment and the type fixation.
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a1", 6)))
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d", w.Code)
	}
	w, _ = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("gset", crdtGSetOp("g1", "x")))
	if w.Code != http.StatusConflict {
		t.Fatalf("type fixation after restart = %d", w.Code)
	}

	// The merged state is unchanged and new ops still commit.
	if got := crdtStateBody(t, h2, "doc"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after restart = %q", got)
	}
	w, body = postSessionCRDTOps(t, h2, "sess", "doc", sessionCRDTBody("counter", crdtCounterOp("a2", 9)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("post-restart commit = %s", w.Body.String())
	}
	if got := crdtStateBody(t, h2, "doc"); got != `{"type":"counter","value":9}`+"\n" {
		t.Fatalf("state after post-restart commit = %q", got)
	}
}

// The document-level CRDT commit is untouched by the new entry: it still
// requires its body deviceId and answers with its own shapes.
func TestSessionCRDTOpsDocumentPostUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", `{"type":"counter","ops":[{"id":"a","value":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("document post without deviceId = %d, want 400", w.Code)
	}

	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev", crdtCounterOp("a", 1)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("document post result = %s", w.Body.String())
	}

	// The session path reads back exactly what the document path wrote.
	w, _ = doRequest(t, h, http.MethodGet, sessionCRDTStatePath("sess", "doc"))
	if w.Body.String() != `{"type":"counter","value":1}`+"\n" {
		t.Fatalf("session read = %q", w.Body.String())
	}
}
