package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// crdtStateBody reads the document-level CRDT state and returns the raw body,
// failing the test on any non-200 response.
func crdtStateBody(t *testing.T, h http.Handler, doc string) string {
	t.Helper()
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state %s = %d %s", doc, w.Code, w.Body.String())
	}
	return w.Body.String()
}

// submitCRDT posts one CRDT batch and returns the recorder.
func submitCRDT(t *testing.T, h http.Handler, doc string, body any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/crdt/ops", body)
	return w
}

// postJSONResult decodes results[0] from a recorded ops response, failing the
// test unless the response is a 200 with exactly one result.
func postJSONResult(t *testing.T, w *httptest.ResponseRecorder) (string, bool) {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("submit = %d %s", w.Code, w.Body.String())
	}
	var decoded struct {
		Results []struct {
			ID      string `json:"id"`
			Created bool   `json:"created"`
		} `json:"results"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode results: %v body=%s", err, w.Body.String())
	}
	if len(decoded.Results) != 1 {
		t.Fatalf("results = %s, want exactly one entry", w.Body.String())
	}
	return decoded.Results[0].ID, decoded.Results[0].Created
}

// Counter idempotency and conflict judgments: an identical replay is
// created=false, the same id with a different value or from another device is
// a 409, and none of the rejections move the merged state.
func TestCRDTCounterIdempotencyAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	w := submitCRDT(t, h, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if _, created := postJSONResult(t, w); !created {
		t.Fatal("first submit must be created=true")
	}

	// Identical replay: idempotent.
	w = submitCRDT(t, h, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if _, created := postJSONResult(t, w); created {
		t.Fatal("identical replay must be created=false")
	}
	// Same id, different value: 409.
	if w := submitCRDT(t, h, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 6))); w.Code != http.StatusConflict {
		t.Fatalf("different value = %d, want 409", w.Code)
	}
	// Same id, same value, different device: 409.
	if w := submitCRDT(t, h, "doc", crdtCounterBody("dev-2", crdtCounterOp("a1", 5))); w.Code != http.StatusConflict {
		t.Fatalf("different device = %d, want 409", w.Code)
	}
	// The rejections moved nothing.
	if got := crdtStateBody(t, h, "doc"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after conflicts = %q", got)
	}
}

// Equal register versions across devices are decided by the lexicographically
// smaller op id, regardless of arrival order: two documents that receive the
// same two operations in opposite orders converge to the same winner.
func TestCRDTRegisterTieBreakBySmallerOpID(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	reg := func(device, id string, version int, value string) map[string]any {
		return crdtRegisterBody(device, map[string]any{"id": id, "version": version, "value": value})
	}
	// doc1: the lexicographically larger id arrives first.
	if w := submitCRDT(t, h, "doc1", reg("dev-1", "z-op", 3, "from-z")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateBody(t, h, "doc1"); got != `{"type":"register","value":"from-z"}`+"\n" {
		t.Fatalf("doc1 before tie = %q", got)
	}
	if w := submitCRDT(t, h, "doc1", reg("dev-2", "a-op", 3, "from-a")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// doc2: the same two operations in the opposite order.
	if w := submitCRDT(t, h, "doc2", reg("dev-2", "a-op", 3, "from-a")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "doc2", reg("dev-1", "z-op", 3, "from-z")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	want := `{"type":"register","value":"from-a"}` + "\n"
	if got := crdtStateBody(t, h, "doc1"); got != want {
		t.Fatalf("doc1 = %q, want %q", got, want)
	}
	if got := crdtStateBody(t, h, "doc2"); got != want {
		t.Fatalf("doc2 = %q, want %q (order-independent merge)", got, want)
	}
}

// The first accepted batch fixes the document type: explicit counter-first
// and gset-first documents reject every other type with a 409 and keep their
// merged state. (Register-first and orset-first have their own tests.)
func TestCRDTTypeFixationCounterAndGSetFirst(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	if w := submitCRDT(t, h, "doc-counter", crdtCounterBody("dev-1", crdtCounterOp("c1", 2))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "doc-gset", crdtGSetBody("dev-1", crdtGSetOp("g1", "x"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	otherTypes := []struct {
		name string
		body map[string]any
	}{
		{"counter", crdtCounterBody("dev-1", crdtCounterOp("n1", 1))},
		{"gset", crdtGSetBody("dev-1", crdtGSetOp("n1", "y"))},
		{"register", crdtRegisterBody("dev-1", map[string]any{"id": "n1", "version": 1, "value": 1})},
		{"orset", crdtORSetBody("dev-1", crdtORSetOp("n1", "add", "z"))},
	}
	for _, tc := range otherTypes {
		t.Run("counter-first rejects "+tc.name, func(t *testing.T) {
			if tc.name == "counter" {
				t.Skip("same type")
			}
			if w := submitCRDT(t, h, "doc-counter", tc.body); w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", w.Code)
			}
		})
		t.Run("gset-first rejects "+tc.name, func(t *testing.T) {
			if tc.name == "gset" {
				t.Skip("same type")
			}
			if w := submitCRDT(t, h, "doc-gset", tc.body); w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409", w.Code)
			}
		})
	}
	if got := crdtStateBody(t, h, "doc-counter"); got != `{"type":"counter","value":2}`+"\n" {
		t.Fatalf("counter state = %q", got)
	}
	if got := crdtStateBody(t, h, "doc-gset"); got != `{"type":"gset","value":["x"]}`+"\n" {
		t.Fatalf("gset state = %q", got)
	}
}

// Device and permission gating is decided before any CRDT content is read:
// an unregistered device gets 404 and a revoked device 403 even when the
// batch would otherwise lose to type fixation, and a malformed request is a
// 400 even from an unregistered device. None of the answers reveal the type
// or the state.
func TestCRDTOpsGatingPrecedesTypeAndStateChecks(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	if w := submitCRDT(t, h, "doc", crdtCounterBody("dev-1", crdtCounterOp("c1", 7))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Shape validation beats the device lookup: malformed JSON and a missing
	// field from an unregistered device are 400, not 404.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", `{"deviceId":"ghost","type":`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("malformed from ghost = %d, want 400", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", `{"deviceId":"ghost","ops":[{"id":"a","value":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing type from ghost = %d, want 400", w.Code)
	}

	// An unregistered device is 404 even though the declared type would lose
	// to the fixed counter type.
	w = submitCRDT(t, h, "doc", crdtGSetBody("ghost", crdtGSetOp("g1", "x")))
	if w.Code != http.StatusNotFound {
		t.Fatalf("ghost with wrong type = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// A revoked device is 403 on any batch, type-conforming or not.
	w = submitCRDT(t, h, "doc", crdtGSetBody("dev-2", crdtGSetOp("g1", "x")))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked with wrong type = %d, want 403", w.Code)
	}
	assertJSONError(t, w)
	w = submitCRDT(t, h, "doc", crdtCounterBody("dev-2", crdtCounterOp("c2", 1)))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked with right type = %d, want 403", w.Code)
	}

	// Nothing moved.
	if got := crdtStateBody(t, h, "doc"); got != `{"type":"counter","value":7}`+"\n" {
		t.Fatalf("state after gated rejections = %q", got)
	}
}

// A rejected batch writes nothing: neither a 400 (one invalid op) nor a 409
// (one conflicting or regressing op) records the batch's valid operations.
func TestCRDTOpsBatchAtomicityOn400And409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	seed := func(doc string) {
		t.Helper()
		if w := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("seed", 5))); w.Code != http.StatusOK {
			t.Fatalf("seed %s: %s", doc, w.Body.String())
		}
	}

	// 409: the batch pairs a valid new op with a conflicting existing id.
	seed("doc-conflict")
	w := submitCRDT(t, h, "doc-conflict", crdtCounterBody("dev-1",
		crdtCounterOp("new-1", 6), crdtCounterOp("seed", 9)))
	if w.Code != http.StatusConflict {
		t.Fatalf("conflicting batch = %d, want 409", w.Code)
	}
	if got := crdtStateBody(t, h, "doc-conflict"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after conflicting batch = %q", got)
	}
	// The valid op of the rejected batch was not recorded.
	w = submitCRDT(t, h, "doc-conflict", crdtCounterBody("dev-1", crdtCounterOp("new-1", 6)))
	if _, created := postJSONResult(t, w); !created {
		t.Fatal("new-1 must still be a fresh id after the rejected batch")
	}

	// 409: the batch pairs a valid advance with a regressing contribution.
	seed("doc-regress")
	w = submitCRDT(t, h, "doc-regress", crdtCounterBody("dev-1",
		crdtCounterOp("up", 7), crdtCounterOp("down", 3)))
	if w.Code != http.StatusConflict {
		t.Fatalf("regressing batch = %d, want 409", w.Code)
	}
	if got := crdtStateBody(t, h, "doc-regress"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after regressing batch = %q", got)
	}
	w = submitCRDT(t, h, "doc-regress", crdtCounterBody("dev-1", crdtCounterOp("up", 7)))
	if _, created := postJSONResult(t, w); !created {
		t.Fatal("up must still be a fresh id after the rejected batch")
	}

	// 400: the batch pairs a valid op with an invalid one.
	seed("doc-invalid")
	w = submitCRDT(t, h, "doc-invalid", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"ok","value":6},{"id":"bad","value":-1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid batch = %d, want 400", w.Code)
	}
	if got := crdtStateBody(t, h, "doc-invalid"); got != `{"type":"counter","value":5}`+"\n" {
		t.Fatalf("state after invalid batch = %q", got)
	}
	w = submitCRDT(t, h, "doc-invalid", crdtCounterBody("dev-1", crdtCounterOp("ok", 6)))
	if _, created := postJSONResult(t, w); !created {
		t.Fatal("ok must still be a fresh id after the 400 batch")
	}
}

// At every moment the session-scoped read and the document-level read are
// byte-for-byte identical, for all four CRDT types.
func TestCRDTSessionAndDocumentStateIdenticalForAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	assertSame := func(t *testing.T, doc string) {
		t.Helper()
		docRec := httptest.NewRecorder()
		h.ServeHTTP(docRec, httptest.NewRequest(http.MethodGet, "/v1/documents/"+doc+"/crdt/state", nil))
		sessRec := httptest.NewRecorder()
		h.ServeHTTP(sessRec, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", doc), nil))
		if docRec.Code != sessRec.Code || docRec.Body.String() != sessRec.Body.String() {
			t.Fatalf("doc %s: document = %d %q, session = %d %q",
				doc, docRec.Code, docRec.Body.String(), sessRec.Code, sessRec.Body.String())
		}
	}

	steps := []struct {
		doc  string
		body map[string]any
	}{
		{"doc-counter", crdtCounterBody("dev-1", crdtCounterOp("c1", 4))},
		{"doc-counter", crdtCounterBody("dev-2", crdtCounterOp("c2", 3))},
		{"doc-gset", crdtGSetBody("dev-1", crdtGSetOp("g1", "b", "a"))},
		{"doc-gset", crdtGSetBody("dev-2", crdtGSetOp("g2", "c"))},
		{"doc-register", crdtRegisterBody("dev-1", map[string]any{"id": "r1", "version": 1, "value": map[string]any{"k": true}})},
		{"doc-register", crdtRegisterBody("dev-2", map[string]any{"id": "r2", "version": 2, "value": nil})},
		{"doc-orset", crdtORSetBody("dev-1", crdtORSetOp("a1", "add", "apple"))},
		{"doc-orset", crdtORSetBody("dev-2", crdtORSetOp("r1", "remove", "apple"))},
	}
	for _, step := range steps {
		if w := submitCRDT(t, h, step.doc, step.body); w.Code != http.StatusOK {
			t.Fatalf("submit %s: %s", step.doc, w.Body.String())
		}
		assertSame(t, step.doc)
	}

	// The final bodies are the contract's exact shapes, register included.
	if got := crdtStateBody(t, h, "doc-register"); got != `{"type":"register","value":null}`+"\n" {
		t.Fatalf("register state = %q", got)
	}
	if got := crdtStateBody(t, h, "doc-orset"); got != `{"type":"orset","value":[]}`+"\n" {
		t.Fatalf("orset state = %q", got)
	}
}

// Type fixation, idempotency and conflict judgments survive a restart for
// counter and register documents alike; the merged state is byte-identical.
func TestCRDTJudgmentsPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt-contract.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	if w := submitCRDT(t, h, "cdoc", crdtCounterBody("dev-1", crdtCounterOp("c1", 5))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := submitCRDT(t, h, "cdoc", crdtCounterBody("dev-2", crdtCounterOp("d1", 3))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	reg := func(id string, version int, value string) map[string]any {
		return crdtRegisterBody("dev-1", map[string]any{"id": id, "version": version, "value": value})
	}
	if w := submitCRDT(t, h, "rdoc", reg("r1", 5, "five")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	counterBefore := crdtStateBody(t, h, "cdoc")
	registerBefore := crdtStateBody(t, h, "rdoc")
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	// The merged states are byte-identical after the restart.
	if got := crdtStateBody(t, h, "cdoc"); got != counterBefore {
		t.Fatalf("counter state after restart = %q, want %q", got, counterBefore)
	}
	if got := crdtStateBody(t, h, "rdoc"); got != registerBefore {
		t.Fatalf("register state after restart = %q, want %q", got, registerBefore)
	}

	// Idempotent replays are still idempotent.
	w := submitCRDT(t, h, "cdoc", crdtCounterBody("dev-1", crdtCounterOp("c1", 5)))
	if _, created := postJSONResult(t, w); created {
		t.Fatal("counter replay after restart must be created=false")
	}
	w = submitCRDT(t, h, "rdoc", reg("r1", 5, "five"))
	if _, created := postJSONResult(t, w); created {
		t.Fatal("register replay after restart must be created=false")
	}

	// Regressions and stalls are still 409.
	if w := submitCRDT(t, h, "cdoc", crdtCounterBody("dev-1", crdtCounterOp("c2", 4))); w.Code != http.StatusConflict {
		t.Fatalf("counter regression after restart = %d, want 409", w.Code)
	}
	if w := submitCRDT(t, h, "rdoc", reg("r2", 4, "four")); w.Code != http.StatusConflict {
		t.Fatalf("register regression after restart = %d, want 409", w.Code)
	}
	if w := submitCRDT(t, h, "rdoc", reg("r3", 5, "stall")); w.Code != http.StatusConflict {
		t.Fatalf("register stall after restart = %d, want 409", w.Code)
	}

	// Type fixation still holds on both documents.
	if w := submitCRDT(t, h, "cdoc", crdtGSetBody("dev-1", crdtGSetOp("g1", "x"))); w.Code != http.StatusConflict {
		t.Fatalf("gset on counter doc after restart = %d, want 409", w.Code)
	}
	if w := submitCRDT(t, h, "rdoc", crdtCounterBody("dev-1", crdtCounterOp("c9", 1))); w.Code != http.StatusConflict {
		t.Fatalf("counter on register doc after restart = %d, want 409", w.Code)
	}

	// And every rejection left the merged states untouched.
	if got := crdtStateBody(t, h, "cdoc"); got != counterBefore {
		t.Fatalf("counter state after rejections = %q, want %q", got, counterBefore)
	}
	if got := crdtStateBody(t, h, "rdoc"); got != registerBefore {
		t.Fatalf("register state after rejections = %q, want %q", got, registerBefore)
	}
}
