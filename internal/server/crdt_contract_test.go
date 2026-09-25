package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// This file fills the point-by-point public-contract coverage for the CRDT
// surface. Every assertion goes through the documented HTTP endpoints
// (POST .../crdt/ops and the document/session state reads); no internal package
// field or storage structure is inspected. It concentrates on the branches not
// already pinned elsewhere: counter idempotency/conflict/no-op, the remaining
// type-fixation matrix, whole-batch atomicity, gate ordering ahead of CRDT
// content, register tie-breaking, byte-identical document/session reads for all
// four types, change-log independence, and persistence of the idempotency and
// conflict judgments across a restart.

// submitCRDT posts a CRDT batch and returns the recorder plus decoded body.
func submitCRDT(t *testing.T, h http.Handler, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+doc+"/crdt/ops", body)
}

func crdtStateRaw(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

// Counter: repeated id semantics, the accepted equal-contribution no-op,
// regression rejection and multi-op result ordering — all over public HTTP.
func TestCRDTContractCounterIdempotencyConflictAndNoop(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	const doc = "doc"
	state := func() float64 {
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/crdt/state")
		if w.Code != http.StatusOK {
			t.Fatalf("state = %d %s", w.Code, w.Body.String())
		}
		return body["value"].(float64)
	}

	// First contribution: created=true, sum 5.
	w, body := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("first = %d %v", w.Code, body)
	}
	if state() != 5 {
		t.Fatal("value after first op")
	}

	// Identical replay: created=false, no movement.
	w, body = submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("idempotent replay = %d %v", w.Code, body)
	}
	if state() != 5 {
		t.Fatal("value moved on idempotent replay")
	}

	// Same id, different value: 409 and the state is untouched.
	if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 9))); w.Code != http.StatusConflict {
		t.Fatalf("same id different value = %d, want 409", w.Code)
	}
	// Same id from another device: 409 as well.
	if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-2", crdtCounterOp("a1", 5))); w.Code != http.StatusConflict {
		t.Fatalf("same id different device = %d, want 409", w.Code)
	}
	if state() != 5 {
		t.Fatal("value moved on a conflicting replay")
	}

	// An equal contribution under a fresh id is accepted (created=true) but is a
	// merge no-op: the per-device maximum stays 5.
	w, body = submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a2", 5)))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("equal contribution = %d %v", w.Code, body)
	}
	if state() != 5 {
		t.Fatal("equal contribution moved the sum")
	}

	// A regressing contribution is a 409 and moves nothing.
	if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a3", 4))); w.Code != http.StatusConflict {
		t.Fatalf("regression = %d, want 409", w.Code)
	}
	if state() != 5 {
		t.Fatal("value moved on regression")
	}

	// A multi-op batch reports results in request order; advancing to 8 then
	// adding dev-2's 3 yields 11.
	w, body = submitCRDT(t, h, doc, crdtCounterBody("dev-1",
		crdtCounterOp("a4", 8), crdtCounterOp("a5", 8)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	res := body["results"].([]any)
	if len(res) != 2 || res[0].(map[string]any)["id"] != "a4" || res[1].(map[string]any)["id"] != "a5" {
		t.Fatalf("result order = %v", res)
	}
	if res[0].(map[string]any)["created"] != true || res[1].(map[string]any)["created"] != true {
		t.Fatalf("created flags = %v", res)
	}
	w, body = submitCRDT(t, h, doc, crdtCounterBody("dev-2", crdtCounterOp("b1", 3)))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if state() != 11 {
		t.Fatalf("value = %v, want 11", state())
	}
}

// A counter-fixed document rejects every later non-counter batch with 409 and
// keeps the counter type and value; the same holds for a gset-fixed document.
func TestCRDTContractFixationMatrix(t *testing.T) {
	t.Run("counter rejects the other three", func(t *testing.T) {
		h, _ := newTestHandler(t)
		registerDevice(t, h, "dev-1")
		if w, _ := submitCRDT(t, h, "c", crdtCounterBody("dev-1", crdtCounterOp("a1", 2))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		others := []map[string]any{
			crdtGSetBody("dev-1", crdtGSetOp("g1", "x")),
			crdtRegisterBody("dev-1", crdtRegisterOp("r1", 1, 1)),
			crdtORSetBody("dev-1", crdtORSetOp("o1", "add", "x")),
		}
		for _, b := range others {
			if w, _ := submitCRDT(t, h, "c", b); w.Code != http.StatusConflict {
				t.Fatalf("counter doc got %d for %v, want 409", w.Code, b["type"])
			}
		}
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/c/crdt/state")
		if w.Code != http.StatusOK || body["type"] != "counter" || body["value"].(float64) != 2 {
			t.Fatalf("state after rejected types = %d %v", w.Code, body)
		}
	})

	t.Run("gset rejects the other three", func(t *testing.T) {
		h, _ := newTestHandler(t)
		registerDevice(t, h, "dev-1")
		if w, _ := submitCRDT(t, h, "g", crdtGSetBody("dev-1", crdtGSetOp("g1", "a"))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		others := []map[string]any{
			crdtCounterBody("dev-1", crdtCounterOp("c1", 1)),
			crdtRegisterBody("dev-1", crdtRegisterOp("r1", 1, 1)),
			crdtORSetBody("dev-1", crdtORSetOp("o1", "add", "x")),
		}
		for _, b := range others {
			if w, _ := submitCRDT(t, h, "g", b); w.Code != http.StatusConflict {
				t.Fatalf("gset doc got %d for %v, want 409", w.Code, b["type"])
			}
		}
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/g/crdt/state")
		if w.Code != http.StatusOK || body["type"] != "gset" {
			t.Fatalf("state after rejected types = %d %v", w.Code, body)
		}
	})
}

// A batch is atomic for every type: a valid leading op followed by a conflicting
// op is a 409 that writes nothing, and a 400 batch writes nothing. The rolled-
// back op is replayable as a genuinely new op afterwards.
func TestCRDTContractBatchAtomicZeroWrite(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	t.Run("counter 409 rolls the leading advance back", func(t *testing.T) {
		const doc = "c"
		if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		// z1 would advance 5 -> 10, then the known a1 with another value fails.
		bad := crdtCounterBody("dev-1", crdtCounterOp("z1", 10), crdtCounterOp("a1", 99))
		if w, _ := submitCRDT(t, h, doc, bad); w.Code != http.StatusConflict {
			t.Fatalf("mixed batch = %d, want 409", w.Code)
		}
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/crdt/state")
		if w.Code != http.StatusOK || body["value"].(float64) != 5 {
			t.Fatalf("value after rolled-back batch = %v, want 5", body["value"])
		}
		// z1 was fully rolled back: replaying it now creates and advances.
		if w, b := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("z1", 10))); w.Code != http.StatusOK ||
			b["results"].([]any)[0].(map[string]any)["created"] != true {
			t.Fatalf("replay of rolled-back z1 = %d %v", w.Code, b)
		}
	})

	t.Run("gset 409 rolls the leading element back", func(t *testing.T) {
		const doc = "gs"
		if w, _ := submitCRDT(t, h, doc, crdtGSetBody("dev-1", crdtGSetOp("g1", "a"))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		bad := crdtGSetBody("dev-1", crdtGSetOp("gnew", "z"), crdtGSetOp("g1", "b"))
		if w, _ := submitCRDT(t, h, doc, bad); w.Code != http.StatusConflict {
			t.Fatalf("mixed batch = %d, want 409", w.Code)
		}
		if got := crdtStateRaw(h, "/v1/documents/"+doc+"/crdt/state").Body.String(); got != `{"type":"gset","value":["a"]}`+"\n" {
			t.Fatalf("state after rollback = %q", got)
		}
		if w, b := submitCRDT(t, h, doc, crdtGSetBody("dev-1", crdtGSetOp("gnew", "z"))); w.Code != http.StatusOK ||
			b["results"].([]any)[0].(map[string]any)["created"] != true {
			t.Fatalf("replay of rolled-back gnew = %d %v", w.Code, b)
		}
	})

	t.Run("register 409 rolls the leading winner back", func(t *testing.T) {
		const doc = "r"
		if w, _ := submitCRDT(t, h, doc, crdtRegisterBody("dev-1", crdtRegisterOp("r1", 1, "one"))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		// rnew at v9 would win, then the known r1 with another value fails.
		bad := crdtRegisterBody("dev-1", crdtRegisterOp("rnew", 9, "nine"), crdtRegisterOp("r1", 1, "other"))
		if w, _ := submitCRDT(t, h, doc, bad); w.Code != http.StatusConflict {
			t.Fatalf("mixed batch = %d, want 409", w.Code)
		}
		if got := crdtStateRaw(h, "/v1/documents/"+doc+"/crdt/state").Body.String(); got != `{"type":"register","value":"one"}`+"\n" {
			t.Fatalf("state after rollback = %q", got)
		}
		if w, _ := submitCRDT(t, h, doc, crdtRegisterBody("dev-1", crdtRegisterOp("rnew", 9, "nine"))); w.Code != http.StatusOK {
			t.Fatalf("replay of rolled-back rnew = %d", w.Code)
		}
	})

	t.Run("orset 409 rolls the leading add back", func(t *testing.T) {
		const doc = "o"
		if w, _ := submitCRDT(t, h, doc, crdtORSetBody("dev-1", crdtORSetOp("a1", "add", "x"))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		bad := crdtORSetBody("dev-1", crdtORSetOp("anew", "add", "z"), crdtORSetOp("a1", "add", "y"))
		if w, _ := submitCRDT(t, h, doc, bad); w.Code != http.StatusConflict {
			t.Fatalf("mixed batch = %d, want 409", w.Code)
		}
		if got := crdtStateRaw(h, "/v1/documents/"+doc+"/crdt/state").Body.String(); got != `{"type":"orset","value":["x"]}`+"\n" {
			t.Fatalf("state after rollback = %q", got)
		}
		if w, _ := submitCRDT(t, h, doc, crdtORSetBody("dev-1", crdtORSetOp("anew", "add", "z"))); w.Code != http.StatusOK {
			t.Fatalf("replay of rolled-back anew = %d", w.Code)
		}
	})

	t.Run("400 in-batch duplicate writes nothing", func(t *testing.T) {
		const doc = "c400"
		if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		// A leading advancing op plus a duplicated id is a 400 with zero writes.
		bad := crdtCounterBody("dev-1", crdtCounterOp("lead", 9), crdtCounterOp("dup", 1), crdtCounterOp("dup", 2))
		if w, _ := submitCRDT(t, h, doc, bad); w.Code != http.StatusBadRequest {
			t.Fatalf("dup batch = %d, want 400", w.Code)
		}
		_, body := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/crdt/state")
		if body["value"].(float64) != 5 {
			t.Fatalf("value after 400 batch = %v, want 5", body["value"])
		}
	})
}

// Register version ties are broken by the lexicographically smaller operation
// id, independently of arrival order, read through the plain HTTP state.
func TestCRDTContractRegisterTieBreakByID(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// Submit the larger id first; the smaller id arriving later must win.
	if w, _ := submitCRDT(t, h, "doc", crdtRegisterBody("dev-2", crdtRegisterOp("z9", 5, "from-z"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := submitCRDT(t, h, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("a9", 5, "from-a"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := crdtStateRaw(h, "/v1/documents/doc/crdt/state").Body.String(); got != `{"type":"register","value":"from-a"}`+"\n" {
		t.Fatalf("tie winner = %q, want from-a", got)
	}
}

// For all four types the document-level and session-level state reads return
// byte-for-byte the same compact body at the same instant.
func TestCRDTContractDocumentAndSessionReadsByteIdentical(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	registerDevice(t, h, "dev-2")

	// counter: per-device maxima 8 + 3 = 11.
	submitCRDT(t, h, "d-counter", crdtCounterBody("dev-1", crdtCounterOp("a1", 8)))
	submitCRDT(t, h, "d-counter", crdtCounterBody("dev-2", crdtCounterOp("b1", 3)))
	// gset: unordered inserts come out ascending.
	submitCRDT(t, h, "d-gset", crdtGSetBody("dev-1", crdtGSetOp("g1", "b", "a")))
	submitCRDT(t, h, "d-gset", crdtGSetBody("dev-2", crdtGSetOp("g2", "c", "a")))
	// register: the higher version wins.
	submitCRDT(t, h, "d-register", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 1, map[string]any{"old": true})))
	submitCRDT(t, h, "d-register", crdtRegisterBody("dev-2", crdtRegisterOp("r2", 4, "win")))
	// orset: b is added then removed, a survives.
	submitCRDT(t, h, "d-orset", crdtORSetBody("dev-1", crdtORSetOp("a1", "add", "b"), crdtORSetOp("a2", "add", "a")))
	submitCRDT(t, h, "d-orset", crdtORSetBody("dev-1", crdtORSetOp("r1", "remove", "b")))

	cases := []struct {
		doc  string
		want string
	}{
		{"d-counter", `{"type":"counter","value":11}` + "\n"},
		{"d-gset", `{"type":"gset","value":["a","b","c"]}` + "\n"},
		{"d-register", `{"type":"register","value":"win"}` + "\n"},
		{"d-orset", `{"type":"orset","value":["a"]}` + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.doc, func(t *testing.T) {
			doc := crdtStateRaw(h, "/v1/documents/"+tc.doc+"/crdt/state")
			sess := crdtStateRaw(h, sessionCRDTStatePath("sess", tc.doc))
			if doc.Code != http.StatusOK || sess.Code != http.StatusOK {
				t.Fatalf("codes doc=%d sess=%d", doc.Code, sess.Code)
			}
			if doc.Body.String() != tc.want {
				t.Fatalf("document body = %q, want %q", doc.Body.String(), tc.want)
			}
			if sess.Body.String() != doc.Body.String() {
				t.Fatalf("session body %q != document body %q", sess.Body.String(), doc.Body.String())
			}
		})
	}
}

// CRDT state lives outside the change log for every type: CRDT commits create no
// change record and advance no document cursor.
func TestCRDTContractIndependentOfChangeLogAllTypes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// One ordinary change per document fixes the cursor at 1.
	for _, doc := range []string{"d-counter", "d-gset", "d-register", "d-orset"} {
		w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}

	submitCRDT(t, h, "d-counter", crdtCounterBody("dev-1", crdtCounterOp("a1", 7)))
	submitCRDT(t, h, "d-gset", crdtGSetBody("dev-1", crdtGSetOp("g1", "a", "b")))
	submitCRDT(t, h, "d-register", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 1, "v")))
	submitCRDT(t, h, "d-orset", crdtORSetBody("dev-1", crdtORSetOp("o1", "add", "a")))

	// The change page is untouched for every type: still one row, cursor 1.
	for _, doc := range []string{"d-counter", "d-gset", "d-register", "d-orset"} {
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/changes")
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		if len(body["changes"].([]any)) != 1 || body["nextCursor"].(float64) != 1 {
			t.Fatalf("%s change page mutated by CRDT: %v", doc, body)
		}
	}
}

// Gate ordering is fixed ahead of CRDT content: shape (400) beats device
// registration (404), which beats permission (403), which beats type fixation
// (409). None of the early failures leaks the fixed type or the value.
func TestCRDTContractGateOrderingBeforeContent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	const doc = "doc"
	// Fix the type as counter and give it state.
	if w, _ := submitCRDT(t, h, doc, crdtCounterBody("dev-1", crdtCounterOp("a1", 5))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Revoke dev-2.
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/permissions", map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A valid batch declaring a different fixed type still yields the gate's
	// verdict, not a 409: unregistered -> 404, revoked -> 403.
	if w, body := submitCRDT(t, h, doc, crdtGSetBody("ghost", crdtGSetOp("g1", "x"))); w.Code != http.StatusNotFound {
		t.Fatalf("unregistered wrong-type = %d %v, want 404", w.Code, body)
	} else {
		assertCRDTGateDoesNotLeak(t, w.Body.String())
	}
	if w, body := submitCRDT(t, h, doc, crdtGSetBody("dev-2", crdtGSetOp("g1", "x"))); w.Code != http.StatusForbidden {
		t.Fatalf("revoked wrong-type = %d %v, want 403", w.Code, body)
	} else {
		assertCRDTGateDoesNotLeak(t, w.Body.String())
	}

	// A malformed body (400) is decided before registration or permission.
	if w, _ := submitCRDT(t, h, doc, `{"deviceId":"ghost","type":`); w.Code != http.StatusBadRequest {
		t.Fatalf("malformed from unregistered = %d, want 400", w.Code)
	}
	if w, _ := submitCRDT(t, h, doc, `{"deviceId":"dev-2","type":`); w.Code != http.StatusBadRequest {
		t.Fatalf("malformed from revoked = %d, want 400", w.Code)
	}
}

func assertCRDTGateDoesNotLeak(t *testing.T, body string) {
	t.Helper()
	for _, secret := range []string{"counter", "gset", "register", "orset", "value"} {
		if strings.Contains(body, secret) {
			t.Fatalf("gate error leaks CRDT content %q: %q", secret, body)
		}
	}
}

// Counter judgments survive a restart: type fixation, the merged sum, the
// idempotent replay, value/device conflicts and the regression rejection.
func TestCRDTContractCounterPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt-counter-contract.db")
	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	registerDevice(t, h1, "dev-2")
	if w, _ := postJSON(t, h1, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"}); w.Code != http.StatusOK {
		t.Fatalf("create session = %d %s", w.Code, w.Body.String())
	}
	submitCRDT(t, h1, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	submitCRDT(t, h1, "doc", crdtCounterBody("dev-2", crdtCounterOp("b1", 3)))
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	want := `{"type":"counter","value":8}` + "\n"
	if got := crdtStateRaw(h2, "/v1/documents/doc/crdt/state").Body.String(); got != want {
		t.Fatalf("document state after restart = %q, want %q", got, want)
	}
	// The document and session reads still agree byte-for-byte.
	if got := crdtStateRaw(h2, sessionCRDTStatePath("sess", "doc")).Body.String(); got != want {
		t.Fatalf("session state after restart = %q, want %q", got, want)
	}

	// Identical replay stays idempotent after the restart.
	w, body := submitCRDT(t, h2, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 5)))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("idempotent replay after restart = %d %v", w.Code, body)
	}
	// Different value and different device on the same id stay 409.
	if w, _ := submitCRDT(t, h2, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 9))); w.Code != http.StatusConflict {
		t.Fatalf("value conflict after restart = %d, want 409", w.Code)
	}
	if w, _ := submitCRDT(t, h2, "doc", crdtCounterBody("dev-2", crdtCounterOp("a1", 5))); w.Code != http.StatusConflict {
		t.Fatalf("device conflict after restart = %d, want 409", w.Code)
	}
	// A regression stays rejected and the type remains fixed.
	if w, _ := submitCRDT(t, h2, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 4))); w.Code != http.StatusConflict {
		t.Fatalf("regression after restart = %d, want 409", w.Code)
	}
	if w, _ := submitCRDT(t, h2, "doc", crdtGSetBody("dev-1", crdtGSetOp("g1", "x"))); w.Code != http.StatusConflict {
		t.Fatalf("type change after restart = %d, want 409", w.Code)
	}
	if got := crdtStateRaw(h2, "/v1/documents/doc/crdt/state").Body.String(); got != want {
		t.Fatalf("state changed after rejected replays = %q", got)
	}
}

// Register judgments survive a restart: winner value, idempotent replay,
// value/version/device conflicts, the stall rejection and type fixation.
func TestCRDTContractRegisterPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt-register-contract.db")
	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	registerDevice(t, h1, "dev-2")
	submitCRDT(t, h1, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 5, "five")))
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	want := `{"type":"register","value":"five"}` + "\n"
	if got := crdtStateRaw(h2, "/v1/documents/doc/crdt/state").Body.String(); got != want {
		t.Fatalf("state after restart = %q, want %q", got, want)
	}

	// Identical replay stays idempotent.
	w, body := submitCRDT(t, h2, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 5, "five")))
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("idempotent replay after restart = %d %v", w.Code, body)
	}
	// Different value, different version, different device on the same id: 409.
	if w, _ := submitCRDT(t, h2, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 5, "other"))); w.Code != http.StatusConflict {
		t.Fatalf("value conflict after restart = %d, want 409", w.Code)
	}
	if w, _ := submitCRDT(t, h2, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("r1", 6, "five"))); w.Code != http.StatusConflict {
		t.Fatalf("version conflict after restart = %d, want 409", w.Code)
	}
	if w, _ := submitCRDT(t, h2, "doc", crdtRegisterBody("dev-2", crdtRegisterOp("r1", 5, "five"))); w.Code != http.StatusConflict {
		t.Fatalf("device conflict after restart = %d, want 409", w.Code)
	}
	// A fresh id stalling at the same per-device version is rejected.
	if w, _ := submitCRDT(t, h2, "doc", crdtRegisterBody("dev-1", crdtRegisterOp("r2", 5, "stall"))); w.Code != http.StatusConflict {
		t.Fatalf("stall after restart = %d, want 409", w.Code)
	}
	// The type stays fixed.
	if w, _ := submitCRDT(t, h2, "doc", crdtCounterBody("dev-1", crdtCounterOp("c1", 1))); w.Code != http.StatusConflict {
		t.Fatalf("type change after restart = %d, want 409", w.Code)
	}
	if got := crdtStateRaw(h2, "/v1/documents/doc/crdt/state").Body.String(); got != want {
		t.Fatalf("state changed after rejected replays = %q", got)
	}
}
