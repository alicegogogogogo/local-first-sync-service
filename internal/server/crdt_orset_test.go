package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func crdtORSetBody(device string, ops ...map[string]any) map[string]any {
	return map[string]any{"deviceId": device, "type": "orset", "ops": ops}
}

func crdtORSetAddOp(id string, elements ...string) map[string]any {
	return map[string]any{"id": id, "action": "add", "elements": elements}
}

func crdtORSetRemoveOp(id string, elements ...string) map[string]any {
	return map[string]any{"id": id, "action": "remove", "elements": elements}
}

// Adds union into a sorted present-set and the state body is the compact
// orset frame; a remove drops observed elements; removing a never-added
// element is a successful no-op.
func TestCRDTORSetEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	post := func(device string, op map[string]any) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody(device, op))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %v: %d %s", op, w.Code, w.Body.String())
		}
	}
	post("dev-1", crdtORSetAddOp("a1", "banana", "apple"))
	post("dev-2", crdtORSetAddOp("a2", "cherry", "apple"))
	// Remove an element that was never added: 200, set unchanged.
	post("dev-1", crdtORSetRemoveOp("r0", "ghost"))
	// Remove a present element.
	post("dev-1", crdtORSetRemoveOp("r1", "banana"))

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"type":"orset","value":["apple","cherry"]}`+"\n" {
		t.Fatalf("raw body = %q", got)
	}
}

// A remove observes only the adds already committed; a later add survives even
// when it uses the same element the remove named.
func TestCRDTORSetRemoveDoesNotAffectLaterAdd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	post := func(device string, op map[string]any) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody(device, op))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %v: %d %s", op, w.Code, w.Body.String())
		}
	}
	post("dev-1", crdtORSetAddOp("a1", "x"))
	post("dev-2", crdtORSetRemoveOp("r1", "x"))
	post("dev-1", crdtORSetAddOp("a2", "x")) // not observed by r1

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d", w.Code)
	}
	if body["type"] != "orset" {
		t.Fatalf("type = %v", body["type"])
	}
	got := body["value"].([]any)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("value = %v, want [x]", body["value"])
	}
}

// Re-posting the same id is idempotent only with the same device, action and
// element set; any mismatch is a 409 that leaves the membership untouched.
func TestCRDTORSetIdempotencyAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("g1", "banana", "apple"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Identical content, elements reordered: idempotent created=false.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("g1", "apple", "banana")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatal("identical repeat must be created=false")
	}
	// Different elements -> 409.
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("g1", "apple", "cherry"))); w.Code != http.StatusConflict {
		t.Fatalf("different elements = %d, want 409", w.Code)
	}
	// Add vs remove -> 409.
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetRemoveOp("g1", "banana", "apple"))); w.Code != http.StatusConflict {
		t.Fatalf("different action = %d, want 409", w.Code)
	}
	// Different device -> 409.
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-2",
		crdtORSetAddOp("g1", "banana", "apple"))); w.Code != http.StatusConflict {
		t.Fatalf("cross-device repeat = %d, want 409", w.Code)
	}

	w, state := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || state["type"] != "orset" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	got := state["value"].([]any)
	if len(got) != 2 || got[0] != "apple" || got[1] != "banana" {
		t.Fatalf("elements after conflicts = %v", got)
	}
}

// Once fixed, an orset rejects every other declared type with 409 and vice
// versa.
func TestCRDTORSetTypeFixation409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("o1", "x")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "c1", "value": 1}))
	if w.Code != http.StatusConflict {
		t.Fatalf("counter after orset = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"y"}}))
	if w.Code != http.StatusConflict {
		t.Fatalf("gset after orset = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/other/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "c1", "value": 1}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/other/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("o1", "x")))
	if w.Code != http.StatusConflict {
		t.Fatalf("orset after counter = %d, want 409", w.Code)
	}
}

// Malformed orset bodies are 400 JSON errors and write nothing.
func TestCRDTORSetValidation400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	url := "/v1/documents/doc/crdt/ops"

	cases := []struct {
		name string
		raw  string
	}{
		{"bad type", `{"deviceId":"dev-1","type":"set","ops":[{"id":"a","action":"add","elements":["x"]}]}`},
		{"missing action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","elements":["x"]}]}`},
		{"bad action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"delete","elements":["x"]}]}`},
		{"null action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":null,"elements":["x"]}]}`},
		{"missing elements", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add"}]}`},
		{"empty elements", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","elements":[]}]}`},
		{"empty element string", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"remove","elements":["x",""]}]}`},
		{"elements not array", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","elements":"x"}]}`},
		{"empty ops", `{"deviceId":"dev-1","type":"orset","ops":[]}`},
		{"dup op ids", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","elements":["x"]},{"id":"a","action":"add","elements":["y"]}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := postJSON(t, h, url, tc.raw)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", w.Code, w.Body.String())
			}
		})
	}

	// Every rejected batch wrote nothing: the document still has no state.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("state after only-bad batches = %d, want 404", w.Code)
	}
}

// The shared registration/permission gate applies to orset batches exactly as
// it does to the other types.
func TestCRDTORSetDeviceAndPermissionGating(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("ghost",
		crdtORSetAddOp("a", "x")))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered = %d, want 404", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "x")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	registerDevice(t, h, "dev-2")
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-2",
		crdtORSetAddOp("b1", "y")))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d, want 403", w.Code)
	}
}

// After a restart the orset membership and the idempotent/conflict decisions
// are unchanged.
func TestCRDTORSetPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt-orset.db")
	s1, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "b", "a"), crdtORSetRemoveOp("r1", "a"), crdtORSetAddOp("a2", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
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
	w, body := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || body["type"] != "orset" {
		t.Fatalf("state after restart = %d %s", w.Code, w.Body.String())
	}
	got := body["value"].([]any)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("elements after restart = %v", got)
	}
	// Idempotent replay stays non-creating after restart.
	w, rb := postJSON(t, h2, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetRemoveOp("r1", "a")))
	if w.Code != http.StatusOK || rb["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatalf("idempotent replay after restart = %d %s", w.Code, w.Body.String())
	}
	// A conflicting re-post is still a 409.
	w, _ = postJSON(t, h2, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "b", "a", "c")))
	if w.Code != http.StatusConflict {
		t.Fatalf("conflicting re-post after restart = %d, want 409", w.Code)
	}
}

// --- subscription -----------------------------------------------------------

// The subscription pushes the current orset state first, then one frame per
// genuine membership change; removes of unseen elements, re-adds of present
// elements and idempotent replays push nothing. Each frame is byte-identical
// to the state read body.
func TestCRDTSubscribeORSetPushesOnlyRealChanges(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	// First add pushes the sorted present-set.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "banana", "apple"))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	state := conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 2 || got[0] != "apple" || got[1] != "banana" {
		t.Fatalf("frame = %v, want [apple banana]", got)
	}

	expectSilence := func(msg string) {
		t.Helper()
		conn.setReadDeadline(300 * time.Millisecond)
		if _, _, _, ok := conn.readFrameMaybe(); ok {
			t.Fatal(msg)
		}
		conn.clearReadDeadline()
	}

	// A remove of an element never added creates the op but moves nothing.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetRemoveOp("r0", "ghost"))); code != http.StatusOK {
		t.Fatalf("remove unseen = %d", code)
	}
	// Re-adding an already-present element with a new id moves nothing.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetAddOp("a2", "apple"))); code != http.StatusOK {
		t.Fatalf("re-add present = %d", code)
	}
	// An idempotent replay moves nothing.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "apple", "banana"))); code != http.StatusOK {
		t.Fatalf("idempotent replay = %d", code)
	}
	expectSilence("an unseen-remove/re-add/idempotent commit pushed a frame")

	// Removing a present element pushes the shrunk set.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetRemoveOp("r1", "banana"))); code != http.StatusOK {
		t.Fatalf("remove = %d", code)
	}
	state = conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 1 || got[0] != "apple" {
		t.Fatalf("frame after remove = %v, want [apple]", got)
	}

	// A fresh add after the remove (observed-remove) brings the element back
	// and pushes the grown set.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetAddOp("a3", "banana"))); code != http.StatusOK {
		t.Fatalf("re-add after remove = %d", code)
	}
	state = conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 2 || got[0] != "apple" || got[1] != "banana" {
		t.Fatalf("frame after re-add = %v, want [apple banana]", got)
	}

	// Every pushed frame matches the state read body byte for byte.
	docBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")
	if docBody != `{"type":"orset","value":["apple","banana"]}`+"\n" {
		t.Fatalf("state read body = %q", docBody)
	}
}

// An orset state already present at subscribe time is pushed as the first
// frame, identical to the state read.
func TestCRDTSubscribeInitialORSetState(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "b", "a"), crdtORSetRemoveOp("r1", "b"))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	stateBody := mustReadBody(t, srv, "/v1/documents/doc/crdt/state")
	if stateBody != `{"type":"orset","value":["a"]}`+"\n" {
		t.Fatalf("state body = %q", stateBody)
	}

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d, want 101", hs.StatusCode)
	}
	defer conn.close()
	if raw := string(conn.readCRDTRaw()); raw != stateBody {
		t.Fatalf("frame %q != state read body %q", raw, stateBody)
	}
}

// The session-scoped read returns the same orset state, byte-identical to the
// document-level read.
func TestSessionCRDTStateORSet(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetAddOp("a1", "b", "a"), crdtORSetRemoveOp("r1", "b")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	session := httptest.NewRecorder()
	h.ServeHTTP(session, httptest.NewRequest(http.MethodGet, sessionCRDTStatePath("sess", "doc"), nil))
	document := httptest.NewRecorder()
	h.ServeHTTP(document, httptest.NewRequest(http.MethodGet, "/v1/documents/doc/crdt/state", nil))
	want := `{"type":"orset","value":["a"]}` + "\n"
	if session.Code != http.StatusOK || session.Body.String() != want {
		t.Fatalf("session read = %d %q", session.Code, session.Body.String())
	}
	if session.Body.String() != document.Body.String() {
		t.Fatalf("session body %q != document body %q", session.Body.String(), document.Body.String())
	}
}
