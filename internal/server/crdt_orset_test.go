package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func crdtORSetBody(device string, ops ...map[string]any) map[string]any {
	return map[string]any{"deviceId": device, "type": "orset", "ops": ops}
}

func crdtORSetOp(id, action, element string) map[string]any {
	return map[string]any{"id": id, "action": action, "element": element}
}

func TestCRDTORSetEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	post := func(device string, ops ...map[string]any) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody(device, ops...))
		return w
	}

	if w := post("dev-1", crdtORSetOp("a1", "add", "banana"), crdtORSetOp("a2", "add", "apple")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post("dev-2", crdtORSetOp("b1", "add", "cherry")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post("dev-2", crdtORSetOp("r1", "remove", "banana")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
	if body["type"] != "orset" {
		t.Fatalf("type = %v", body["type"])
	}
	// Elements are presented in ascending order; the body is compact
	// single-line JSON with one trailing newline.
	if got := w.Body.String(); got != `{"type":"orset","value":["apple","cherry"]}`+"\n" {
		t.Fatalf("raw body = %q", got)
	}

	// Removing the rest leaves an empty set, rendered as [].
	if w := post("dev-1", crdtORSetOp("r2", "remove", "apple"), crdtORSetOp("r3", "remove", "cherry")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if got := w.Body.String(); got != `{"type":"orset","value":[]}`+"\n" {
		t.Fatalf("empty body = %q", got)
	}
}

func TestCRDTORSetRemoveSemantics(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	post := func(ops ...map[string]any) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1", ops...))
		return w
	}
	state := func() string {
		t.Helper()
		w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
		if w.Code != http.StatusOK {
			t.Fatalf("state = %d %s", w.Code, w.Body.String())
		}
		return w.Body.String()
	}

	// Removing a never-added element is a created no-op, not an error.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("r0", "remove", "ghost")))
	if w.Code != http.StatusOK {
		t.Fatalf("remove of unknown element = %d %s", w.Code, w.Body.String())
	}
	if !body["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatal("a remove of a never-added element is still created")
	}
	if got := state(); got != `{"type":"orset","value":[]}`+"\n" {
		t.Fatalf("state after no-op remove = %q", got)
	}

	// A remove deletes only the adds it observes; a later add survives.
	if w := post(crdtORSetOp("a1", "add", "x")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post(crdtORSetOp("r1", "remove", "x")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := state(); got != `{"type":"orset","value":[]}`+"\n" {
		t.Fatalf("state after remove = %q", got)
	}
	if w := post(crdtORSetOp("a2", "add", "x")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := state(); got != `{"type":"orset","value":["x"]}`+"\n" {
		t.Fatalf("state after re-add = %q", got)
	}
}

func TestCRDTORSetIdempotencyAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "x"))); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical repeat is idempotent: created=false.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "x")))
	if w.Code != http.StatusOK {
		t.Fatalf("identical repeat = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatal("identical repeat must be created=false")
	}

	// Same id with a different element, action or device is a 409.
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "y"))); w.Code != http.StatusConflict {
		t.Fatalf("different element = %d, want 409", w.Code)
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "remove", "x"))); w.Code != http.StatusConflict {
		t.Fatalf("different action = %d, want 409", w.Code)
	}
	if w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-2",
		crdtORSetOp("a1", "add", "x"))); w.Code != http.StatusConflict {
		t.Fatalf("different device = %d, want 409", w.Code)
	}

	// The conflicts moved nothing.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if got := w.Body.String(); got != `{"type":"orset","value":["x"]}`+"\n" {
		t.Fatalf("state after conflicts = %q", got)
	}
}

func TestCRDTORSetValidation400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	url := "/v1/documents/doc/crdt/ops"

	cases := []struct {
		name string
		raw  string
	}{
		{"orset missing action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","element":"x"}]}`},
		{"orset bad action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"delete","element":"x"}]}`},
		{"orset numeric action", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":1,"element":"x"}]}`},
		{"orset missing element", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add"}]}`},
		{"orset empty element", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","element":""}]}`},
		{"orset null element", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","element":null}]}`},
		{"orset numeric element", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","element":5}]}`},
		{"orset remove missing element", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"remove"}]}`},
		{"orset dup op ids", `{"deviceId":"dev-1","type":"orset","ops":[{"id":"a","action":"add","element":"x"},{"id":"a","action":"remove","element":"x"}]}`},
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

func TestCRDTORSetTypeFixation409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "x")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Counter, gset and register batches are now 409; the orset state is
	// unchanged.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "c1", "value": 1}))
	if w.Code != http.StatusConflict {
		t.Fatalf("counter after orset = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"x"}}))
	if w.Code != http.StatusConflict {
		t.Fatalf("gset after orset = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtRegisterBody("dev-1",
		map[string]any{"id": "r1", "version": 1, "value": 1}))
	if w.Code != http.StatusConflict {
		t.Fatalf("register after orset = %d, want 409", w.Code)
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || body["type"] != "orset" {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
}

func TestCRDTORSetSessionStateView(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// No CRDT state yet: the session view is the same 404 as the document
	// view.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("session state before ops = %d, want 404", w.Code)
	}

	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "apple")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state")
	if got := w.Body.String(); got != `{"type":"orset","value":["apple"]}`+"\n" {
		t.Fatalf("session state body = %q", got)
	}

	// A revoked permission is a 403 decided before the state lookup, with no
	// state content.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/crdt/state")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked session state = %d, want 403", w.Code)
	}
}

func TestCRDTORSetStatePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt.db")
	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "b"), crdtORSetOp("a2", "add", "a")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("r1", "remove", "b")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)
	w, _ = doRequest(t, h2, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state after restart = %d", w.Code)
	}
	if got := w.Body.String(); got != `{"type":"orset","value":["a"]}`+"\n" {
		t.Fatalf("state after restart = %q", got)
	}
	// The remove's tombstone survived: re-adding b and reading still works,
	// and the idempotent replay of a1 is created=false.
	w, body := postJSON(t, h2, "/v1/documents/doc/crdt/ops", crdtORSetBody("dev-1",
		crdtORSetOp("a1", "add", "b")))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatal("idempotent replay after restart must be created=false")
	}
}

// An orset commit that moves the live element set pushes the new set to every
// subscriber; no-op removes, duplicate adds and idempotent replays push
// nothing.
func TestCRDTSubscribeORSetPushesOnlyRealChanges(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	// First ever state: [apple].
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("a1", "add", "apple"))); code != http.StatusOK {
		t.Fatalf("submit = %d", code)
	}
	state := conn.readCRDTState()
	if state.Type != "orset" {
		t.Fatalf("type = %q, want orset", state.Type)
	}
	if got := crdtSetElements(t, state); len(got) != 1 || got[0] != "apple" {
		t.Fatalf("elements = %v, want [apple]", got)
	}

	// A duplicate add (new id, already-present element), a remove of a
	// never-added element and an idempotent replay all move nothing and push
	// nothing.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("a2", "add", "apple"))); code != http.StatusOK {
		t.Fatalf("duplicate add = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("r0", "remove", "ghost"))); code != http.StatusOK {
		t.Fatalf("no-op remove = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("a1", "add", "apple"))); code != http.StatusOK {
		t.Fatalf("idempotent replay = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a no-op commit pushed a frame")
	}
	conn.clearReadDeadline()

	// A real remove pushes the empty set.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("r1", "remove", "apple"))); code != http.StatusOK {
		t.Fatalf("remove = %d", code)
	}
	state = conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 0 {
		t.Fatalf("elements = %v, want []", got)
	}

	// A re-add after the remove pushes the element again.
	if code := postCRDTOps(t, srv, "doc", crdtORSetBody("dev-1", crdtORSetOp("a3", "add", "apple"))); code != http.StatusOK {
		t.Fatalf("re-add = %d", code)
	}
	state = conn.readCRDTState()
	if got := crdtSetElements(t, state); len(got) != 1 || got[0] != "apple" {
		t.Fatalf("elements = %v, want [apple]", got)
	}
}
