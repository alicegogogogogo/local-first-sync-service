package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func crdtCounterBody(device string, ops ...map[string]any) map[string]any {
	return map[string]any{"deviceId": device, "type": "counter", "ops": ops}
}

func crdtGSetBody(device string, ops ...map[string]any) map[string]any {
	return map[string]any{"deviceId": device, "type": "gset", "ops": ops}
}

func TestCRDTOpsCounterEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	submit := func(device string, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device,
			map[string]any{"id": id, "value": value}))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %s=%d: %d %s", device, value, w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 5)
	submit("dev-2", "b1", 3)
	submit("dev-1", "a2", 8) // advance dev-1 max 5 -> 8

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
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

func TestCRDTState404BeforeAnyOp(t *testing.T) {
	h, _ := newTestHandler(t)
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/never/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if body["error"] == "" {
		t.Fatal("expected a JSON error")
	}
}

func TestCRDTTypeFixationAndConcurrentDeclaration(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// Fire one counter and one gset first-batch concurrently; exactly one wins.
	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	start := make(chan struct{})
	submit := func(raw string) {
		defer wg.Done()
		<-start
		w, _ := postJSON(t, h, "/v1/documents/race/crdt/ops", raw)
		statuses <- w.Code
	}
	wg.Add(2)
	go submit(`{"deviceId":"dev-1","type":"counter","ops":[{"id":"c1","value":1}]}`)
	go submit(`{"deviceId":"dev-2","type":"gset","ops":[{"id":"g1","elements":["x"]}]}`)
	close(start)
	wg.Wait()
	close(statuses)
	var ok, conflict int
	for code := range statuses {
		switch code {
		case http.StatusOK:
			ok++
		case http.StatusConflict:
			conflict++
		default:
			t.Fatalf("unexpected status %d", code)
		}
	}
	if ok != 1 || conflict != 1 {
		t.Fatalf("ok=%d conflict=%d, want exactly one of each", ok, conflict)
	}

	// Exactly one type is now readable and fixed; the other keeps getting 409.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/race/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d", w.Code)
	}
	fixed := body["type"].(string)
	var losingBody map[string]any
	if fixed == "counter" {
		losingBody = crdtGSetBody("dev-1", map[string]any{"id": "later", "elements": []string{"y"}})
	} else {
		losingBody = crdtCounterBody("dev-1", map[string]any{"id": "later", "value": 2})
	}
	w, _ = postJSON(t, h, "/v1/documents/race/crdt/ops", losingBody)
	if w.Code != http.StatusConflict {
		t.Fatalf("losing type later = %d, want 409", w.Code)
	}
}

func TestCRDTCounterRegressionIs409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a2", "value": 4}))
	if w.Code != http.StatusConflict {
		t.Fatalf("regression status = %d, want 409", w.Code)
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if body["value"].(float64) != 5 {
		t.Fatalf("value after regression = %v, want 5", body["value"])
	}
}

func TestCRDTGSetUnionSortedAndIdempotent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	post := func(device string, id string, elements []string) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody(device,
			map[string]any{"id": id, "elements": elements}))
		return w
	}
	if w := post("dev-1", "g1", []string{"banana", "apple"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post("dev-2", "g2", []string{"cherry", "apple"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Same id, identical content: idempotent created=false.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"apple", "banana"}}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	created := body["results"].([]any)[0].(map[string]any)["created"].(bool)
	if created {
		t.Fatal("identical repeat must be created=false")
	}
	// Same id, different elements: 409.
	if w := post("dev-1", "g1", []string{"apple", "different"}); w.Code != http.StatusConflict {
		t.Fatalf("conflicting repeat = %d, want 409", w.Code)
	}
	// Same id from a different device: 409.
	if w := post("dev-2", "g1", []string{"banana", "apple"}); w.Code != http.StatusConflict {
		t.Fatalf("cross-device repeat = %d, want 409", w.Code)
	}

	w, state := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || state["type"] != "gset" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	got := state["value"].([]any)
	want := []string{"apple", "banana", "cherry"}
	if len(got) != len(want) {
		t.Fatalf("elements = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].(string) != want[i] {
			t.Fatalf("elements = %v, want sorted %v", got, want)
		}
	}
}

func TestCRDTOpsValidation400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	url := "/v1/documents/doc/crdt/ops"

	cases := []struct {
		name string
		raw  string
	}{
		{"not json", `{`},
		{"trailing content", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":1}]} junk`},
		{"missing deviceId", `{"type":"counter","ops":[{"id":"a","value":1}]}`},
		{"missing type", `{"deviceId":"dev-1","ops":[{"id":"a","value":1}]}`},
		{"bad type", `{"deviceId":"dev-1","type":"list","ops":[{"id":"a","value":1}]}`},
		{"empty ops", `{"deviceId":"dev-1","type":"counter","ops":[]}`},
		{"missing ops", `{"deviceId":"dev-1","type":"counter"}`},
		{"empty op id", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"","value":1}]}`},
		{"dup op ids", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":1},{"id":"a","value":2}]}`},
		{"counter missing value", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a"}]}`},
		{"counter fractional value", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":1.5}]}`},
		{"counter negative value", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":-1}]}`},
		{"counter string value", `{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":"1"}]}`},
		{"gset missing elements", `{"deviceId":"dev-1","type":"gset","ops":[{"id":"a"}]}`},
		{"gset empty elements", `{"deviceId":"dev-1","type":"gset","ops":[{"id":"a","elements":[]}]}`},
		{"gset empty element string", `{"deviceId":"dev-1","type":"gset","ops":[{"id":"a","elements":["x",""]}]}`},
		{"register missing value", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","version":1}]}`},
		{"register missing version", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","value":1}]}`},
		{"register null version", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","value":1,"version":null}]}`},
		{"register fractional version", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","value":1,"version":1.5}]}`},
		{"register negative version", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","value":1,"version":-1}]}`},
		{"register string version", `{"deviceId":"dev-1","type":"register","ops":[{"id":"a","value":1,"version":"1"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := postJSON(t, h, url, tc.raw)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", w.Code, w.Body.String())
			}
		})
	}

	// A rejected batch writes nothing: the document still has no state.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusNotFound {
		t.Fatalf("state after only-bad batches = %d, want 404", w.Code)
	}

	// A wrong Content-Type is a 400 regardless of body.
	r := httptest.NewRequest(http.MethodPost, url,
		strings.NewReader(`{"deviceId":"dev-1","type":"counter","ops":[{"id":"a","value":1}]}`))
	r.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("content-type status = %d, want 400", rec.Code)
	}
}

func TestCRDTOpsDeviceAndPermissionGating(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// Unregistered device: 404 with no state revealed.
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("ghost",
		map[string]any{"id": "a", "value": 1}))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered = %d, want 404", w.Code)
	}
	// Establish state as dev-1, then revoke dev-2.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 1}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	registerDevice(t, h, "dev-2")
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-2",
		map[string]any{"id": "b1", "value": 1}))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d, want 403", w.Code)
	}
}

func TestCRDTPathGuards(t *testing.T) {
	h, _ := newTestHandler(t)

	bad := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/documents//crdt/ops"},
		{http.MethodGet, "/v1/documents//crdt/state"},
		{http.MethodPost, "/v1/documents/doc/crdt/ops/"},
		{http.MethodGet, "/v1/documents/doc/crdt/state/"},
		{http.MethodPost, "/v1/documents/doc/crdt/ops/extra"},
		{http.MethodGet, "/v1/documents/doc/crdt/state/extra"},
		{http.MethodPost, "/v1/documents/doc/crdt"},
		{http.MethodGet, "/v1/documents/doc/crdt/"},
		{http.MethodPost, "/v1/documents/doc/crdt/other"},
		{http.MethodGet, "/v1/documents/doc/crdt/other"},
		// Wrong verbs on the exact endpoints are also 400 JSON.
		{http.MethodGet, "/v1/documents/doc/crdt/ops"},
		{http.MethodPost, "/v1/documents/doc/crdt/state"},
		{http.MethodDelete, "/v1/documents/doc/crdt/ops"},
	}
	for _, tc := range bad {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w, _ := doRequest(t, h, tc.method, tc.path)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", w.Code)
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("content type = %q, want JSON (no HTML/redirect)", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}
}

func TestCRDTDocumentNamedCrdtKeepsOrdinaryRoutes(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// A document literally called "crdt" still uses the change log normally.
	w, _ := postJSON(t, h, "/v1/documents/crdt/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("changes on doc named crdt: %d %s", w.Code, w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/crdt/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("read changes on doc named crdt: %d", w.Code)
	}
	if len(body["changes"].([]any)) != 1 {
		t.Fatalf("changes = %v", body["changes"])
	}
}

func TestCRDTStatePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt.db")
	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"b", "a"}}))
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
	w, body := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state after restart = %d", w.Code)
	}
	if body["type"] != "gset" {
		t.Fatalf("type after restart = %v", body["type"])
	}
	elements := body["value"].([]any)
	if len(elements) != 2 || elements[0] != "a" || elements[1] != "b" {
		t.Fatalf("elements after restart = %v", elements)
	}
}

// --- subscribe guard fix: ids literally named "subscribe" -------------------

// httpDo issues a JSON request against a real test server and returns the
// response (so WebSocket tests and HTTP setup can share one server).
func httpDo(t *testing.T, srv *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func TestSubscribeIdentifierNamedSubscribe(t *testing.T) {
	srv, _ := newWSTestServer(t)

	resp := httpDo(t, srv, http.MethodPost, "/v1/devices", `{"deviceId":"dev-1"}`)
	resp.Body.Close()
	// A session named "subscribe" is an ordinary session id.
	resp = httpDo(t, srv, http.MethodPost, "/v1/devices/dev-1/sessions", `{"sessionId":"subscribe"}`)
	resp.Body.Close()
	// A document named "subscribe" accepts ordinary changes.
	resp = httpDo(t, srv, http.MethodPost, "/v1/documents/subscribe/changes",
		`{"deviceId":"dev-1","changes":[{"id":"c1","payload":{"n":1}}]}`)
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("changes status = %d %s", resp.StatusCode, data)
	}
	resp.Body.Close()

	// Session-scoped read with both ids named subscribe works like any id.
	resp = httpDo(t, srv, http.MethodGet, "/v1/sessions/subscribe/documents/subscribe/changes?after=0", "")
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("session changes = %d %s", resp.StatusCode, data)
	}
	var page struct {
		Changes []json.RawMessage `json:"changes"`
	}
	if err := json.Unmarshal(data, &page); err != nil || len(page.Changes) != 1 {
		t.Fatalf("page = %s (err %v)", data, err)
	}

	// The WebSocket subscription upgrades normally with these ids and replays
	// the already-committed change.
	conn, wsResp := dialWS(t, subscribeURL(srv, "subscribe", "subscribe", "0"))
	if wsResp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %d, want 101", wsResp.StatusCode)
	}
	defer conn.close()
	change := conn.readChange()
	if change.ID != "c1" {
		t.Fatalf("pushed change id = %q, want c1", change.ID)
	}
}

func TestSubscribeDocumentNamedSubscribeGetsLivePush(t *testing.T) {
	srv, _ := newWSTestServer(t)

	resp := httpDo(t, srv, http.MethodPost, "/v1/devices", `{"deviceId":"dev-1"}`)
	resp.Body.Close()
	resp = httpDo(t, srv, http.MethodPost, "/v1/devices/dev-1/sessions", `{"sessionId":"sess"}`)
	resp.Body.Close()

	conn, wsResp := dialWS(t, subscribeURL(srv, "sess", "subscribe", "0"))
	if wsResp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade = %d, want 101", wsResp.StatusCode)
	}
	defer conn.close()

	resp = httpDo(t, srv, http.MethodPost, "/v1/documents/subscribe/changes",
		`{"deviceId":"dev-1","changes":[{"id":"live-1","payload":{"n":2}}]}`)
	resp.Body.Close()
	conn.setReadDeadline(2 * time.Second)
	change := conn.readChange()
	conn.clearReadDeadline()
	if change.ID != "live-1" {
		t.Fatalf("live push id = %q, want live-1", change.ID)
	}
}

func TestSubscribeMalformedPathsStill400WithSubscribeId(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "subscribe")
	// An extra segment past a valid shape whose identifiers are both
	// "subscribe" is still a malformed path even though both ids equal the
	// keyword.
	w, _ := doRequest(t, h, http.MethodGet,
		"/v1/sessions/subscribe/documents/subscribe/changes/subscribe/extra")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("extra segment = %d, want 400", w.Code)
	}
}

func crdtRegisterBody(device string, ops ...map[string]any) map[string]any {
	return map[string]any{"deviceId": device, "type": "register", "ops": ops}
}

func TestCRDTRegisterEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	post := func(device, id string, version int, value any) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtRegisterBody(device,
			map[string]any{"id": id, "version": version, "value": value}))
		return w
	}

	if w := post("dev-1", "a1", 1, "first"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := post("dev-2", "b1", 4, map[string]any{"winner": true}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A lower version from dev-1 loses the merge but is still accepted.
	if w := post("dev-1", "a2", 2, "loser"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
	if body["type"] != "register" {
		t.Fatalf("type = %v", body["type"])
	}
	value, ok := body["value"].(map[string]any)
	if !ok || value["winner"] != true {
		t.Fatalf("value = %v, want the v4 object", body["value"])
	}
	// The body is compact single-line JSON with one trailing newline.
	if got := w.Body.String(); got != `{"type":"register","value":{"winner":true}}`+"\n" {
		t.Fatalf("raw body = %q", got)
	}
}

func TestCRDTRegisterNullAndScalarValues(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// An explicit null value is a legal register value.
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops",
		`{"deviceId":"dev-1","type":"register","ops":[{"id":"a1","version":1,"value":null}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("null value = %d %s", w.Code, w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if got := w.Body.String(); got != `{"type":"register","value":null}`+"\n" {
		t.Fatalf("null state body = %q", got)
	}

	// An array value overtakes it with a greater version.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops",
		`{"deviceId":"dev-1","type":"register","ops":[{"id":"a2","version":2,"value":[1,"two",null]}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if got := w.Body.String(); got != `{"type":"register","value":[1,"two",null]}`+"\n" {
		t.Fatalf("array state body = %q", got)
	}
}

func TestCRDTRegisterVersionConflictIs409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	post := func(id string, version int, value string) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtRegisterBody("dev-1",
			map[string]any{"id": id, "version": version, "value": value}))
		return w
	}
	if w := post("a1", 5, "five"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A regressing version is a 409.
	if w := post("a2", 4, "four"); w.Code != http.StatusConflict {
		t.Fatalf("regression = %d, want 409", w.Code)
	}
	// An equal version with a fresh id is a 409 too.
	if w := post("a3", 5, "five-again"); w.Code != http.StatusConflict {
		t.Fatalf("stall = %d, want 409", w.Code)
	}
	// The identical repeat is idempotent, not a conflict.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtRegisterBody("dev-1",
		map[string]any{"id": "a1", "version": 5, "value": "five"}))
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent repeat = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["created"].(bool) {
		t.Fatal("identical repeat must be created=false")
	}
	// Same id with a different value or version is a 409.
	if w := post("a1", 5, "different"); w.Code != http.StatusConflict {
		t.Fatalf("different value = %d, want 409", w.Code)
	}
	if w := post("a1", 6, "five"); w.Code != http.StatusConflict {
		t.Fatalf("different version = %d, want 409", w.Code)
	}

	// The rejections moved nothing.
	w, state := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || state["value"] != "five" {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
}

func TestCRDTRegisterTypeFixation409(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtRegisterBody("dev-1",
		map[string]any{"id": "r1", "version": 1, "value": 1}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Counter and gset batches are now 409; the register state is unchanged.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "c1", "value": 1}))
	if w.Code != http.StatusConflict {
		t.Fatalf("counter after register = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtGSetBody("dev-1",
		map[string]any{"id": "g1", "elements": []string{"x"}}))
	if w.Code != http.StatusConflict {
		t.Fatalf("gset after register = %d, want 409", w.Code)
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if w.Code != http.StatusOK || body["type"] != "register" {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
}
