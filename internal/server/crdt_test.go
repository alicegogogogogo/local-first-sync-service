package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func crdtPost(t *testing.T, h http.Handler, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+doc+"/crdt/ops", body)
}

func crdtState(t *testing.T, h http.Handler, doc string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/crdt/state")
}

func TestCRDTCounterSubmitAndState(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	registerDevice(t, h, "dev-b")

	w, body := crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a",
		"type":     "counter",
		"ops":      []any{map[string]any{"id": "o1", "value": 3}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("submit = %d %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	r0 := results[0].(map[string]any)
	if r0["id"] != "o1" || r0["created"] != true {
		t.Fatalf("result = %v", r0)
	}

	crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o2", "value": 7}},
	})
	crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-b", "type": "counter",
		"ops": []any{map[string]any{"id": "p1", "value": 5}},
	})

	w, body = crdtState(t, h, "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d %s", w.Code, w.Body.String())
	}
	if body["type"] != "counter" || body["value"].(float64) != 12 {
		t.Fatalf("body = %s, want counter 12", w.Body.String())
	}

	// Re-posting the identical op is idempotent.
	w, body = crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 3}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false {
		t.Fatalf("idempotent repost = %d %s", w.Code, w.Body.String())
	}
}

func TestCRDTGSetSubmitAndState(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	registerDevice(t, h, "dev-b")

	cases := []any{
		map[string]any{"deviceId": "dev-a", "type": "gset", "ops": []any{
			map[string]any{"id": "a1", "value": "cherry"},
			map[string]any{"id": "a2", "value": "apple"},
		}},
		map[string]any{"deviceId": "dev-b", "type": "gset", "ops": []any{
			map[string]any{"id": "b1", "value": "banana"},
			map[string]any{"id": "b2", "value": "apple"}, // same element, new id
		}},
	}
	for _, c := range cases {
		if w, _ := crdtPost(t, h, "doc", c); w.Code != http.StatusOK {
			t.Fatalf("submit = %d %s", w.Code, w.Body)
		}
	}

	w, body := crdtState(t, h, "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("state = %d", w.Code)
	}
	members := body["value"].([]any)
	want := []string{"apple", "banana", "cherry"}
	if body["type"] != "gset" || len(members) != len(want) {
		t.Fatalf("body = %s", w.Body.String())
	}
	for i := range want {
		if members[i].(string) != want[i] {
			t.Fatalf("members = %v, want %v", members, want)
		}
	}
}

func TestCRDTState404BeforeAnyOp(t *testing.T) {
	h, _ := newTestHandler(t)
	// A document that only has change-log rows still has no CRDT state.
	w, _ := postJSON(t, h, "/v1/documents/log-only/changes", map[string]any{
		"deviceId": "dev-a",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := crdtState(t, h, "log-only")
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("state = %d %s, want 404 JSON error", w.Code, w.Body.String())
	}
}

func TestCRDTConflicts(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	registerDevice(t, h, "dev-b")

	crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 5}},
	})

	// Once fixed as a counter, a gset declaration is a 409 with no write.
	w, _ := crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "gset",
		"ops": []any{map[string]any{"id": "g1", "value": "x"}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("type conflict = %d, want 409", w.Code)
	}

	// Backward cumulative contribution is a 409 with no write.
	w, _ = crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o2", "value": 4}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("monotonic regression = %d, want 409", w.Code)
	}

	// Same stable id with a different value is a 409 naming the conflict id.
	w, body := crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 6}},
	})
	if w.Code != http.StatusConflict || body["conflictId"] != "o1" {
		t.Fatalf("value conflict = %d body=%s", w.Code, w.Body.String())
	}

	// Same stable id from a different device is likewise a 409.
	w, _ = crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-b", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 5}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict = %d, want 409", w.Code)
	}

	// A batch whose second op conflicts writes neither op.
	w, _ = crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-b", "type": "counter",
		"ops": []any{
			map[string]any{"id": "fresh-1", "value": 5},
			map[string]any{"id": "o1", "value": 5},
		},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("batch conflict = %d, want 409", w.Code)
	}
	w, body = crdtState(t, h, "doc")
	if body["value"].(float64) != 5 {
		t.Fatalf("counter = %v, want 5 (rejected batches must not write)", body["value"])
	}
}

func TestCRDTValidation400(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":1}]}`},
		{"malformed json", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[`},
		{"trailing content", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":1}]}{}`},
		{"missing deviceId", "application/json", `{"type":"counter","ops":[{"id":"o1","value":1}]}`},
		{"empty deviceId", "application/json", `{"deviceId":"","type":"counter","ops":[{"id":"o1","value":1}]}`},
		{"missing type", "application/json", `{"deviceId":"dev-a","ops":[{"id":"o1","value":1}]}`},
		{"bad type", "application/json", `{"deviceId":"dev-a","type":"pn","ops":[{"id":"o1","value":1}]}`},
		{"missing ops", "application/json", `{"deviceId":"dev-a","type":"counter"}`},
		{"empty ops", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[]}`},
		{"empty op id", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"","value":1}]}`},
		{"duplicate op id in batch", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"x","value":1},{"id":"x","value":2}]}`},
		{"counter missing value", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1"}]}`},
		{"counter null value", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":null}]}`},
		{"counter negative value", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":-1}]}`},
		{"counter fractional value", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":1.5}]}`},
		{"counter string value", "application/json", `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":"1"}]}`},
		{"gset missing value", "application/json", `{"deviceId":"dev-a","type":"gset","ops":[{"id":"o1"}]}`},
		{"gset numeric value", "application/json", `{"deviceId":"dev-a","type":"gset","ops":[{"id":"o1","value":1}]}`},
		{"gset null value", "application/json", `{"deviceId":"dev-a","type":"gset","ops":[{"id":"o1","value":null}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, "/v1/documents/doc/crdt/ops", tc.body, tc.contentType)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q", ct)
			}
		})
	}

	// Validation failures write nothing: no type row, so state stays 404.
	w, _ := crdtState(t, h, "doc")
	if w.Code != http.StatusNotFound {
		t.Fatalf("state after rejected submits = %d, want 404", w.Code)
	}
}

func TestCRDTRegistrationAndPermissionGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 1}},
	})

	// An unregistered device gets 404 and no state is revealed.
	w, body := crdtPost(t, h, "doc", map[string]any{
		"deviceId": "ghost", "type": "counter",
		"ops": []any{map[string]any{"id": "g1", "value": 1}},
	})
	if w.Code != http.StatusNotFound || body["value"] != nil {
		t.Fatalf("unknown device = %d body=%s", w.Code, w.Body.String())
	}

	// Revoke permission for dev-a: further submits are 403.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-a", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o2", "value": 2}},
	})
	if w.Code != http.StatusForbidden || body["value"] != nil {
		t.Fatalf("revoked device = %d body=%s", w.Code, w.Body.String())
	}
	w, body = crdtState(t, h, "doc")
	if w.Code != http.StatusOK || body["value"].(float64) != 1 {
		t.Fatalf("state after rejected submit = %d %s", w.Code, w.Body.String())
	}
}

func TestCRDTPathGuards(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	good := `{"deviceId":"dev-a","type":"counter","ops":[{"id":"o1","value":1}]}`

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"empty document id on ops", http.MethodPost, "/v1/documents//crdt/ops"},
		{"empty document id on state", http.MethodGet, "/v1/documents//crdt/state"},
		{"trailing slash on ops", http.MethodPost, "/v1/documents/doc/crdt/ops/"},
		{"trailing slash on state", http.MethodGet, "/v1/documents/doc/crdt/state/"},
		{"extra segment after ops", http.MethodPost, "/v1/documents/doc/crdt/ops/extra"},
		{"missing terminal segment", http.MethodPost, "/v1/documents/doc/crdt/"},
		{"unknown crdt resource", http.MethodPost, "/v1/documents/doc/crdt/bogus"},
		{"wrong method on ops", http.MethodGet, "/v1/documents/doc/crdt/ops?x=1"},
		{"wrong method on state", http.MethodPost, "/v1/documents/doc/crdt/state"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r *http.Request
			if tc.method == http.MethodPost {
				r = newJSONRequest(tc.method, tc.path, good, "application/json")
			} else {
				r = httptest.NewRequest(tc.method, tc.path, nil)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content-type = %q, want JSON not HTML", ct)
			}
			if strings.Contains(rec.Body.String(), "<html") || strings.Contains(rec.Body.String(), "Location") {
				t.Fatalf("response redirects or emits HTML: %s", rec.Body.String())
			}
		})
	}

	// A document literally named "crdt" keeps all of its ordinary routes.
	w, _ := postJSON(t, h, "/v1/documents/crdt/changes", map[string]any{
		"deviceId": "dev-a",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("changes for document named crdt = %d %s", w.Code, w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/crdt/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("read changes for document named crdt = %d", w.Code)
	}
}

func TestCRDTConcurrentCounterSubmits(t *testing.T) {
	h, _ := newTestHandler(t)
	const n = 40
	for i := 0; i < n; i++ {
		registerDevice(t, h, "dev-"+strconv.Itoa(i))
	}

	// One valid (first-ever, monotonic) op per device, submitted concurrently:
	// every batch must land completely and the merge must be the sum of each
	// device's single contribution regardless of arrival order.
	var wg sync.WaitGroup
	var failMu sync.Mutex
	var failures []string
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := crdtPost(t, h, "doc", map[string]any{
				"deviceId": "dev-" + strconv.Itoa(i),
				"type":     "counter",
				"ops":      []any{map[string]any{"id": "op-" + strconv.Itoa(i), "value": i + 1}},
			})
			if w.Code != http.StatusOK {
				failMu.Lock()
				failures = append(failures, fmt.Sprintf("op %d: %d", i, w.Code))
				failMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	if len(failures) != 0 {
		t.Fatalf("failed submits: %v", failures[:3])
	}

	w, body := crdtState(t, h, "doc")
	var want float64 = n * (n + 1) / 2 // sum of 1..n
	if w.Code != http.StatusOK || body["value"].(float64) != want {
		t.Fatalf("state = %d %s, want counter %v", w.Code, w.Body.String(), want)
	}
}

// CRDT state is independent of the change log: CRDT ops take no cursor and a
// document with only CRDT ops is still "unknown" to the change endpoints.
func TestCRDTIndependentOfChangeLog(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")

	crdtPost(t, h, "crdt-only", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "o1", "value": 9}},
	})

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/crdt-only/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("changes = %d", w.Code)
	}
	if len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("changes page = %s, want unknown-document empty/0", w.Body.String())
	}

	// Conversely, change-log rows do not create CRDT state.
	postJSON(t, h, "/v1/documents/log-only/changes", map[string]any{
		"deviceId": "dev-a",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	w, _ = crdtState(t, h, "log-only")
	if w.Code != http.StatusNotFound {
		t.Fatalf("crdt state = %d, want 404", w.Code)
	}
}

func TestCRDTPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-a")
	crdtPost(t, h, "doc", map[string]any{
		"deviceId": "dev-a", "type": "gset",
		"ops": []any{
			map[string]any{"id": "o1", "value": "b"},
			map[string]any{"id": "o2", "value": "a"},
		},
	})
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)

	w, body := crdtState(t, h2, "doc")
	if w.Code != http.StatusOK || body["type"] != "gset" {
		t.Fatalf("state after restart = %d %s", w.Code, w.Body.String())
	}
	members := body["value"].([]any)
	if len(members) != 2 || members[0] != "a" || members[1] != "b" {
		t.Fatalf("members after restart = %v", members)
	}

	// Type remains fixed after restart: a counter declaration is still 409.
	w, _ = crdtPost(t, h2, "doc", map[string]any{
		"deviceId": "dev-a", "type": "counter",
		"ops": []any{map[string]any{"id": "c1", "value": 1}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("type conflict after restart = %d, want 409", w.Code)
	}
}

// Identifiers literally named "subscribe" must be treated as ordinary names:
// the path guard must not flag their change reads or WebSocket subscriptions.
func TestSubscribeNamedIdentifiersAreOrdinary(t *testing.T) {
	// In-process checks for the session-scoped change read and the guard.
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-sub")
	w, _ := postJSON(t, h, "/v1/devices/dev-sub/sessions", map[string]any{"sessionId": "subscribe"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	postJSON(t, h, "/v1/documents/subscribe/changes", map[string]any{
		"deviceId": "dev-sub",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})

	// Session named "subscribe": its change read is an ordinary 200.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/subscribe/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("session=subscribe read = %d %s", w.Code, w.Body.String())
	}
	// Document named "subscribe": same.
	w, body = doRequest(t, h, http.MethodGet, "/v1/sessions/subscribe/documents/subscribe/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("doc=subscribe read = %d %s", w.Code, w.Body.String())
	}

	// Genuinely malformed subscribe paths are still 400 JSON.
	bad := []string{
		"/v1/sessions/subscribe/doc/changes/subscribe?cursor=0",
		"/v1/sessions/subscribe/documents/doc/changes/subscribe/extra?cursor=0",
		"/v1/sessions/subscribe/documents/doc/changes/subscribe/?cursor=0",
	}
	for _, target := range bad {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, upgradeRequest(target))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", target, rec.Code)
		}
	}

	// Real WebSocket upgrades with "subscribe" as the session id and as the
	// document id must complete the 101 handshake.
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-ws-1", "subscribe", "docX", 1)
	setupSession(t, srv, "dev-ws-2", "sessX", "subscribe", 1)

	for _, target := range []string{
		subscribeURL(srv, "subscribe", "docX", "1"),
		subscribeURL(srv, "sessX", "subscribe", "1"),
	} {
		conn, resp := dialWS(t, target)
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("%s status = %d, want 101", target, resp.StatusCode)
		}
		conn.close()
	}
}
