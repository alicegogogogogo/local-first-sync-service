package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCRDTSnapshotCounterEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	submit := func(device, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device,
			map[string]any{"id": id, "value": value}))
		if w.Code != http.StatusOK {
			t.Fatalf("submit %s=%d: %d %s", id, value, w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 3)
	submit("dev-2", "b1", 7)
	submit("dev-1", "a2", 5)

	// The snapshot read is one compact JSON line with the keys in type, value,
	// operations, tombstones order and a trailing newline.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	wantBefore := "{\"type\":\"counter\",\"value\":12,\"operations\":3,\"tombstones\":0}\n"
	if got := w.Body.String(); got != wantBefore {
		t.Fatalf("snapshot body = %q, want %q", got, wantBefore)
	}

	// Compaction drops the dominated op (a1) and answers with exactly what a
	// snapshot read immediately afterwards returns.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	wantAfter := "{\"type\":\"counter\",\"value\":12,\"operations\":2,\"tombstones\":0}\n"
	if got := w.Body.String(); got != wantAfter {
		t.Fatalf("compact body = %q, want %q", got, wantAfter)
	}
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	if got := w.Body.String(); got != wantAfter {
		t.Fatalf("snapshot after compact = %q, want %q", got, wantAfter)
	}

	// The merged state read is byte-for-byte unchanged.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/state")
	if got := w.Body.String(); got != "{\"type\":\"counter\",\"value\":12}\n" {
		t.Fatalf("state after compact = %q", got)
	}

	// Repeating the compaction is an idempotent 200 with the same body.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK || w.Body.String() != wantAfter {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}
}

func TestCRDTSnapshotORSetCounts(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	orset := func(id, action, element string) map[string]any {
		return map[string]any{"id": id, "action": action, "element": element}
	}
	submit := func(ops ...map[string]any) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops",
			map[string]any{"deviceId": "dev-1", "type": "orset", "ops": ops})
		if w.Code != http.StatusOK {
			t.Fatalf("submit: %d %s", w.Code, w.Body.String())
		}
	}
	submit(orset("o1", "add", "apple"))
	submit(orset("o2", "add", "banana"))
	submit(orset("o3", "remove", "apple"))

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	want := "{\"type\":\"orset\",\"value\":[\"banana\"],\"operations\":3,\"tombstones\":1}\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("snapshot = %q, want %q", got, want)
	}

	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	want = "{\"type\":\"orset\",\"value\":[\"banana\"],\"operations\":3,\"tombstones\":0}\n"
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("compact = %d %q, want %q", w.Code, w.Body.String(), want)
	}
}

func TestCRDTCompactRequestValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatalf("seed submit = %d", w.Code)
	}

	cases := []struct {
		name        string
		body        string
		contentType string
	}{
		{"missing deviceId", `{}`, "application/json"},
		{"empty deviceId", `{"deviceId":""}`, "application/json"},
		{"numeric deviceId", `{"deviceId":5}`, "application/json"},
		{"invalid JSON", `{"deviceId":`, "application/json"},
		{"trailing content", `{"deviceId":"dev-1"} {}`, "application/json"},
		{"wrong content type", `{"deviceId":"dev-1"}`, "text/plain"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/crdt/compact", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("body is not a JSON error: %s", w.Body.String())
			}
		})
	}

	// Every rejection wrote nothing: the operation count is untouched.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	if got := w.Body.String(); got != "{\"type\":\"counter\",\"value\":5,\"operations\":1,\"tombstones\":0}\n" {
		t.Fatalf("snapshot after rejections = %q", got)
	}
}

func TestCRDTCompactDeviceAndPermissionGates(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatalf("seed submit = %d", w.Code)
	}

	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "ghost"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered device = %d, want 404", w.Code)
	}

	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-2"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d, want 403", w.Code)
	}

	// Neither rejection trimmed anything.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	if !strings.Contains(w.Body.String(), `"operations":1`) {
		t.Fatalf("snapshot after rejections = %q", w.Body.String())
	}
}

func TestCRDTCompactAndSnapshot404BeforeAnyOp(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/never/crdt/snapshot")
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("snapshot = %d %v, want 404 JSON", w.Code, body)
	}
	w, body = postJSON(t, h, "/v1/documents/never/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("compact = %d %v, want 404 JSON", w.Code, body)
	}
}

func TestCRDTCompactSnapshotMethodAndPathMismatch(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/documents/doc/crdt/compact"},
		{http.MethodPut, "/v1/documents/doc/crdt/compact"},
		{http.MethodPost, "/v1/documents/doc/crdt/snapshot"},
		{http.MethodDelete, "/v1/documents/doc/crdt/snapshot"},
		{http.MethodGet, "/v1/documents/doc/crdt/snapshot/x"},
		{http.MethodPost, "/v1/documents/doc/crdt/compact/x"},
		{http.MethodGet, "/v1/documents/doc/crdt/snapshot/"},
		{http.MethodPost, "/v1/documents/doc/crdt/compact/"},
		{http.MethodGet, "/v1/documents//crdt/snapshot"},
		{http.MethodPost, "/v1/documents//crdt/compact"},
	}
	for _, tc := range cases {
		w, body := doRequest(t, h, tc.method, tc.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		if body["error"] == "" {
			t.Fatalf("%s %s: expected a JSON error, got %s", tc.method, tc.path, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s: content type = %q", tc.method, tc.path, ct)
		}
	}
}

func TestCRDTCompactLeavesChangeLogAlone(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatalf("seed submit = %d", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d", w.Code)
	}

	// Compaction allocates no cursor and writes no change record.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("changes = %d", w.Code)
	}
	if body["nextCursor"].(float64) != 0 {
		t.Fatalf("nextCursor = %v, want 0", body["nextCursor"])
	}
	if changes, ok := body["changes"].([]any); !ok || len(changes) != 0 {
		t.Fatalf("changes = %v, want empty", body["changes"])
	}
}

// Compaction never moves the merged value, so subscribers observe nothing.
func TestCRDTCompactIsSilentForSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, hs := dialWS(t, crdtSubscribeURL(srv, "sess", "doc"))
	if conn == nil {
		t.Fatalf("status = %d", hs.StatusCode)
	}
	defer conn.close()

	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a1", 3))); code != http.StatusOK {
		t.Fatalf("submit a1 = %d", code)
	}
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a2", 5))); code != http.StatusOK {
		t.Fatalf("submit a2 = %d", code)
	}
	// The subscription sees the first state and the advance; drain both.
	if got := crdtCounterValue(t, conn.readCRDTState()); got != 3 {
		t.Fatalf("first state = %d, want 3", got)
	}
	if got := crdtCounterValue(t, conn.readCRDTState()); got != 5 {
		t.Fatalf("second state = %d, want 5", got)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"}); code != http.StatusOK {
		t.Fatalf("compact = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("compaction pushed a frame")
	}
	conn.clearReadDeadline()

	// The channel is still alive: a genuine state change still pushes.
	if code := postCRDTOps(t, srv, "doc", crdtCounterBody("dev-1", crdtCounterOp("a3", 9))); code != http.StatusOK {
		t.Fatalf("submit a3 = %d", code)
	}
	if got := crdtCounterValue(t, conn.readCRDTState()); got != 9 {
		t.Fatalf("state after compact = %d, want 9", got)
	}
}

// Concurrent compactions serialize: every one answers 200 and the final
// counts match a single compaction.
func TestCRDTCompactConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	for i, v := range []int{1, 2, 3} {
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
			map[string]any{"id": string(rune('a'+i)) + "1", "value": v}))
		if w.Code != http.StatusOK {
			t.Fatalf("seed submit = %d", w.Code)
		}
	}

	const n = 8
	var wg sync.WaitGroup
	statuses := make(chan int, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			w, _ := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
			statuses <- w.Code
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)
	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("concurrent compact = %d, want 200", code)
		}
	}

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/crdt/snapshot")
	want := "{\"type\":\"counter\",\"value\":3,\"operations\":1,\"tombstones\":0}\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("snapshot after concurrent compacts = %q, want %q", got, want)
	}
}
