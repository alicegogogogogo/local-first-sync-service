package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// compactBody is the exact response body both CRDT snapshot surfaces emit.
func compactBody(typ, value string, operations, tombstones string) string {
	return `{"type":"` + typ + `","value":` + value + `,"operations":` + operations + `,"tombstones":` + tombstones + "}" + "\n"
}

func getRaw(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, url, nil))
	return w
}

func TestCRDTCompactCounterEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	submit := func(device, id string, value int) {
		t.Helper()
		w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody(device,
			map[string]any{"id": id, "value": value}))
		if w.Code != http.StatusOK {
			t.Fatalf("submit: %d %s", w.Code, w.Body.String())
		}
	}
	submit("dev-1", "a1", 5)
	submit("dev-2", "b1", 3)
	submit("dev-1", "a2", 8)

	w := getRaw(t, h, "/v1/documents/doc/crdt/snapshot")
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != compactBody("counter", "11", "3", "0") {
		t.Fatalf("snapshot body = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("snapshot content type = %q", ct)
	}

	// The state read's merge is byte-identical before and after compaction.
	stateBefore := getRaw(t, h, "/v1/documents/doc/crdt/state").Body.String()

	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	want := compactBody("counter", "11", "2", "0")
	if got := w.Body.String(); got != want {
		t.Fatalf("compact body = %q, want %q", got, want)
	}

	// The compact response and the immediately following snapshot read are
	// byte-for-byte identical.
	w = getRaw(t, h, "/v1/documents/doc/crdt/snapshot")
	if got := w.Body.String(); got != want {
		t.Fatalf("snapshot after compact = %q, want %q", got, want)
	}
	if got := getRaw(t, h, "/v1/documents/doc/crdt/state").Body.String(); got != stateBefore {
		t.Fatalf("state changed by compaction: %q -> %q", stateBefore, got)
	}

	// Repeating the compaction is not an error and reports the same body.
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-2"})
	if w.Code != http.StatusOK || w.Body.String() != want {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}
}

func TestCRDTCompactGSetRegisterAndORSet(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	post := func(url string, body any) *httptest.ResponseRecorder {
		t.Helper()
		w, _ := postJSON(t, h, url, body)
		if w.Code != http.StatusOK {
			t.Fatalf("POST %s: %d %s", url, w.Code, w.Body.String())
		}
		return w
	}

	// gset: g3 is fully covered by g1 and g2.
	post("/v1/documents/g/crdt/ops", crdtGSetBody("dev-1", map[string]any{"id": "g1", "elements": []string{"apple", "banana"}}))
	post("/v1/documents/g/crdt/ops", crdtGSetBody("dev-1", map[string]any{"id": "g2", "elements": []string{"banana", "cherry"}}))
	post("/v1/documents/g/crdt/ops", crdtGSetBody("dev-1", map[string]any{"id": "g3", "elements": []string{"apple"}}))
	if got := getRaw(t, h, "/v1/documents/g/crdt/snapshot").Body.String(); got != compactBody("gset", `["apple","banana","cherry"]`, "3", "0") {
		t.Fatalf("gset snapshot = %q", got)
	}
	if got := post("/v1/documents/g/crdt/compact", map[string]any{"deviceId": "dev-1"}).Body.String(); got != compactBody("gset", `["apple","banana","cherry"]`, "2", "0") {
		t.Fatalf("gset compact = %q", got)
	}

	// register: r1 is superseded by r2 from the same device.
	post("/v1/documents/r/crdt/ops", map[string]any{"deviceId": "dev-1", "type": "register", "ops": []map[string]any{
		{"id": "r1", "version": 1, "value": "first"},
	}})
	post("/v1/documents/r/crdt/ops", map[string]any{"deviceId": "dev-1", "type": "register", "ops": []map[string]any{
		{"id": "r2", "version": 2, "value": "second"},
	}})
	if got := post("/v1/documents/r/crdt/compact", map[string]any{"deviceId": "dev-1"}).Body.String(); got != compactBody("register", `"second"`, "1", "0") {
		t.Fatalf("register compact = %q", got)
	}

	// orset: the tombstoned tag and its tombstone cancel out.
	orset := func(id, action, element string) {
		post("/v1/documents/o/crdt/ops", map[string]any{"deviceId": "dev-1", "type": "orset", "ops": []map[string]any{
			{"id": id, "action": action, "element": element},
		}})
	}
	orset("o1", "add", "apple")
	orset("o2", "add", "banana")
	orset("o3", "remove", "apple")
	if got := getRaw(t, h, "/v1/documents/o/crdt/snapshot").Body.String(); got != compactBody("orset", `["banana"]`, "2", "1") {
		t.Fatalf("orset snapshot = %q", got)
	}
	if got := post("/v1/documents/o/crdt/compact", map[string]any{"deviceId": "dev-1"}).Body.String(); got != compactBody("orset", `["banana"]`, "1", "0") {
		t.Fatalf("orset compact = %q", got)
	}
	if got := getRaw(t, h, "/v1/documents/o/crdt/state").Body.String(); got != "{\"type\":\"orset\",\"value\":[\"banana\"]}\n" {
		t.Fatalf("orset state after compact = %q", got)
	}
}

func TestCRDTCompactBodyContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	before := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String()

	cases := []struct {
		name        string
		body        string
		contentType string
	}{
		{"not json content type", `{"deviceId":"dev-1"}`, "text/plain"},
		{"malformed json", `{"deviceId":`, "application/json"},
		{"trailing content", `{"deviceId":"dev-1"} {}`, "application/json"},
		{"missing deviceId", `{}`, "application/json"},
		{"empty deviceId", `{"deviceId":""}`, "application/json"},
		{"deviceId wrong type", `{"deviceId":7}`, "application/json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/crdt/compact", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("body is not a JSON error: %s", w.Body.String())
			}
		})
	}

	// Every rejection wrote nothing.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != before {
		t.Fatalf("snapshot changed by rejected compacts: %q -> %q", before, got)
	}
}

func TestCRDTCompactDeviceGateAndMissingState(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Unregistered device: 404 JSON, no state leaked.
	w, body := postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "ghost"})
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("unregistered device = %d %v", w.Code, body)
	}

	// Revoked device: 403 JSON, no state leaked.
	registerDevice(t, h, "dev-2")
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-2"})
	if w.Code != http.StatusForbidden || body["error"] == "" {
		t.Fatalf("revoked device = %d %v", w.Code, body)
	}

	// A document with no CRDT operation: 404 JSON.
	w, body = postJSON(t, h, "/v1/documents/never/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("empty document compact = %d %v", w.Code, body)
	}
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/never/crdt/snapshot")
	if w.Code != http.StatusNotFound || body["error"] == "" {
		t.Fatalf("empty document snapshot = %d %v", w.Code, body)
	}

	// Nothing was trimmed by the rejections.
	if got := getRaw(t, h, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compactBody("counter", "5", "1", "0") {
		t.Fatalf("snapshot after rejections = %q", got)
	}
}

func TestCRDTCompactPathAndMethodShape(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"compact with GET", http.MethodGet, "/v1/documents/doc/crdt/compact"},
		{"compact with DELETE", http.MethodDelete, "/v1/documents/doc/crdt/compact"},
		{"snapshot with POST", http.MethodPost, "/v1/documents/doc/crdt/snapshot"},
		{"snapshot with PUT", http.MethodPut, "/v1/documents/doc/crdt/snapshot"},
		{"empty document id compact", http.MethodPost, "/v1/documents//crdt/compact"},
		{"empty document id snapshot", http.MethodGet, "/v1/documents//crdt/snapshot"},
		{"compact trailing slash", http.MethodPost, "/v1/documents/doc/crdt/compact/"},
		{"snapshot trailing slash", http.MethodGet, "/v1/documents/doc/crdt/snapshot/"},
		{"compact extra segment", http.MethodPost, "/v1/documents/doc/crdt/compact/x"},
		{"snapshot extra segment", http.MethodGet, "/v1/documents/doc/crdt/snapshot/x"},
		{"unknown crdt verb", http.MethodGet, "/v1/documents/doc/crdt/compacts"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400 (%s)", tc.method, tc.path, w.Code, w.Body.String())
			}
			if !strings.Contains(w.Body.String(), `"error"`) {
				t.Fatalf("body is not a JSON error: %s", w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}
}

func TestCRDTCompactLeavesChangeLogAlone(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// The document has an ordinary change log next to its CRDT state.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c1", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ = postJSON(t, h, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// No cursor was allocated and no change record appeared.
	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	changes := body["changes"].([]any)
	if len(changes) != 1 || changes[0].(map[string]any)["id"] != "c1" {
		t.Fatalf("changes after compaction = %v", changes)
	}
	if body["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor after compaction = %v, want 1", body["nextCursor"])
	}
}

func TestCRDTSnapshotPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	s1, err := store.Open(filepath.Join(dir, "sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	h1 := NewHandler(s1)
	registerDevice(t, h1, "dev-1")
	w, _ := postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a1", "value": 5}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h1, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a2", "value": 8}))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h1, "/v1/documents/doc/crdt/compact", map[string]any{"deviceId": "dev-1"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	compacted := w.Body.String()
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(filepath.Join(dir, "sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	// Counts, state and decisions are unchanged after the restart.
	if got := getRaw(t, h2, "/v1/documents/doc/crdt/snapshot").Body.String(); got != compacted {
		t.Fatalf("snapshot after restart = %q, want %q", got, compacted)
	}
	w, _ = postJSON(t, h2, "/v1/documents/doc/crdt/ops", crdtCounterBody("dev-1",
		map[string]any{"id": "a3", "value": 7}))
	if w.Code != http.StatusConflict {
		t.Fatalf("regression after restart = %d, want 409", w.Code)
	}
}
