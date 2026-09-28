package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func TestChangesQueryOnlineHitsAndMissingInOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)

	// The answer follows the request order, mixes online hits with ids that
	// never appeared, and reports count equal to the number of answers.
	w, body := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes": []map[string]any{
			{"id": "c3"}, {"id": "ghost"}, {"id": "c1"}, {"id": "c2"},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("query = %d %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	if len(results) != 4 || int(body["count"].(float64)) != 4 {
		t.Fatalf("results/count = %v / %v", results, body["count"])
	}
	want := []struct {
		id, status, device string
		cursor             float64
		payload            any
	}{
		{"c3", "online", "dev-1", 3, map[string]any{"n": float64(3)}},
		{"ghost", "missing", "", 0, nil},
		{"c1", "online", "dev-1", 1, map[string]any{"n": float64(1)}},
		{"c2", "online", "dev-1", 2, map[string]any{"n": float64(2)}},
	}
	for i, wr := range want {
		got := results[i].(map[string]any)
		if got["id"] != wr.id || got["status"] != wr.status {
			t.Fatalf("result %d = %v, want id=%s status=%s", i, got, wr.id, wr.status)
		}
		if wr.status == "online" {
			if got["deviceId"] != wr.device || got["cursor"] != wr.cursor {
				t.Fatalf("result %d = %v, want device/cursor %s/%v", i, got, wr.device, wr.cursor)
			}
			if got["payload"] == nil || !reflect.DeepEqual(got["payload"], wr.payload) {
				t.Fatalf("result %d payload = %v, want %v", i, got["payload"], wr.payload)
			}
		} else if _, ok := got["payload"]; ok {
			t.Fatalf("missing result %d carries payload: %v", i, got)
		}
	}

	// The body is one compact JSON line plus trailing newline with the fixed
	// top-level results-then-count order and the fixed per-element key order
	// id, status, deviceId, payload, cursor.
	if got := w.Body.String(); got !=
		`{"results":[{"id":"c3","status":"online","deviceId":"dev-1","payload":{"n":3},"cursor":3},{"id":"ghost","status":"missing"},{"id":"c1","status":"online","deviceId":"dev-1","payload":{"n":1},"cursor":1},{"id":"c2","status":"online","deviceId":"dev-1","payload":{"n":2},"cursor":2}],"count":4}`+"\n" {
		t.Fatalf("query body = %q", got)
	}
}

func TestChangesQueryUnknownDocumentAllMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w, body := postJSON(t, h, "/v1/documents/never/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "a"}, {"id": "b"}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for _, r := range body["results"].([]any) {
		if r.(map[string]any)["status"] != "missing" {
			t.Fatalf("want every id missing, got %v", r)
		}
	}
	if int(body["count"].(float64)) != 2 {
		t.Fatalf("count = %v", body["count"])
	}
}

func TestChangesQueryTrimmedIdsReportSummaryOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// c1 and c2 were trimmed: the answer reports the trimmed marker and the
	// first cursor only — no device, no payload. c3 stays online and the
	// never-seen id stays missing.
	w, body := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c2"}, {"id": "never"}, {"id": "c1"}, {"id": "c3"}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	results := body["results"].([]any)
	c2 := results[0].(map[string]any)
	if c2["status"] != "compacted" || c2["cursor"] != float64(2) {
		t.Fatalf("c2 = %v, want compacted cursor 2", c2)
	}
	if _, ok := c2["payload"]; ok {
		t.Fatalf("trimmed c2 leaks payload: %v", c2)
	}
	if _, ok := c2["deviceId"]; ok {
		t.Fatalf("trimmed c2 leaks deviceId: %v", c2)
	}
	if results[1].(map[string]any)["status"] != "missing" {
		t.Fatalf("never = %v, want missing", results[1])
	}
	c1 := results[2].(map[string]any)
	if c1["status"] != "compacted" || c1["cursor"] != float64(1) {
		t.Fatalf("c1 = %v, want compacted cursor 1", c1)
	}
	c3 := results[3].(map[string]any)
	if c3["status"] != "online" || c3["cursor"] != float64(3) {
		t.Fatalf("c3 = %v, want online cursor 3", c3)
	}

	if got := w.Body.String(); got !=
		`{"results":[{"id":"c2","status":"compacted","cursor":2},{"id":"never","status":"missing"},{"id":"c1","status":"compacted","cursor":1},{"id":"c3","status":"online","deviceId":"dev-1","payload":{"n":3},"cursor":3}],"count":4}`+"\n" {
		t.Fatalf("trimmed query body = %q", got)
	}
}

func TestChangesQueryStableAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-restart.db")
	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)

	w, _ := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c2"}, {"id": "x"}, {"id": "c1"}},
	})
	before := w.Body.String()
	if w.Code != http.StatusOK {
		t.Fatal(before)
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
	w2, _ := postJSON(t, h2, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c2"}, {"id": "x"}, {"id": "c1"}},
	})
	if w2.Body.String() != before {
		t.Fatalf("query body changed across restart:\nbefore=%q\nafter =%q", before, w2.Body.String())
	}
}

func TestChangesQueryRejectsBadShapeBeforeDeviceLookup(t *testing.T) {
	h, _ := newTestHandler(t)
	// No device is registered: a request-shape failure must still beat the
	// 404 device-existence verdict.
	url := "/v1/documents/doc/changes/query"
	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-1","changes":[{"id":"a"}]}`},
		{"missing content type", "", `{"deviceId":"dev-1","changes":[{"id":"a"}]}`},
		{"invalid json", "application/json", `{not json`},
		{"trailing content", "application/json", `{"deviceId":"dev-1","changes":[{"id":"a"}]} {}`},
		{"missing changes", "application/json", `{"deviceId":"dev-1"}`},
		{"null changes", "application/json", `{"deviceId":"dev-1","changes":null}`},
		{"empty changes", "application/json", `{"deviceId":"dev-1","changes":[]}`},
		{"changes not an array", "application/json", `{"deviceId":"dev-1","changes":"a"}`},
		{"missing deviceId", "application/json", `{"changes":[{"id":"a"}]}`},
		{"empty deviceId", "application/json", `{"deviceId":"","changes":[{"id":"a"}]}`},
		{"non-string deviceId", "application/json", `{"deviceId":7,"changes":[{"id":"a"}]}`},
		{"element missing id", "application/json", `{"deviceId":"dev-1","changes":[{}]}`},
		{"null element", "application/json", `{"deviceId":"dev-1","changes":[null]}`},
		{"scalar element", "application/json", `{"deviceId":"dev-1","changes":["a"]}`},
		{"non-string id", "application/json", `{"deviceId":"dev-1","changes":[{"id":3}]}`},
		{"duplicate ids", "application/json", `{"deviceId":"dev-1","changes":[{"id":"a"},{"id":"a"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, url, strings.NewReader(tc.body))
			if tc.contentType != "" {
				r.Header.Set("Content-Type", tc.contentType)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}
}

func TestChangesQueryUnknownDeviceIs404(t *testing.T) {
	h, _ := newTestHandler(t)

	w, _ := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "ghost",
		"changes":  []map[string]any{{"id": "c1"}},
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), `"results"`) {
		t.Fatalf("404 body leaks results: %s", w.Body.String())
	}
}

func TestChangesQueryRevokedDeviceIs403WithoutContent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc", "dev-1", 2)
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, _ := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-2",
		"changes":  []map[string]any{{"id": "c1"}, {"id": "c2"}},
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), `"payload"`) || strings.Contains(w.Body.String(), `"results"`) {
		t.Fatalf("403 body leaks change content: %s", w.Body.String())
	}

	// A granted device reads normally.
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "grant",
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-2",
		"changes":  []map[string]any{{"id": "c1"}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["status"] != "online" {
		t.Fatalf("granted query = %d %s", w.Code, w.Body.String())
	}
}

func TestChangesQueryIsReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 1)

	// Querying does not consume a cursor: the next commit still lands on 2.
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c1"}, {"id": "ghost"}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if cursor := body["results"].([]any)[0].(map[string]any)["cursor"]; cursor != float64(2) {
		t.Fatalf("next cursor after query = %v, want 2", cursor)
	}
}

// A parked long poll is not woken by a by-id query: the wait runs to its
// deadline and reports a timeout rather than returning early.
func TestChangesQueryDoesNotWakeLongPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 1)

	pollDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet,
			"/v1/documents/doc/changes/poll?after=1&waitMs=1000", nil))
		pollDone <- w
	}()

	// Give the poll time to park, then run a query that finds the change.
	time.Sleep(100 * time.Millisecond)
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes/query", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "c1"}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	select {
	case w := <-pollDone:
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["timedOut"] != true {
			t.Fatalf("poll returned early after query: %s", w.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked poll never returned")
	}
}

func TestChangesQueryMethodAndPathShape(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method, url string
	}{
		{http.MethodGet, "/v1/documents/doc/changes/query"},
		{http.MethodPut, "/v1/documents/doc/changes/query"},
		{http.MethodDelete, "/v1/documents/doc/changes/query"},
		{http.MethodPost, "/v1/documents/doc/changes/query/"},
		{http.MethodPost, "/v1/documents/doc/changes/query/extra"},
		{http.MethodPost, "/v1/documents//changes/query"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.url, func(t *testing.T) {
			w, _ := doRequest(t, h, tc.method, tc.url)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q, want application/json", ct)
			}
		})
	}
}

// A failed version rebind — posting an already-bound name against another
// cursor — now carries the offending version name in a structured conflictId
// field alongside the error message (PUT .../versions/{name} moves the marker
// and cannot conflict, so the rebind failure only arises on the collection
// POST).
func TestSnapshotVersionRebindConflictCarriesConflictID(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)

	w, body := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("rebind conflict = %d %s", w.Code, w.Body.String())
	}
	if body["conflictId"] != "v1" || body["error"] == nil {
		t.Fatalf("409 body = %v, want error and conflictId=v1", body)
	}

	// The marker stayed on its original cursor; the failed rebind wrote nothing.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if !strings.Contains(w.Body.String(), `"name":"v1","snapshotCursor":1`) {
		t.Fatalf("conflict changed the binding: %s", w.Body.String())
	}
}
