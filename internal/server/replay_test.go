package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func replayRequestRaw(t *testing.T, h http.Handler, doc string, contentType string, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/"+doc+"/replay", strings.NewReader(body))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var decoded map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &decoded)
	}
	return w, decoded
}

func registerReplayDevice(t *testing.T, h http.Handler, id string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/devices", map[string]string{"deviceId": id})
	if w.Code != http.StatusOK {
		t.Fatalf("register device %s: %d %s", id, w.Code, w.Body.String())
	}
}

func replayBody(t *testing.T, h http.Handler, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+doc+"/replay", body)
}

func TestReplayCreatesChanges(t *testing.T) {
	h, _ := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes": []any{
			map[string]any{"id": "o1", "payload": map[string]any{"n": 1}},
			map[string]any{"id": "o2", "payload": []any{1, true, nil}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	if len(results) != 2 {
		t.Fatalf("results = %v", results)
	}
	r0 := results[0].(map[string]any)
	if r0["id"] != "o1" || r0["created"] != true || r0["cursor"].(float64) != 1 {
		t.Fatalf("r0 = %v", r0)
	}
	r1 := results[1].(map[string]any)
	if r1["id"] != "o2" || r1["created"] != true || r1["cursor"].(float64) != 2 {
		t.Fatalf("r1 = %v", r1)
	}

	// Results are readable through the ordinary change listing in order.
	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 2 ||
		rows[0].(map[string]any)["id"] != "o1" ||
		rows[1].(map[string]any)["deviceId"] != "dev-1" ||
		list["nextCursor"].(float64) != 2 {
		t.Fatalf("listing = %v", list)
	}
}

func TestReplayIdempotentAcrossEndpoints(t *testing.T) {
	h, _ := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	// First post via the ordinary changes endpoint.
	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	// Replay the same operation: idempotent with the first cursor.
	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes": []any{
			map[string]any{"id": "x", "payload": map[string]any{"k": "v"}},
			map[string]any{"id": "y", "payload": map[string]any{"k": 2}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	if results[0].(map[string]any)["created"] != false || results[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("idempotent item = %v", results[0])
	}
	if results[1].(map[string]any)["created"] != true || results[1].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("new item = %v", results[1])
	}

	// Replaying again is fully idempotent with unchanged cursors.
	w, body = replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes": []any{
			map[string]any{"id": "x", "payload": map[string]any{"k": "v"}},
			map[string]any{"id": "y", "payload": map[string]any{"k": 2}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("repeat replay status = %d", w.Code)
	}
	for i, wantCursor := range []float64{1, 2} {
		r := body["results"].([]any)[i].(map[string]any)
		if r["created"] != false || r["cursor"].(float64) != wantCursor {
			t.Fatalf("repeat item %d = %v", i, r)
		}
	}
}

func TestReplayConflict409ZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	// Seed an id via ordinary post.
	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	before, beforeNext, _ := s.ListChanges("doc1", 0, 100)

	// Payload mismatch -> 409 naming the conflicting id, batch untouched.
	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes": []any{
			map[string]any{"id": "new", "payload": map[string]any{"n": 1}},
			map[string]any{"id": "x", "payload": map[string]any{"k": "different"}},
		},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("payload conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil || !strings.Contains(body["error"].(string), "x") {
		t.Fatalf("conflict should report id x: %v", body)
	}

	// Device mismatch on the existing id -> 409.
	registerReplayDevice(t, h, "dev-2")
	w, _ = replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict status = %d body = %s", w.Code, w.Body.String())
	}

	after, afterNext, _ := s.ListChanges("doc1", 0, 100)
	if len(after) != len(before) || afterNext != beforeNext {
		t.Fatalf("conflict batch wrote: before=%d/%d after=%d/%d", len(before), beforeNext, len(after), afterNext)
	}
}

func TestReplayRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")
	const doc = "doc1"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-1","changes":[{"id":"a","payload":1}]}`},
		{"missing content type", "", `{"deviceId":"dev-1","changes":[{"id":"a","payload":1}]}`},
		{"json suffix content type", "application/vnd.api+json", `{"deviceId":"dev-1","changes":[{"id":"a","payload":1}]}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"dev-1","changes":[{"id":"a","payload":1}]}x`},
		{"empty deviceId", "application/json", `{"deviceId":"","changes":[{"id":"a","payload":1}]}`},
		{"missing deviceId", "application/json", `{"changes":[{"id":"a","payload":1}]}`},
		{"numeric deviceId", "application/json", `{"deviceId":7,"changes":[{"id":"a","payload":1}]}`},
		{"empty changes", "application/json", `{"deviceId":"dev-1","changes":[]}`},
		{"missing changes", "application/json", `{"deviceId":"dev-1"}`},
		{"empty id", "application/json", `{"deviceId":"dev-1","changes":[{"id":"","payload":1}]}`},
		{"numeric id", "application/json", `{"deviceId":"dev-1","changes":[{"id":9,"payload":1}]}`},
		{"missing payload", "application/json", `{"deviceId":"dev-1","changes":[{"id":"a"}]}`},
		{"duplicate ids in batch", "application/json", `{"deviceId":"dev-1","changes":[{"id":"a","payload":1},{"id":"a","payload":2}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, body := replayRequestRaw(t, h, doc, tc.contentType, tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			if body["error"] == nil {
				t.Fatalf("body = %s", w.Body.String())
			}
		})
	}

	// Zero writes across all rejected requests.
	rows, next, err := s.ListChanges(doc, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestReplayUnknownDevice404(t *testing.T) {
	h, s := newTestHandler(t)

	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "ghost",
		"changes":  []any{map[string]any{"id": "a", "payload": 1}},
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("body = %s", w.Body.String())
	}

	// Error response carries no change contents, and nothing was written.
	raw := w.Body.String()
	if strings.Contains(raw, `"payload"`) || strings.Contains(raw, `"changes"`) {
		t.Fatalf("404 leaked change contents: %s", raw)
	}
	known, err := s.DocumentExists("doc1")
	if err != nil {
		t.Fatal(err)
	}
	if known {
		t.Fatal("replay from unknown device created the document")
	}
}

func TestReplayRevokedDevice403(t *testing.T) {
	h, s := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	// Seed one change so the document exists.
	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "seed", "payload": map[string]any{"n": 0}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]string{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke status = %d", w.Code)
	}

	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "a", "payload": 1}},
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403, body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("body = %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"payload"`) {
		t.Fatalf("403 leaked change contents: %s", w.Body.String())
	}
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("revoked replay wrote: rows=%d next=%d", len(rows), next)
	}

	// Granting again lets the same replay through.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]string{
		"deviceId": "dev-1", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("grant status = %d", w.Code)
	}
	w, body = replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "a", "payload": 1}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != true {
		t.Fatalf("replay after grant = %d %v", w.Code, body)
	}
}

func TestReplayConcurrentSharesCursors(t *testing.T) {
	h, _ := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := replayBody(t, h, "doc1", map[string]any{
				"deviceId": "dev-1",
				"changes":  []any{map[string]any{"id": fmt.Sprintf("r%02d", i), "payload": map[string]any{"i": i}}},
			})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("replay %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != n {
		t.Fatalf("stored %d changes, want %d", len(rows), n)
	}
	if body["nextCursor"].(float64) != n {
		t.Fatalf("nextCursor = %v, want %d", body["nextCursor"], n)
	}
	// Cursors must be contiguous 1..n with no duplicates.
	seen := make(map[int64]bool, n)
	for _, row := range rows {
		c := int64(row.(map[string]any)["cursor"].(float64))
		if c < 1 || c > n || seen[c] {
			t.Fatalf("bad/duplicate cursor %d in %v", c, rows)
		}
		seen[c] = true
	}
}

// Replay interleaved with ordinary commits shares one contiguous cursor run.
func TestReplayInterleavedWithCommits(t *testing.T) {
	h, _ := newTestHandler(t)
	registerReplayDevice(t, h, "dev-1")

	w, _ := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "post-1", "payload": 1}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w, body := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "replay-1", "payload": 2}},
	})
	if w.Code != 200 || body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 2 {
		t.Fatalf("replay cursor = %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "post-2", "payload": 3}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 3 || list["nextCursor"].(float64) != 3 {
		t.Fatalf("interleaved log = %v", list)
	}
	got := []string{
		rows[0].(map[string]any)["id"].(string),
		rows[1].(map[string]any)["id"].(string),
		rows[2].(map[string]any)["id"].(string),
	}
	want := []string{"post-1", "replay-1", "post-2"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order = %v, want %v", got, want)
		}
	}
}

func TestReplayPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerReplayDevice(t, h, "dev-1")
	w, _ := replayBody(t, h, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "o1", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Identical replay is idempotent with the original cursor and payload.
	w, body := replayBody(t, h2, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "o1", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != http.StatusOK || body["results"].([]any)[0].(map[string]any)["created"] != false ||
		body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("replay after restart = %d %v", w.Code, body)
	}
	// A differing payload is still a 409 after restart.
	w, _ = replayBody(t, h2, "doc1", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "o1", "payload": map[string]any{"k": "x"}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d", w.Code)
	}
	// The original payload reads back intact.
	w, list := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	row := list["changes"].([]any)[0].(map[string]any)
	if row["payload"].(map[string]any)["k"] != "v" {
		t.Fatalf("payload after restart = %v", row["payload"])
	}
}

func TestReplayEmptyDocumentIDAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)

	// Empty documentID -> 400 JSON, not a redirect.
	r := httptest.NewRequest(http.MethodPost, "/v1/documents//replay", bytes.NewReader([]byte(`{"deviceId":"d","changes":[{"id":"a","payload":1}]}`)))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("empty doc replay = %d %q", w.Code, w.Header().Get("Content-Type"))
	}

	// GET on the replay path -> 400 JSON.
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, httptest.NewRequest(http.MethodGet, "/v1/documents/d/replay", nil))
	if w2.Code != http.StatusBadRequest || w2.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("GET replay = %d %q", w2.Code, w2.Header().Get("Content-Type"))
	}
}
