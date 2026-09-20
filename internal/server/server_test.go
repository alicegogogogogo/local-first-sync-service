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

func newTestHandler(t *testing.T) (http.Handler, *store.Store) {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return NewHandler(s), s
}

func postJSON(t *testing.T, h http.Handler, url string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var r *http.Request
	switch b := body.(type) {
	case string:
		r = httptest.NewRequest(http.MethodPost, url, strings.NewReader(b))
	case []byte:
		r = httptest.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	default:
		raw, _ := json.Marshal(b)
		r = httptest.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var decoded map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &decoded)
	}
	return w, decoded
}

func doRequest(t *testing.T, h http.Handler, method, url string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, url, nil))
	var decoded map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &decoded)
	}
	return w, decoded
}

func TestHealth(t *testing.T) {
	h, _ := newTestHandler(t)
	w, body := doRequest(t, h, http.MethodGet, "/healthz")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	if body["status"] != "ok" {
		t.Fatalf("body = %s", w.Body.String())
	}
}

func TestPostValidBatch(t *testing.T) {
	h, _ := newTestHandler(t)
	w, body := postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev-A",
		"changes": []map[string]any{
			{"id": "c1", "payload": map[string]any{"text": "hello"}},
			{"id": "c2", "payload": []any{1, 2, 3}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	results, ok := body["results"].([]any)
	if !ok || len(results) != 2 {
		t.Fatalf("results = %v", body["results"])
	}
	r0 := results[0].(map[string]any)
	if r0["id"] != "c1" || r0["created"] != true || r0["cursor"].(float64) != 1 {
		t.Fatalf("r0 = %v", r0)
	}
	r1 := results[1].(map[string]any)
	if r1["id"] != "c2" || r1["cursor"].(float64) != 2 {
		t.Fatalf("r1 = %v", r1)
	}
}

func TestPostRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	url := "/v1/documents/doc1/changes"
	if _, err := s.PostChanges("doc1", []store.Change{
		{ID: "existing", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
	}{
		{"wrong content type", "text/plain", `{"deviceId":"d","changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"missing content type", "", `{"deviceId":"d","changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"content type with json suffix", "application/vnd.api+json", `{"deviceId":"d","changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"malformed json", "application/json", `{"deviceId":"d","changes":[`, http.StatusBadRequest},
		{"trailing content", "application/json", `{"deviceId":"d","changes":[{"id":"a","payload":1}]}garbage`, http.StatusBadRequest},
		{"empty deviceId", "application/json", `{"deviceId":"","changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"missing deviceId", "application/json", `{"changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"numeric deviceId", "application/json", `{"deviceId":7,"changes":[{"id":"a","payload":1}]}`, http.StatusBadRequest},
		{"empty changes", "application/json", `{"deviceId":"d","changes":[]}`, http.StatusBadRequest},
		{"missing changes", "application/json", `{"deviceId":"d"}`, http.StatusBadRequest},
		{"empty id", "application/json", `{"deviceId":"d","changes":[{"id":"","payload":1}]}`, http.StatusBadRequest},
		{"numeric id", "application/json", `{"deviceId":"d","changes":[{"id":123,"payload":1}]}`, http.StatusBadRequest},
		{"missing payload", "application/json", `{"deviceId":"d","changes":[{"id":"a"}]}`, http.StatusBadRequest},
		{"duplicate ids in batch", "application/json", `{"deviceId":"d","changes":[{"id":"a","payload":1},{"id":"a","payload":2}]}`, http.StatusBadRequest},
		{"conflicting existing", "application/json", `{"deviceId":"d","changes":[{"id":"existing","payload":2}]}`, http.StatusConflict},
		{"conflict device mismatch", "application/json", `{"deviceId":"other","changes":[{"id":"existing","payload":{"v":1}}]}`, http.StatusConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, url, strings.NewReader(tc.body))
			if tc.contentType != "" {
				r.Header.Set("Content-Type", tc.contentType)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d, body = %s", w.Code, tc.wantStatus, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("error content type = %q", ct)
			}
			var errBody map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &errBody); err != nil || errBody["error"] == "" {
				t.Fatalf("error body = %s", w.Body.String())
			}
		})
	}
}

func TestPostRejectsNonStringAndBatchZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)

	// Numeric id instead of string.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", `{"deviceId":"d","changes":[{"id":123,"payload":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("numeric id status = %d", w.Code)
	}

	// Batch with duplicate id must write nothing: seed a valid batch, then send
	// a mixed bad batch, then verify listing only contains the seed.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "ok1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes": []any{
			map[string]any{"id": "bad-a", "payload": map[string]any{"n": 2}},
			map[string]any{"id": "bad-a", "payload": map[string]any{"n": 3}},
		},
	})
	if w.Code != 400 {
		t.Fatalf("dup batch status = %d", w.Code)
	}
	rows, _, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "ok1" {
		t.Fatalf("zero-write violated: %+v", rows)
	}
}

func TestPostIdempotentAndConflictHTTP(t *testing.T) {
	h, s := newTestHandler(t)

	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	// Identical repost: 200, created false, same cursor.
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	})
	if w.Code != 200 {
		t.Fatalf("repost status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("repost result = %v", r)
	}

	// Conflicting repost: 409 and zero additional writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes": []any{
			map[string]any{"id": "y", "payload": map[string]any{"n": 2}},
			map[string]any{"id": "x", "payload": map[string]any{"k": "different"}},
		},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", w.Code, w.Body.String())
	}
	rows, _, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 {
		t.Fatalf("conflict batch wrote rows: %+v", rows)
	}
}

func TestEmptyDocumentIDReturnsJSON400(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		name   string
		method string
		url    string
		body   any
	}{
		{"empty get", http.MethodGet, "/v1/documents//changes", nil},
		{"empty post", http.MethodPost, "/v1/documents//changes", map[string]any{"deviceId": "d", "changes": []any{map[string]any{"id": "a", "payload": 1}}}},
		{"empty merge", http.MethodPost, "/v1/documents//merge", map[string]any{"deviceId": "d", "baseCursor": 0, "change": map[string]any{"id": "a", "payload": map[string]any{}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var w *httptest.ResponseRecorder
			var body map[string]any
			if tc.method == http.MethodPost {
				w, body = postJSON(t, h, tc.url, tc.body)
			} else {
				w, body = doRequest(t, h, tc.method, tc.url)
			}
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q, want application/json", ct)
			}
			if body["error"] == "" || body["error"] == nil {
				t.Fatalf("body = %s, want JSON error", w.Body.String())
			}
		})
	}

	// The empty-path rejection must never redirect or write records.
	h2, s := newTestHandler(t)
	wZero, _ := postJSON(t, h2, "/v1/documents//changes", map[string]any{
		"deviceId": "d",
		"changes":  []any{map[string]any{"id": "a", "payload": map[string]any{"x": 1}}},
	})
	if wZero.Code != http.StatusBadRequest {
		t.Fatalf("empty post status = %d", wZero.Code)
	}
	if rows, _, err := s.ListChanges("", 0, 10); err != nil || len(rows) != 0 {
		t.Fatalf("empty-doc request wrote rows: %+v err=%v", rows, err)
	}

	// A normal non-empty path still works and is untouched by the guard.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/real/changes")
	if w.Code != http.StatusOK {
		t.Fatalf("non-empty GET status = %d", w.Code)
	}
}

func mergeBody(t *testing.T, h http.Handler, doc string, raw string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+doc+"/merge", raw)
}

func TestMergeAppliedAndMerged(t *testing.T) {
	h, _ := newTestHandler(t)

	// New document, base 0: applied.
	w, body := mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":0,"change":{"id":"c1","payload":{"a":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("apply status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 || body["id"] != "c1" {
		t.Fatalf("apply body = %v", body)
	}

	// Seed an intervening change through the original batch API.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"b": 2}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	// Laggard at base 1, disjoint top-level field: merged at cursor 3.
	w, body = mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":1,"change":{"id":"c3","payload":{"c":3}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("merge status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "merged" || body["cursor"].(float64) != 3 {
		t.Fatalf("merged body = %v", body)
	}

	// At head (base 3): applied.
	w, body = mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":3,"change":{"id":"c4","payload":{"d":4}}}`)
	if w.Code != 200 || body["outcome"] != "applied" || body["cursor"].(float64) != 4 {
		t.Fatalf("head apply = %d %v", w.Code, body)
	}
}

func TestMergeIdempotentResponse(t *testing.T) {
	h, _ := newTestHandler(t)
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	}); w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	w, body := mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":0,"change":{"id":"x","payload":{"k":"v"}}}`)
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("body = %v", body)
	}
	result, ok := body["result"].(map[string]any)
	if !ok || result["id"] != "x" || result["created"] != false || result["cursor"].(float64) != 1 {
		t.Fatalf("result = %v", body["result"])
	}
}

func TestMergeRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.PostChanges("doc", []store.Change{
		{ID: "seed", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name       string
		doc        string
		contentTyp string
		body       string
		want       int
	}{
		{"wrong content type", "doc", "text/plain", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"malformed json", "doc", "application/json", `{bad`, http.StatusBadRequest},
		{"trailing content", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":{}}}x`, http.StatusBadRequest},
		{"missing deviceId", "doc", "application/json", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"empty deviceId", "doc", "application/json", `{"deviceId":"","baseCursor":0,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"numeric deviceId", "doc", "application/json", `{"deviceId":7,"baseCursor":0,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"missing baseCursor", "doc", "application/json", `{"deviceId":"d","change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"null baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":null,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"negative baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":-1,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"fraction baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":1.5,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"exponent baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":1e3,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"oversize baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":999999999999999999999999,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"string baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":"0","change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"boolean baseCursor", "doc", "application/json", `{"deviceId":"d","baseCursor":true,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"missing change", "doc", "application/json", `{"deviceId":"d","baseCursor":0}`, http.StatusBadRequest},
		{"null change", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":null}`, http.StatusBadRequest},
		{"missing change id", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"payload":{}}}`, http.StatusBadRequest},
		{"empty change id", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"","payload":{}}}`, http.StatusBadRequest},
		{"numeric change id", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":1,"payload":{}}}`, http.StatusBadRequest},
		{"missing payload", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a"}}`, http.StatusBadRequest},
		{"array payload", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":[1]}}`, http.StatusBadRequest},
		{"scalar payload", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":1}}`, http.StatusBadRequest},
		{"null payload", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":null}}`, http.StatusBadRequest},
		{"unknown doc nonzero base", "ghost", "application/json", `{"deviceId":"d","baseCursor":1,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"base beyond current", "doc", "application/json", `{"deviceId":"d","baseCursor":99,"change":{"id":"a","payload":{}}}`, http.StatusBadRequest},
		{"field collision", "doc", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":{"a":2}}}`, http.StatusConflict},
		{"existing id mismatch", "doc", "application/json", `{"deviceId":"d","baseCursor":1,"change":{"id":"seed","payload":{"a":2}}}`, http.StatusConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/documents/"+tc.doc+"/merge", strings.NewReader(tc.body))
			if tc.contentTyp != "" {
				r.Header.Set("Content-Type", tc.contentTyp)
			}
			wRec := httptest.NewRecorder()
			h.ServeHTTP(wRec, r)
			if wRec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body = %s", wRec.Code, tc.want, wRec.Body.String())
			}
			if ct := wRec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			var errBody map[string]string
			if err := json.Unmarshal(wRec.Body.Bytes(), &errBody); err != nil || errBody["error"] == "" {
				t.Fatalf("error body = %s", wRec.Body.String())
			}
		})
	}
}

func TestMergeConflictZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)
	if _, err := s.PostChanges("doc", []store.Change{
		{ID: "seed", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	w, _ := mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":0,"change":{"id":"new","payload":{"a":2}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	rows, _, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "seed" {
		t.Fatalf("conflict merge wrote rows: %+v", rows)
	}
}

func TestMergeEmptyObjectPayloadAccepted(t *testing.T) {
	h, _ := newTestHandler(t)
	// {} is an object and collides with no keys, so a laggard with an empty
	// object merges even behind non-object-free history (there is no history
	// here, but confirm acceptance at a lagging base).
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "p", "payload": map[string]any{"x": 1}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	w, body := mergeBody(t, h, "doc", `{"deviceId":"dev","baseCursor":0,"change":{"id":"e","payload":{}}}`)
	if w.Code != 200 || body["outcome"] != "merged" || body["cursor"].(float64) != 2 {
		t.Fatalf("empty-object merge = %d %v %s", w.Code, body, w.Body.String())
	}
}

func TestMergeCharsetContentType(t *testing.T) {
	h, _ := newTestHandler(t)
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/merge",
		strings.NewReader(`{"deviceId":"d","baseCursor":0,"change":{"id":"a","payload":{}}}`))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestConcurrentMergesHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	// Seed one change so every merge is a laggard at base 0.
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "seed", "payload": map[string]any{"seed": 0}}},
	}); w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	const n = 30
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"deviceId":"dev","baseCursor":0,"change":{"id":"m%02d","payload":{"f%d":%d}}}`, i, i, i)
			w, _ := postJSON(t, h, "/v1/documents/doc/merge", body)
			if w.Code != 200 {
				errs <- fmt.Errorf("merge %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if got := len(body["changes"].([]any)); got != n+1 {
		t.Fatalf("stored %d changes, want %d", got, n+1)
	}
	if body["nextCursor"].(float64) != n+1 {
		t.Fatalf("nextCursor = %v, want %d", body["nextCursor"], n+1)
	}
}

func TestGetPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	for i := 0; i < 5; i++ {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
			"deviceId": "dev",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("id%d", i), "payload": map[string]any{"i": i}}},
		})
		if w.Code != 200 {
			t.Fatal(w.Body.String())
		}
	}

	// Defaults: no params -> up to 100, ascending.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	list := body["changes"].([]any)
	if len(list) != 5 || body["nextCursor"].(float64) != 5 {
		t.Fatalf("default list = %v next=%v", list, body["nextCursor"])
	}
	first := list[0].(map[string]any)
	if first["id"] != "id0" || first["deviceId"] != "dev" || first["cursor"].(float64) != 1 {
		t.Fatalf("first row = %v", first)
	}
	if first["payload"].(map[string]any)["i"].(float64) != 0 {
		t.Fatalf("payload = %v", first["payload"])
	}

	// Page with after/limit.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=2&limit=2")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	list = body["changes"].([]any)
	if len(list) != 2 {
		t.Fatalf("page len = %d", len(list))
	}
	if list[0].(map[string]any)["id"] != "id2" || list[1].(map[string]any)["id"] != "id3" {
		t.Fatalf("page ids = %v", list)
	}
	if body["nextCursor"].(float64) != 4 {
		t.Fatalf("nextCursor = %v", body["nextCursor"])
	}

	// after at high-water mark: empty, nextCursor == after.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=5")
	if w.Code != 200 || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 5 {
		t.Fatalf("tail: code=%d body=%s", w.Code, w.Body.String())
	}

	// Unknown document: empty list, cursor 0.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/ghost/changes?after=50")
	if w.Code != 200 || len(body["changes"].([]any)) != 0 || body["nextCursor"].(float64) != 0 {
		t.Fatalf("unknown: code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestGetRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	bad := []string{
		"/v1/documents/doc/changes?after=-1",
		"/v1/documents/doc/changes?after=x",
		"/v1/documents/doc/changes?after=1.5",
		"/v1/documents/doc/changes?limit=0",
		"/v1/documents/doc/changes?limit=1001",
		"/v1/documents/doc/changes?limit=abc",
		"/v1/documents/doc/changes?after=1&limit=-5",
	}
	for _, url := range bad {
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", url, w.Code)
		}
		if body["error"] == nil {
			t.Fatalf("%s body = %s", url, w.Body.String())
		}
	}
}

func TestContentCharsetAccepted(t *testing.T) {
	h, _ := newTestHandler(t)
	// application/json with charset parameter is still application/json.
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/changes",
		strings.NewReader(`{"deviceId":"d","changes":[{"id":"a","payload":1}]}`))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
}

func TestConcurrentPostsHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
				"deviceId": "dev",
				"changes":  []any{map[string]any{"id": fmt.Sprintf("id-%02d", i), "payload": map[string]any{"i": i}}},
			})
			if w.Code != 200 {
				errs <- fmt.Errorf("post %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if got := len(body["changes"].([]any)); got != n {
		t.Fatalf("stored %d changes, want %d", got, n)
	}
	if body["nextCursor"].(float64) != n {
		t.Fatalf("nextCursor = %v, want %d", body["nextCursor"], n)
	}
}
