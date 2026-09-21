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

func TestEmptyDocumentIDReturnsJSON400(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/documents//changes", ""},
		{http.MethodPost, "/v1/documents//changes", `{"deviceId":"d","changes":[{"id":"c","payload":1}]}`},
		{http.MethodPost, "/v1/documents//merge", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":{}}}`},
	}
	for _, tc := range cases {
		var r *http.Request
		if tc.body != "" {
			r = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
		} else {
			r = httptest.NewRequest(tc.method, tc.path, nil)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s content-type = %q, want application/json", tc.method, tc.path, ct)
		}
		var b map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
			t.Fatalf("%s %s body = %q, want JSON error", tc.method, tc.path, w.Body.String())
		}
	}
}

func mergeBody(t *testing.T, h http.Handler, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, "/v1/documents/"+doc+"/merge", body)
}

func TestMergeHTTPValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	const url = "/v1/documents/doc/merge"

	bad := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":{}}}x`},
		{"missing deviceId", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"empty deviceId", "application/json", `{"deviceId":"","baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"missing baseCursor", "application/json", `{"deviceId":"d","change":{"id":"c","payload":{}}}`},
		{"null baseCursor", "application/json", `{"deviceId":"d","baseCursor":null,"change":{"id":"c","payload":{}}}`},
		{"negative baseCursor", "application/json", `{"deviceId":"d","baseCursor":-1,"change":{"id":"c","payload":{}}}`},
		{"float baseCursor", "application/json", `{"deviceId":"d","baseCursor":1.5,"change":{"id":"c","payload":{}}}`},
		{"string baseCursor", "application/json", `{"deviceId":"d","baseCursor":"0","change":{"id":"c","payload":{}}}`},
		{"boolean baseCursor", "application/json", `{"deviceId":"d","baseCursor":true,"change":{"id":"c","payload":{}}}`},
		{"missing change", "application/json", `{"deviceId":"d","baseCursor":0}`},
		{"empty change id", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"","payload":{}}}`},
		{"numeric change id", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":7,"payload":{}}}`},
		{"missing payload", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"c"}}`},
		{"array payload", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":[1]}}`},
		{"scalar payload", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":5}}`},
		{"null payload", "application/json", `{"deviceId":"d","baseCursor":0,"change":{"id":"c","payload":null}}`},
	}
	for _, tc := range bad {
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
			var b map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
				t.Fatalf("body = %q", w.Body.String())
			}
		})
	}

	// baseCursor for unknown doc must be 0; non-zero -> 400.
	w, _ := mergeBody(t, h, "ghost", map[string]any{
		"deviceId": "d", "baseCursor": 1,
		"change": map[string]any{"id": "c", "payload": map[string]any{}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown doc non-zero base = %d body=%s", w.Code, w.Body.String())
	}
}

func TestMergeHTTPOutcomes(t *testing.T) {
	h, s := newTestHandler(t)

	// First change on a new doc, baseCursor 0 -> applied cursor 1.
	w, body := mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 0,
		"change": map[string]any{"id": "c1", "payload": map[string]any{"a": 1}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("applied = %v", body)
	}

	// baseCursor ahead -> 400.
	w, _ = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 9,
		"change": map[string]any{"id": "cX", "payload": map[string]any{"z": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ahead base = %d", w.Code)
	}

	// Same id repost -> idempotent with embedded result.
	w, body = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 1,
		"change": map[string]any{"id": "c1", "payload": map[string]any{"a": 1}},
	})
	if w.Code != 200 || body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("idempotent = %d %v", w.Code, body)
	}
	res := body["result"].(map[string]any)
	if res["id"] != "c1" || res["created"] != false || res["cursor"].(float64) != 1 {
		t.Fatalf("embedded result = %v", res)
	}

	// Same id, different payload -> 409.
	w, _ = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 1,
		"change": map[string]any{"id": "c1", "payload": map[string]any{"a": 2}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict = %d body=%s", w.Code, w.Body.String())
	}

	// Seed a second object {"b":2} at cursor 2.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"b": 2}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	// Stale client at baseCursor 1 posts {"c":3}: disjoint from later {"b":2} -> merged cursor 3.
	w, body = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 1,
		"change": map[string]any{"id": "c3", "payload": map[string]any{"c": 3}},
	})
	if w.Code != 200 || body["outcome"] != "merged" || body["cursor"].(float64) != 3 {
		t.Fatalf("merged = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Stale client posts a payload colliding with later key "b" -> 409 zero write.
	before, _, _ := s.ListChanges("doc", 0, 100)
	w, _ = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 0,
		"change": map[string]any{"id": "c4", "payload": map[string]any{"b": 99}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("clash = %d body=%s", w.Code, w.Body.String())
	}
	after, _, _ := s.ListChanges("doc", 0, 100)
	if len(after) != len(before) {
		t.Fatalf("conflict changed row count: before=%d after=%d", len(before), len(after))
	}
}

func TestMergeHTTPZeroWriteOnBadBase(t *testing.T) {
	h, s := newTestHandler(t)
	// Seed one change.
	w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "seed", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	// A rejected merge (baseCursor ahead) must not write.
	w, _ = mergeBody(t, h, "doc", map[string]any{
		"deviceId": "dev", "baseCursor": 5,
		"change": map[string]any{"id": "ghost-change", "payload": map[string]any{"x": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", w.Code)
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || rows[0].ID != "seed" || next != 1 {
		t.Fatalf("zero-write violated: %+v next=%d", rows, next)
	}
}

func seedDocOverHTTP(t *testing.T, h http.Handler, doc string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
			"deviceId": "dev",
			"changes": []any{
				map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}},
			},
		})
		if w.Code != 200 {
			t.Fatalf("seed %s: %s", doc, w.Body.String())
		}
	}
}

func TestPostSnapshotHTTPCreateRetryConflict(t *testing.T) {
	h, s := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 2)

	url := "/v1/documents/doc/snapshots"
	w, body := postJSON(t, h, url, map[string]any{"cursor": 2, "state": map[string]any{"title": "a", "v": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 2 || body["created"] != true {
		t.Fatalf("create body = %v", body)
	}

	// Retry with decoded-equal state: 200 created=false.
	w, body = postJSON(t, h, url, `{"cursor":2,"state":{"v":1.0,"title":"a"}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("retry status = %d body = %s", w.Code, w.Body.String())
	}
	if body["created"] != false || body["cursor"].(float64) != 2 {
		t.Fatalf("retry body = %v", body)
	}

	// Conflicting state: 409, snapshot unchanged.
	w, body = postJSON(t, h, url, map[string]any{"cursor": 2, "state": map[string]any{"title": "b"}})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("conflict body = %s", w.Body.String())
	}
	snap, err := s.GetSnapshot("doc", 2)
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]any
	if err := json.Unmarshal(snap.State, &state); err != nil || state["title"] != "a" {
		t.Fatalf("conflict mutated snapshot: %s", snap.State)
	}
}

func TestPostSnapshotHTTPRejectsBadInput(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 6)
	const url = "/v1/documents/doc/snapshots"

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"cursor":1,"state":{}}`},
		{"missing content type", "", `{"cursor":1,"state":{}}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"cursor":1,"state":{}}x`},
		{"missing cursor", "application/json", `{"state":{}}`},
		{"null cursor", "application/json", `{"cursor":null,"state":{}}`},
		{"negative cursor", "application/json", `{"cursor":-1,"state":{}}`},
		{"float cursor", "application/json", `{"cursor":1.5,"state":{}}`},
		{"string cursor", "application/json", `{"cursor":"1","state":{}}`},
		{"boolean cursor", "application/json", `{"cursor":true,"state":{}}`},
		{"missing state", "application/json", `{"cursor":1}`},
		{"malformed state", "application/json", `{"cursor":1,"state":{`},
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
			var b map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil || b["error"] == "" {
				t.Fatalf("body = %q", w.Body.String())
			}
		})
	}

	// state accepts any legal JSON value, including false / 0 / "" / null.
	// Use a distinct cursor for each so no uniqueness/conflict rule kicks in.
	legal := []string{"false", "0", `""`, "null", "[1,true,null]", `"str"`}
	for i, state := range legal {
		w, body := postJSON(t, h, url, fmt.Sprintf(`{"cursor":%d,"state":%s}`, i+1, state))
		if w.Code != http.StatusOK {
			t.Fatalf("state case %d (%s): status=%d body=%s", i, state, w.Code, w.Body.String())
		}
		if body["created"] != true {
			t.Fatalf("state case %d: %v", i, body)
		}
	}
}

func TestPostSnapshotHTTPUnknownCursorZeroWrite(t *testing.T) {
	h, s := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 2)
	const url = "/v1/documents/doc/snapshots"

	// Cursor 0, ahead cursor and unknown document are all 400 with zero write.
	for _, tc := range []struct {
		url  string
		body string
	}{
		{url, `{"cursor":0,"state":{}}`},
		{url, `{"cursor":3,"state":{}}`},
		{"/v1/documents/ghost/snapshots", `{"cursor":1,"state":{}}`},
	} {
		r := httptest.NewRequest(http.MethodPost, tc.url, strings.NewReader(tc.body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s -> status = %d, want 400, body = %s", tc.url, w.Code, w.Body.String())
		}
	}
	if _, err := s.GetSnapshot("doc", 1); err != store.ErrSnapshotNotFound {
		t.Fatalf("snapshots leaked after rejected writes: %v", err)
	}
}

func TestGetSnapshotHTTP(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 2)

	// Seed snapshots at cursors 1 and 2 with varied JSON values.
	for _, tc := range []struct {
		cursor int
		state  string
	}{
		{1, `{"k":"v"}`},
		{2, `[1,{"a":2},null,true]`},
	} {
		w, _ := postJSON(t, h, "/v1/documents/doc/snapshots",
			fmt.Sprintf(`{"cursor":%d,"state":%s}`, tc.cursor, tc.state))
		if w.Code != 200 {
			t.Fatalf("seed %d: %s", tc.cursor, w.Body.String())
		}
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1")
	if w.Code != http.StatusOK {
		t.Fatalf("get status = %d body = %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 1 {
		t.Fatalf("get cursor = %v", body["cursor"])
	}
	state := body["state"].(map[string]any)
	if state["k"] != "v" {
		t.Fatalf("get state = %v", body["state"])
	}

	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/2")
	if w.Code != 200 {
		t.Fatalf("get 2 status = %d body = %s", w.Code, w.Body.String())
	}
	arr := body["state"].([]any)
	if len(arr) != 4 || arr[0].(float64) != 1 || arr[3] != true {
		t.Fatalf("get 2 state = %v", body["state"])
	}
}

func TestGetSnapshotHTTPNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"x": 1}})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}

	for _, url := range []string{
		"/v1/documents/doc/snapshots/2",   // known doc/cursor, no snapshot
		"/v1/documents/doc/snapshots/9",   // cursor without a change
		"/v1/documents/ghost/snapshots/1", // unknown document
		"/v1/documents/doc/snapshots/0",   // cursor 0 never has a snapshot
	} {
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404, body = %s", url, w.Code, w.Body.String())
		}
		if body["error"] == nil {
			t.Fatalf("%s body = %s", url, w.Body.String())
		}
	}
}

func TestGetSnapshotHTTPBadCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, url := range []string{
		"/v1/documents/doc/snapshots/abc",
		"/v1/documents/doc/snapshots/-1",
		"/v1/documents/doc/snapshots/1.5",
		"/v1/documents/doc/snapshots/",
	} {
		w, body := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %s", url, w.Code, w.Body.String())
		}
		if body["error"] == nil {
			t.Fatalf("%s body = %s", url, w.Body.String())
		}
	}
}

func TestSnapshotsHTTPEmptyDocumentID(t *testing.T) {
	h, _ := newTestHandler(t)

	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/documents//snapshots", `{"cursor":1,"state":{}}`},
		{http.MethodGet, "/v1/documents//snapshots/1", ""},
		{http.MethodGet, "/v1/documents/doc/snapshots/", ""},
	}
	for _, tc := range cases {
		var r *http.Request
		if tc.body != "" {
			r = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
		} else {
			r = httptest.NewRequest(tc.method, tc.path, nil)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s status = %d, want 400", tc.method, tc.path, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s content-type = %q", tc.method, tc.path, ct)
		}
	}
}

func TestSnapshotHTTPPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.db")

	s, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedDocOverHTTP(t, h, "doc", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"hello": "world"}})
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
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	w, body := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/snapshots/2")
	if w.Code != 200 {
		t.Fatalf("after reopen status = %d body = %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 2 || body["state"].(map[string]any)["hello"] != "world" {
		t.Fatalf("after reopen body = %v", body)
	}

	// Same-state retry stays idempotent after restart; different state 409s.
	w, body = postJSON(t, h2, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"hello": "world"}})
	if w.Code != 200 || body["created"] != false {
		t.Fatalf("retry after restart: %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h2, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"hello": "changed"}})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart status = %d", w.Code)
	}
}

func TestSnapshotHTTPConcurrentSameCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	seedDocOverHTTP(t, h, "doc", 1)

	const n = 30
	var wg sync.WaitGroup
	type outcome struct {
		status  int
		created bool
	}
	outcomes := make(chan outcome, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every goroutine posts a distinct state, so exactly one must win
			// with created=true; every other must see 409.
			w, body := postJSON(t, h, "/v1/documents/doc/snapshots",
				fmt.Sprintf(`{"cursor":1,"state":{"i":%d}}`, i))
			oc := outcome{status: w.Code}
			if v, ok := body["created"].(bool); ok {
				oc.created = v
			}
			outcomes <- oc
		}(i)
	}
	wg.Wait()
	close(outcomes)

	var winners, conflicts, bad int
	for oc := range outcomes {
		switch {
		case oc.status == http.StatusOK && oc.created:
			winners++
		case oc.status == http.StatusConflict:
			conflicts++
		default:
			bad++
		}
	}
	if winners != 1 || conflicts != n-1 || bad != 0 {
		t.Fatalf("winners=%d conflicts=%d bad=%d, want 1/%d/0", winners, conflicts, bad, n-1)
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1")
	if w.Code != 200 || body["cursor"].(float64) != 1 || body["state"] == nil {
		t.Fatalf("snapshot after race = %d %v", w.Code, body)
	}
}
