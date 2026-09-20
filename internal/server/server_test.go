package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func doRequest(t *testing.T, h http.Handler, method, path, contentType, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, path, nil)
	} else {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)
	return recorder
}

func postChanges(t *testing.T, h http.Handler, docID, body string) *httptest.ResponseRecorder {
	t.Helper()
	return doRequest(t, h, http.MethodPost, "/v1/documents/"+docID+"/changes", "application/json", body)
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), v); err != nil {
		t.Fatalf("decode response: %v (body %q)", err, recorder.Body.String())
	}
}

func TestHealth(t *testing.T) {
	recorder := doRequest(t, Handler(), http.MethodGet, "/healthz", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
}

func TestPostValidation(t *testing.T) {
	h := Handler()
	valid := `{"deviceId":"dev-1","changes":[{"id":"c1","payload":{"x":1}}]}`

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", valid},
		{"missing content type", "", valid},
		{"malformed json", "application/json", `{"deviceId":`},
		{"trailing data", "application/json", valid + ` {}`},
		{"empty deviceId", "application/json", `{"deviceId":"","changes":[{"id":"c1","payload":1}]}`},
		{"missing deviceId", "application/json", `{"changes":[{"id":"c1","payload":1}]}`},
		{"deviceId not a string", "application/json", `{"deviceId":42,"changes":[{"id":"c1","payload":1}]}`},
		{"empty changes", "application/json", `{"deviceId":"dev-1","changes":[]}`},
		{"missing changes", "application/json", `{"deviceId":"dev-1"}`},
		{"empty change id", "application/json", `{"deviceId":"dev-1","changes":[{"id":"","payload":1}]}`},
		{"missing payload", "application/json", `{"deviceId":"dev-1","changes":[{"id":"c1"}]}`},
		{"duplicate id in batch", "application/json", `{"deviceId":"dev-1","changes":[{"id":"c1","payload":1},{"id":"c1","payload":1}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := doRequest(t, h, http.MethodPost, "/v1/documents/doc-bad/changes", tc.contentType, tc.body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body %q)", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != "application/json" {
				t.Fatalf("content type = %q, want application/json", got)
			}
			var errBody map[string]string
			decodeBody(t, recorder, &errBody)
			if errBody["error"] == "" {
				t.Fatalf("expected error message, got %q", recorder.Body.String())
			}
		})
	}

	// A rejected batch must write nothing.
	recorder := doRequest(t, h, http.MethodGet, "/v1/documents/doc-bad/changes", "", "")
	var resp getResponse
	decodeBody(t, recorder, &resp)
	if len(resp.Changes) != 0 || resp.NextCursor != 0 {
		t.Fatalf("rejected batches leaked writes: %+v", resp)
	}
}

func TestPostAcceptsJSONContentTypeWithParams(t *testing.T) {
	h := Handler()
	recorder := doRequest(t, h, http.MethodPost, "/v1/documents/doc-ct/changes",
		"application/json; charset=utf-8", `{"deviceId":"dev-1","changes":[{"id":"c1","payload":null}]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", recorder.Code, recorder.Body.String())
	}
}

func TestPostCreateIdempotentConflict(t *testing.T) {
	h := Handler()

	recorder := postChanges(t, h, "doc-1", `{"deviceId":"dev-1","changes":[
		{"id":"a","payload":{"v":1}},
		{"id":"b","payload":[1,2]},
		{"id":"c","payload":"x"}
	]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", recorder.Code, recorder.Body.String())
	}
	var posted postResponse
	decodeBody(t, recorder, &posted)
	want := []ApplyResult{
		{ID: "a", Created: true, Cursor: 1},
		{ID: "b", Created: true, Cursor: 2},
		{ID: "c", Created: true, Cursor: 3},
	}
	if fmt.Sprintf("%+v", posted.Results) != fmt.Sprintf("%+v", want) {
		t.Fatalf("results = %+v, want %+v", posted.Results, want)
	}

	// Idempotent replay: same deviceId and semantically equal payload,
	// mixed with a new id. Hits keep their first cursor.
	recorder = postChanges(t, h, "doc-1", `{"deviceId":"dev-1","changes":[
		{"id":"a","payload":{"v":1.0}},
		{"id":"d","payload":true}
	]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", recorder.Code, recorder.Body.String())
	}
	decodeBody(t, recorder, &posted)
	want = []ApplyResult{
		{ID: "a", Created: false, Cursor: 1},
		{ID: "d", Created: true, Cursor: 4},
	}
	if fmt.Sprintf("%+v", posted.Results) != fmt.Sprintf("%+v", want) {
		t.Fatalf("results = %+v, want %+v", posted.Results, want)
	}

	// Same id, different payload -> 409 and zero writes.
	recorder = postChanges(t, h, "doc-1", `{"deviceId":"dev-1","changes":[
		{"id":"e","payload":1},
		{"id":"a","payload":{"v":2}}
	]}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %q)", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q, want application/json", got)
	}

	// Same id, different deviceId -> 409 and zero writes.
	recorder = postChanges(t, h, "doc-1", `{"deviceId":"dev-2","changes":[{"id":"a","payload":{"v":1}}]}`)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409 (body %q)", recorder.Code, recorder.Body.String())
	}

	// Neither conflicting batch wrote anything: "e" must not exist.
	recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-1/changes", "", "")
	var listed getResponse
	decodeBody(t, recorder, &listed)
	if len(listed.Changes) != 4 || listed.NextCursor != 4 {
		t.Fatalf("changes = %+v, nextCursor = %d; want 4 changes and cursor 4", listed.Changes, listed.NextCursor)
	}
}

func TestGetPaginationAndNextCursor(t *testing.T) {
	h := Handler()
	postChanges(t, h, "doc-p", `{"deviceId":"dev-1","changes":[
		{"id":"c1","payload":1},{"id":"c2","payload":2},{"id":"c3","payload":3}
	]}`)

	// Unknown document: empty list, nextCursor 0.
	recorder := doRequest(t, h, http.MethodGet, "/v1/documents/unknown/changes", "", "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var resp getResponse
	decodeBody(t, recorder, &resp)
	if resp.Changes == nil || len(resp.Changes) != 0 || resp.NextCursor != 0 {
		t.Fatalf("unknown doc: %+v, want empty list and nextCursor 0", resp)
	}

	// Default after/limit.
	recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-p/changes", "", "")
	decodeBody(t, recorder, &resp)
	if len(resp.Changes) != 3 || resp.NextCursor != 3 {
		t.Fatalf("default listing: %+v", resp)
	}
	for i, c := range resp.Changes {
		if c.Cursor != int64(i+1) || c.ID != fmt.Sprintf("c%d", i+1) || c.DeviceID != "dev-1" {
			t.Fatalf("change %d = %+v", i, c)
		}
	}

	// after + limit walk.
	recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-p/changes?after=1&limit=1", "", "")
	decodeBody(t, recorder, &resp)
	if len(resp.Changes) != 1 || resp.Changes[0].ID != "c2" || resp.NextCursor != 2 {
		t.Fatalf("page after=1 limit=1: %+v", resp)
	}

	// Known document, no results: nextCursor = after.
	recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-p/changes?after=3", "", "")
	decodeBody(t, recorder, &resp)
	if resp.Changes == nil || len(resp.Changes) != 0 || resp.NextCursor != 3 {
		t.Fatalf("empty page: %+v, want empty list and nextCursor 3", resp)
	}

	// Invalid params.
	for _, q := range []string{"after=-1", "after=abc", "after=1.5", "limit=0", "limit=1001", "limit=-2", "limit=x"} {
		recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-p/changes?"+q, "", "")
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status = %d, want 400", q, recorder.Code)
		}
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Fatalf("query %q: content type = %q, want application/json", q, got)
		}
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store)
	recorder := postChanges(t, h, "doc-r", `{"deviceId":"dev-1","changes":[
		{"id":"a","payload":{"n":1}},{"id":"b","payload":"two"}
	]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d (body %q)", recorder.Code, recorder.Body.String())
	}

	// Simulate a restart: a fresh store over the same file.
	reopened, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	h = NewHandler(reopened)

	recorder = doRequest(t, h, http.MethodGet, "/v1/documents/doc-r/changes", "", "")
	var resp getResponse
	decodeBody(t, recorder, &resp)
	if len(resp.Changes) != 2 || resp.NextCursor != 2 {
		t.Fatalf("after restart: %+v", resp)
	}
	if resp.Changes[0].ID != "a" || resp.Changes[1].ID != "b" {
		t.Fatalf("after restart: %+v", resp.Changes)
	}

	// Cursor continues where it left off; idempotency survives the restart.
	recorder = postChanges(t, h, "doc-r", `{"deviceId":"dev-1","changes":[
		{"id":"a","payload":{"n":1}},{"id":"c","payload":3}
	]}`)
	var posted postResponse
	decodeBody(t, recorder, &posted)
	want := []ApplyResult{
		{ID: "a", Created: false, Cursor: 1},
		{ID: "c", Created: true, Cursor: 3},
	}
	if fmt.Sprintf("%+v", posted.Results) != fmt.Sprintf("%+v", want) {
		t.Fatalf("results = %+v, want %+v", posted.Results, want)
	}
}

func TestConcurrentBatchesAtomicAndUniqueCursors(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store)

	const writers = 8
	const perWriter = 10
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				body := fmt.Sprintf(`{"deviceId":"dev-%d","changes":[{"id":"w%d-c%d","payload":%d}]}`,
					w, w, i, i)
				recorder := postChanges(t, h, "doc-conc", body)
				if recorder.Code != http.StatusOK {
					t.Errorf("status = %d (body %q)", recorder.Code, recorder.Body.String())
				}
			}
		}(w)
	}
	wg.Wait()

	recorder := doRequest(t, h, http.MethodGet, "/v1/documents/doc-conc/changes?limit=1000", "", "")
	var resp getResponse
	decodeBody(t, recorder, &resp)
	total := writers * perWriter
	if len(resp.Changes) != total {
		t.Fatalf("got %d changes, want %d", len(resp.Changes), total)
	}
	seen := map[int64]bool{}
	for i, c := range resp.Changes {
		if seen[c.Cursor] {
			t.Fatalf("duplicate cursor %d", c.Cursor)
		}
		seen[c.Cursor] = true
		if c.Cursor != int64(i+1) {
			t.Fatalf("changes not ordered by cursor at %d: %+v", i, c)
		}
	}
	if resp.NextCursor != int64(total) {
		t.Fatalf("nextCursor = %d, want %d", resp.NextCursor, total)
	}
}
