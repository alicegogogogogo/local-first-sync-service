package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// changeStatus sends the raw GET to the status summary and returns the
// recorder so tests can assert the exact single-line body.
func changeStatus(h http.Handler, doc, device string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/v1/documents/"+doc+"/changes/status?deviceId="+device, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// An unknown document succeeds with all four numbers zero: the empty summary
// is a normal answer, creates no change or snapshot, and renders as one
// compact line with the fixed changeCount,boundary,maxCursor,compacted order.
func TestChangeStatusHTTPUnknownDocumentZeros(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	w := changeStatus(h, "never-heard-of-it", "dev")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"changeCount":0,"boundary":0,"maxCursor":0,"compacted":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q want %q", w.Body.String(), want)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
}

// The four numbers follow the persisted data through commit, snapshot,
// compaction and a later append.
func TestChangeStatusHTTPLifecycle(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)

	// After four commits: 4 online, no boundary, max cursor 4, nothing cut.
	w := changeStatus(h, "doc", "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"changeCount":4,"boundary":0,"maxCursor":4,"compacted":0}`+"\n" {
		t.Fatalf("after commits = %q", got)
	}

	// Snapshot at cursor 2: boundary 2 without moving any row.
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = changeStatus(h, "doc", "dev-1")
	if got := w.Body.String(); got != `{"changeCount":4,"boundary":2,"maxCursor":4,"compacted":0}`+"\n" {
		t.Fatalf("after snapshot = %q", got)
	}

	// Compaction moves cursors 1..2 out: 2 online rows (cursors 3 and 4),
	// boundary 2, max online cursor 4, 2 cut.
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = changeStatus(h, "doc", "dev-1")
	if got := w.Body.String(); got != `{"changeCount":2,"boundary":2,"maxCursor":4,"compacted":2}`+"\n" {
		t.Fatalf("after compaction = %q", got)
	}

	// A genuinely new commit grows only the online side and advances maxCursor.
	postRawChanges(t, h, "doc", "dev-1", `[{"id":"c5","payload":{"n":5}}]`)
	w = changeStatus(h, "doc", "dev-1")
	if got := w.Body.String(); got != `{"changeCount":3,"boundary":2,"maxCursor":5,"compacted":2}`+"\n" {
		t.Fatalf("after append = %q", got)
	}

	// A repeat compaction trims nothing and changes none of the numbers.
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = changeStatus(h, "doc", "dev-1")
	if got := w.Body.String(); got != `{"changeCount":3,"boundary":2,"maxCursor":5,"compacted":2}`+"\n" {
		t.Fatalf("after repeat compact = %q", got)
	}
}

// A document whose online log was fully trimmed reports zero online rows and
// maxCursor 0, while boundary and compacted survive.
func TestChangeStatusHTTPFullyCompacted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc", "dev", 2)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w := changeStatus(h, "doc", "dev")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"changeCount":0,"boundary":2,"maxCursor":0,"compacted":2}`+"\n" {
		t.Fatalf("fully compacted = %q", got)
	}
}

// Compacting with no snapshot removes nothing, and hard-deleting the whole
// document returns the summary to four zeros.
func TestChangeStatusHTTPDeleteReturnsToZeros(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc", "dev", 2)

	// Compacting with no snapshot removes nothing; the summary stays online.
	if w := compactChanges(t, h, "doc", "dev"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w := changeStatus(h, "doc", "dev")
	if got := w.Body.String(); got != `{"changeCount":2,"boundary":0,"maxCursor":2,"compacted":0}`+"\n" {
		t.Fatalf("snapshot-less compact = %q", got)
	}

	// Hard-deleting the document returns every number to zero.
	r := httptest.NewRequest(http.MethodDelete, "/v1/documents/doc?deviceId=dev", nil)
	dw := httptest.NewRecorder()
	h.ServeHTTP(dw, r)
	if dw.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", dw.Code, dw.Body.String())
	}
	w = changeStatus(h, "doc", "dev")
	if w.Code != http.StatusOK {
		t.Fatalf("after delete status = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"changeCount":0,"boundary":0,"maxCursor":0,"compacted":0}`+"\n" {
		t.Fatalf("after delete = %q", got)
	}
}

// A parked long poll is not woken by status reads and the reads allocate no
// cursor.
func TestChangeStatusHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc", "dev", 1)

	done := make(chan struct {
		changes  int
		timedOut bool
		err      error
	}, 1)
	go func() {
		changes, _, timedOut, err := s.WaitForChanges(t.Context(), "doc", 1, 100, 250*time.Millisecond)
		done <- struct {
			changes  int
			timedOut bool
			err      error
		}{len(changes), timedOut, err}
	}()

	time.Sleep(30 * time.Millisecond) // let the poll park
	for i := 0; i < 3; i++ {
		if w := changeStatus(h, "doc", "dev"); w.Code != http.StatusOK {
			t.Fatalf("status read %d = %d %s", i, w.Code, w.Body.String())
		}
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("poll: %v", got.err)
	}
	if !got.timedOut || got.changes != 0 {
		t.Fatalf("poll woken by a read-only status call: %+v", got)
	}

	// The status reads consumed no cursor: the next genuinely new commit gets
	// cursor 2.
	postRawChanges(t, h, "doc", "dev", `[{"id":"c2","payload":{"n":2}}]`)
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 || rows[1].Cursor != 2 {
		t.Fatalf("status reads moved the cursor: rows=%+v next=%d", rows, next)
	}
}

// Device existence precedes success: a missing, empty or unregistered
// deviceId is 404; a revoked device is 403. Neither exposes any statistic.
func TestChangeStatusHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc", "dev", 1)

	get := func(rawURL string) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, rawURL, nil)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	// Missing deviceId parameter entirely.
	w := get("/v1/documents/doc/changes/status")
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing deviceId = %d %s", w.Code, w.Body.String())
	}
	// Empty deviceId parameter.
	w = get("/v1/documents/doc/changes/status?deviceId=")
	if w.Code != http.StatusNotFound {
		t.Fatalf("empty deviceId = %d %s", w.Code, w.Body.String())
	}
	// Unregistered device.
	w = changeStatus(h, "doc", "ghost")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered = %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"changeCount"`) {
		t.Fatalf("404 leaked statistics: %s", w.Body.String())
	}

	// Revoked permission: 403 with no statistics.
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w = changeStatus(h, "doc", "dev")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), `"changeCount"`) {
		t.Fatalf("403 leaked statistics: %s", w.Body.String())
	}

	// An unknown document does not turn the gate failures into success.
	if w := changeStatus(h, "brand-new-doc", "ghost"); w.Code != http.StatusNotFound {
		t.Fatalf("unregistered on unknown doc = %d %s", w.Code, w.Body.String())
	}
}

// Method and path shape: only GET at the exact .../changes/status location is
// accepted; every other verb or shape is a JSON 400, never a redirect.
func TestChangeStatusHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/documents/doc1/changes/status"},
		{http.MethodPut, "/v1/documents/doc1/changes/status"},
		{http.MethodDelete, "/v1/documents/doc1/changes/status"},
		{http.MethodPatch, "/v1/documents/doc1/changes/status"},
		{http.MethodGet, "/v1/documents/doc1/changes/status/"},
		{http.MethodGet, "/v1/documents/doc1/changes/status/extra"},
		{http.MethodGet, "/v1/documents/doc1/changes/statusextra"},
		{http.MethodGet, "/v1/documents//changes/status"},
		{http.MethodGet, "/v1/documents/doc1/changesx/status"},
		{http.MethodGet, "/v1/documents/doc1/status"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d want 400 body = %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
				t.Fatalf("content type = %q, want JSON", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("failure redirected to %q", loc)
			}
			if !strings.HasPrefix(w.Body.String(), `{"error":`) {
				t.Fatalf("body = %q, want a JSON error", w.Body.String())
			}
		})
	}
}

// A document literally named "status" keeps its ordinary change routes; the
// status keyword is only recognized in the terminal subresource position.
func TestChangeStatusHTTPDocumentNamedStatus(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "status", "dev", `[{"id":"z","payload":1}]`)

	// Ordinary paged read on the document named "status".
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/status/changes")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"id":"z"`) {
		t.Fatalf("paged read on doc named status = %d %s", w.Code, w.Body.String())
	}
	// The status subresource of that document answers the summary (one online
	// change).
	w = changeStatus(h, "status", "dev")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"changeCount":1`) {
		t.Fatalf("status on doc named status = %d %s", w.Code, w.Body.String())
	}
}

// The summary derives solely from durable data: after a process restart the
// same request yields the byte-identical body.
func TestChangeStatusHTTPRestartStable(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	body := changeStatus(h, "doc", "dev-1").Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w := changeStatus(h2, "doc", "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w.Body.String())
	}
}
