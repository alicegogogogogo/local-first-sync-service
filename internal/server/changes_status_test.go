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

// getChangeStatus sends the status summary GET and returns the recorder.
func getChangeStatus(t *testing.T, h http.Handler, doc string, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	url := "/v1/documents/" + doc + "/changes/status"
	if rawQuery != "" {
		url += "?" + rawQuery
	}
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// The success body is one compact JSON line with the top-level keys in the
// fixed order onlineCount, boundary, maxCursor, trimmedCount.
func TestChangeStatusHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc1", "dev", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc1/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}

	w = getChangeStatus(t, h, "doc1", "deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	want := `{"onlineCount":3,"boundary":2,"maxCursor":3,"trimmedCount":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// An unknown document is a successful all-zero summary, not an error, and the
// read creates no change or snapshot — a repeat stays identical.
func TestChangeStatusHTTPUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	w := getChangeStatus(t, h, "never-heard-of-it", "deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"onlineCount":0,"boundary":0,"maxCursor":0,"trimmedCount":0}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
	again := getChangeStatus(t, h, "never-heard-of-it", "deviceId=dev")
	if again.Body.String() != w.Body.String() {
		t.Fatalf("repeat changed state: %q vs %q", again.Body.String(), w.Body.String())
	}
}

// After compaction the four numbers follow the durable rows: the surviving
// tail online, the saved-snapshot boundary and exactly the moved ids trimmed.
func TestChangeStatusHTTPAfterCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	w = getChangeStatus(t, h, "doc", "deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"onlineCount":2,"boundary":2,"maxCursor":4,"trimmedCount":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// A second compaction at the same boundary trims nothing; new commits then
// extend the online tail without changing the trimmed total.
func TestChangeStatusHTTPIdempotentCompactAndNewCommits(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 1, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A genuinely new commit past the boundary extends the online tail.
	postRawChanges(t, h, "doc", "dev-1", `[{"id":"new","payload":{"n":9}}]`)

	w = getChangeStatus(t, h, "doc", "deviceId=dev-1")
	want := `{"onlineCount":2,"boundary":1,"maxCursor":3,"trimmedCount":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
}

// A missing, empty or unregistered deviceId is a 404 JSON error with no
// statistics; a revoked device is a 403 JSON error with the same restraint.
func TestChangeStatusHTTPGate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	postDocChanges(t, h, "doc1", "dev", 1)

	for _, q := range []string{"", "deviceId=", "deviceId=ghost"} {
		w := getChangeStatus(t, h, "doc1", q)
		if w.Code != http.StatusNotFound {
			t.Fatalf("query %q status = %d body = %s", q, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "onlineCount") {
			t.Fatalf("404 leaked statistics: %s", w.Body.String())
		}
	}

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}
	w = getChangeStatus(t, h, "doc1", "deviceId=dev")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked status = %d body = %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "onlineCount") {
		t.Fatalf("403 leaked statistics: %s", w.Body.String())
	}
}

// Shape verdicts precede the device verdict and are all JSON 400s, never a
// redirect or HTML: wrong method, trailing slash, extra or missing segments.
func TestChangeStatusHTTPMethodAndPath(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/documents/doc1/changes/status?deviceId=dev"},
		{http.MethodPut, "/v1/documents/doc1/changes/status?deviceId=dev"},
		{http.MethodDelete, "/v1/documents/doc1/changes/status?deviceId=dev"},
		{http.MethodPatch, "/v1/documents/doc1/changes/status?deviceId=dev"},
		{http.MethodGet, "/v1/documents/doc1/changes/status/?deviceId=dev"},
		{http.MethodGet, "/v1/documents/doc1/changes/status/extra?deviceId=dev"},
		{http.MethodGet, "/v1/documents/doc1/changes/statuss?deviceId=dev"},
		{http.MethodGet, "/v1/documents/doc1/changesx/status?deviceId=dev"},
		{http.MethodGet, "/v1/documents//changes/status?deviceId=dev"},
		{http.MethodGet, "/v1/documents/doc1/status?deviceId=dev"},
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

	// A malformed path against an unregistered device is still a 400: shape
	// precedes device existence.
	r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc1/changes/status?deviceId=ghost", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape-before-existence = %d body = %s", w.Code, w.Body.String())
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
	// The status summary of that document answers normally too.
	w = getChangeStatus(t, h, "status", "deviceId=dev")
	if w.Code != http.StatusOK || w.Body.String() != `{"onlineCount":1,"boundary":0,"maxCursor":1,"trimmedCount":0}`+"\n" {
		t.Fatalf("status on doc named status = %d %s", w.Code, w.Body.String())
	}
}

// The summary is read-only: it neither wakes a parked long poll nor consumes
// a change cursor.
func TestChangeStatusHTTPReadOnly(t *testing.T) {
	h, s := newTestHandler(t)
	registerDevice(t, h, "dev")
	postRawChanges(t, h, "doc1", "dev", `[{"id":"a","payload":1}]`)

	done := make(chan struct {
		changes  int
		timedOut bool
		err      error
	}, 1)
	go func() {
		changes, _, timedOut, err := s.WaitForChanges(t.Context(), "doc1", 1, 100, 250*time.Millisecond)
		done <- struct {
			changes  int
			timedOut bool
			err      error
		}{len(changes), timedOut, err}
	}()

	time.Sleep(30 * time.Millisecond) // let the poll park
	for i := 0; i < 3; i++ {
		if w := getChangeStatus(t, h, "doc1", "deviceId=dev"); w.Code != http.StatusOK {
			t.Fatalf("status %d = %d %s", i, w.Code, w.Body.String())
		}
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("poll: %v", got.err)
	}
	if !got.timedOut || got.changes != 0 {
		t.Fatalf("poll woken by a read-only status: %+v", got)
	}

	// The reads consumed no cursor: the next commit gets cursor 2.
	postRawChanges(t, h, "doc1", "dev", `[{"id":"b","payload":2}]`)
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 || rows[1].Cursor != 2 {
		t.Fatalf("status moved the cursor: rows=%+v next=%d", rows, next)
	}
}

// The summary derives solely from durable data: after a restart the same
// request yields the byte-identical body.
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
	body := getChangeStatus(t, h, "doc", "deviceId=dev-1").Body.String()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)
	w := getChangeStatus(t, h2, "doc", "deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("after restart = %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != body {
		t.Fatalf("body changed across restart:\nbefore %q\nafter  %q", body, w.Body.String())
	}
}
