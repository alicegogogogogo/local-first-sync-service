package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// pendingStat is one decoded statistics item.
type pendingStat struct {
	DocumentID  string `json:"documentId"`
	ChangeCount int64  `json:"changeCount"`
	MaxCursor   int64  `json:"maxCursor"`
}

// listPending GETs the pending statistics and decodes the documents array plus
// the count when the status is 200.
func listPending(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []pendingStat, int) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		return w, nil, 0
	}
	var body struct {
		Documents []pendingStat `json:"documents"`
		Count     int           `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("pending body is not JSON: %v body=%s", err, w.Body.String())
	}
	return w, body.Documents, body.Count
}

// A registered device's authored documents are stat'd one item each, sorted
// by id; changeCount counts only the device's online changes and maxCursor is
// the largest cursor among them, unaffected by other devices' interleaving
// changes.
func TestListPendingChangesContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-a", "sess-b")
	createSessions(t, h, "dev-2", "sess-foreign")

	// doc-c gets one change, doc-a two interleaved with a foreign change,
	// doc-b two changes; a foreign device also writes its own document.
	commitSessionChange(t, h, "sess-a", "doc-c", "c1")
	commitSessionChange(t, h, "sess-b", "doc-a", "a1")
	w, _ := postJSON(t, h, "/v1/documents/doc-a/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []map[string]any{{"id": "a2", "payload": map[string]int{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	commitSessionChange(t, h, "sess-a", "doc-a", "a3")
	commitSessionChange(t, h, "sess-b", "doc-b", "b1")
	commitSessionChange(t, h, "sess-a", "doc-b", "b2")
	commitSessionChange(t, h, "sess-foreign", "doc-foreign", "f1")

	w, stats, count := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	want := []pendingStat{
		{DocumentID: "doc-a", ChangeCount: 2, MaxCursor: 3},
		{DocumentID: "doc-b", ChangeCount: 2, MaxCursor: 2},
		{DocumentID: "doc-c", ChangeCount: 1, MaxCursor: 1},
	}
	if !equalPendingStats(stats, want) {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	if count != len(stats) {
		t.Fatalf("count = %d, want %d", count, len(stats))
	}

	// Each item carries exactly the three fixed keys; their order is checked
	// byte-for-byte in TestListPendingChangesBodyShape.
	var raw struct {
		Documents []map[string]json.RawMessage `json:"documents"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, item := range raw.Documents {
		if len(item) != 3 {
			t.Fatalf("item %v must have exactly three keys", item)
		}
		for _, key := range []string{"documentId", "changeCount", "maxCursor"} {
			if _, ok := item[key]; !ok {
				t.Fatalf("item %v must carry %s", item, key)
			}
		}
	}

	// The other device sees only its own rows.
	_, stats, _ = listPending(t, h, "/v1/devices/dev-2/documents/pending")
	if !equalPendingStats(stats, []pendingStat{
		{DocumentID: "doc-a", ChangeCount: 1, MaxCursor: 2},
		{DocumentID: "doc-foreign", ChangeCount: 1, MaxCursor: 1},
	}) {
		t.Fatalf("dev-2 stats = %+v", stats)
	}

	// A registered device that never wrote gets an empty array and zero.
	registerDevice(t, h, "dev-3")
	w, stats, count = listPending(t, h, "/v1/devices/dev-3/documents/pending")
	if w.Code != http.StatusOK || len(stats) != 0 || count != 0 {
		t.Fatalf("empty stats = %d %+v count=%d", w.Code, stats, count)
	}
	if w.Body.String() != `{"documents":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", w.Body.String())
	}
}

func equalPendingStats(got, want []pendingStat) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// Byte-level shape: one compact JSON line plus a trailing newline, top-level
// keys in the documents-then-count order and the three fixed item keys.
func TestListPendingChangesBodyShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "s1")
	commitSessionChange(t, h, "s1", "d1", "c1")
	commitSessionChange(t, h, "s1", "d2", "c2")
	commitSessionChange(t, h, "s1", "d2", "c3")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents/pending", nil)
	w := serveRecorder(h, r)
	body := w.Body.String()
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	if !strings.HasSuffix(body, "\n") || strings.Count(body, "\n") != 1 {
		t.Fatalf("body must be one line plus a trailing newline: %q", body)
	}
	if strings.Contains(body, ", ") || strings.Contains(body, ": ") {
		t.Fatalf("body must be compact JSON: %q", body)
	}
	want := `{"documents":[{"documentId":"d1","changeCount":1,"maxCursor":1},{"documentId":"d2","changeCount":2,"maxCursor":2}],"count":2}` + "\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// limit/offset page the fixed order without repeats or gaps; defaults and the
// 1..1000 bounds behave like the document listing.
func TestListPendingChangesPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	for _, doc := range []string{"d0", "d1", "d2", "d3", "d4"} {
		commitSessionChange(t, h, "sess", doc, "c-"+doc)
	}

	// Default limit is 100: one page holds everything in id order.
	_, stats, _ := listPending(t, h, "/v1/devices/dev/documents/pending")
	if !equalPendingStats(stats, []pendingStat{
		{DocumentID: "d0", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d1", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d2", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d3", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d4", ChangeCount: 1, MaxCursor: 1},
	}) {
		t.Fatalf("default page = %+v", stats)
	}

	// Walk the stats in pages of two: no duplicates, no gaps.
	var seen []pendingStat
	for offset := 0; offset < 5; offset += 2 {
		_, page, count := listPending(t, h, fmt.Sprintf("/v1/devices/dev/documents/pending?limit=2&offset=%d", offset))
		if count != len(page) {
			t.Fatalf("page at offset %d count = %d, len = %d", offset, count, len(page))
		}
		seen = append(seen, page...)
	}
	if !equalPendingStats(seen, []pendingStat{
		{DocumentID: "d0", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d1", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d2", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d3", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d4", ChangeCount: 1, MaxCursor: 1},
	}) {
		t.Fatalf("paged walk = %+v", seen)
	}

	// Partial last page and a page past the end.
	_, page, _ := listPending(t, h, "/v1/devices/dev/documents/pending?limit=4&offset=3")
	if !equalPendingStats(page, []pendingStat{
		{DocumentID: "d3", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "d4", ChangeCount: 1, MaxCursor: 1},
	}) {
		t.Fatalf("last partial page = %+v", page)
	}
	w, page, count := listPending(t, h, "/v1/devices/dev/documents/pending?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 || count != 0 {
		t.Fatalf("page past end = %d %+v count=%d", w.Code, page, count)
	}

	// The accepted bounds.
	for _, query := range []string{"limit=1", "limit=1000", "offset=0"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents/pending?"+query, nil)
		if rec := serveRecorder(h, r); rec.Code != http.StatusOK {
			t.Fatalf("?%s = %d", query, rec.Code)
		}
	}
}

// Illegal pagination values are a 400 JSON error checked before the device
// lookup; an unregistered device is a 404 JSON error with no statistics.
func TestListPendingChangesRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess")
	commitSessionChange(t, h, "sess", "doc-1", "c1")

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents/pending?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Unregistered device: 404 JSON with no statistics content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/documents/pending", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "doc-1") || strings.Contains(w.Body.String(), "changeCount") {
		t.Fatalf("404 body leaks statistics: %s", w.Body.String())
	}

	// Shape errors win over the device lookup: 400 even for a ghost device.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/documents/pending?limit=0", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", rec.Code)
	}

	// None of the rejections changed the statistics.
	_, stats, _ := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-1", ChangeCount: 1, MaxCursor: 1}}) {
		t.Fatalf("stats after rejections = %+v", stats)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET on the pending path are all 400 JSON errors — never a redirect or HTML
// — and change nothing.
func TestListPendingChangesShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess")
	commitSessionChange(t, h, "sess", "doc-1", "c1")

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"empty device segment", http.MethodGet, "/v1/devices//documents/pending"},
		{"trailing slash on pending", http.MethodGet, "/v1/devices/dev-1/documents/pending/"},
		{"one extra segment", http.MethodGet, "/v1/devices/dev-1/documents/pending/x"},
		{"two extra segments", http.MethodGet, "/v1/devices/dev-1/documents/pending/x/y"},
		{"post on the pending path", http.MethodPost, "/v1/devices/dev-1/documents/pending"},
		{"put on the pending path", http.MethodPut, "/v1/devices/dev-1/documents/pending"},
		{"patch on the pending path", http.MethodPatch, "/v1/devices/dev-1/documents/pending"},
		{"delete on the pending path", http.MethodDelete, "/v1/devices/dev-1/documents/pending"},
		{"post past pending", http.MethodPost, "/v1/devices/dev-1/documents/pending/x"},
		{"delete past pending", http.MethodDelete, "/v1/devices/dev-1/documents/pending/x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400 body=%s", tc.method, tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("malformed request produced a redirect to %q", loc)
			}
		})
	}

	// Zero writes: the statistics are unchanged and a GET body is ignored.
	_, stats, _ := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-1", ChangeCount: 1, MaxCursor: 1}}) {
		t.Fatalf("stats after rejected shapes = %+v", stats)
	}
}

// A GET with an unexpected body still answers the read-only statistics and
// creates nothing.
func TestListPendingChangesIgnoresBody(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc", "c1")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents/pending", strings.NewReader(`{"documentId":"ignored"}`))
	r.Header.Set("Content-Type", "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET with body = %d %s", w.Code, w.Body.String())
	}
	_, stats, count := listPending(t, h, "/v1/devices/dev/documents/pending")
	if count != 1 || !equalPendingStats(stats, []pendingStat{{DocumentID: "doc", ChangeCount: 1, MaxCursor: 1}}) {
		t.Fatalf("statistics changed after a GET: %+v", stats)
	}
}

// Deleting a whole document removes it from the statistics; compacting every
// change out of the online log removes it too and shrinks the numbers as the
// trim advances. Other documents survive.
func TestListPendingChangesReflectsDeleteAndCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc-a", "a1")
	commitSessionChange(t, h, "sess", "doc-b", "b1")
	commitSessionChange(t, h, "sess", "doc-b", "b2")

	// Whole-document delete removes doc-a.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc-a?deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, stats, _ := listPending(t, h, "/v1/devices/dev/documents/pending")
	if !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-b", ChangeCount: 2, MaxCursor: 2}}) {
		t.Fatalf("stats after document delete = %+v", stats)
	}

	// Pin a snapshot at doc-b's last cursor and compact: both changes leave
	// the online log, so the document no longer appears.
	w, _ = postJSON(t, h, "/v1/documents/doc-b/snapshots", map[string]any{
		"cursor": 2,
		"state":  map[string]int{"n": 1},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc-b/changes/compact", map[string]any{"deviceId": "dev"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rec, stats, count := listPending(t, h, "/v1/devices/dev/documents/pending")
	if rec.Code != http.StatusOK || count != 0 || len(stats) != 0 {
		t.Fatalf("stats after full compaction = %d %+v count=%d", rec.Code, stats, count)
	}

	// A fresh change on the compacted document brings it back with the fresh
	// cursor alone.
	commitSessionChange(t, h, "sess", "doc-b", "b3")
	_, stats, _ = listPending(t, h, "/v1/devices/dev/documents/pending")
	if !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-b", ChangeCount: 1, MaxCursor: 3}}) {
		t.Fatalf("stats after new change = %+v", stats)
	}
}

// While deregistered a device's statistics answer the same 404 a
// never-registered device gets; the shared change log survives, so the same
// id re-registered sees its surviving authored changes stat'd again.
func TestListPendingChangesReflectsDeregister(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "s1")
	createSessions(t, h, "dev-2", "s2")
	commitSessionChange(t, h, "s1", "doc-a", "a1")
	commitSessionChange(t, h, "s1", "doc-a", "a2")
	commitSessionChange(t, h, "s2", "doc-b", "b1")

	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents/pending", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusNotFound {
		t.Fatalf("stats while deregistered = %d, want 404", rec.Code)
	}
	_, stats, _ := listPending(t, h, "/v1/devices/dev-2/documents/pending")
	if !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-b", ChangeCount: 1, MaxCursor: 1}}) {
		t.Fatalf("other device after deregister = %+v", stats)
	}

	// The shared change log survived: re-registering the same id stats doc-a
	// again with its surviving online rows.
	registerDevice(t, h, "dev-1")
	_, stats, count := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if count != 1 || !equalPendingStats(stats, []pendingStat{{DocumentID: "doc-a", ChangeCount: 2, MaxCursor: 2}}) {
		t.Fatalf("re-registered device stats = %+v count=%d", stats, count)
	}
}

// The keyword "pending" as a device id is an ordinary identifier; the word is
// an endpoint segment only in its fixed position.
func TestListPendingChangesKeywordAsDeviceID(t *testing.T) {
	h, _ := newTestHandler(t)

	// Unregistered device literally named "pending": same 404 as any other
	// unregistered id, reached on the ordinary pending route.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/pending/documents/pending", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("device id 'pending' = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Once registered it behaves like every other device: writes and stats.
	registerDevice(t, h, "pending")
	createSessions(t, h, "pending", "sess")
	commitSessionChange(t, h, "sess", "doc", "c1")
	commitSessionChange(t, h, "sess", "doc", "c2")
	_, stats, count := listPending(t, h, "/v1/devices/pending/documents/pending")
	if count != 1 || !equalPendingStats(stats, []pendingStat{{DocumentID: "doc", ChangeCount: 2, MaxCursor: 2}}) {
		t.Fatalf("device id 'pending' stats = %+v count=%d", stats, count)
	}
}

// Repeated reads and reads across a process restart return byte-identical
// bodies; the read changes nothing.
func TestListPendingChangesDeterministicAndDurable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc-c", "c1")
	commitSessionChange(t, h, "sess", "doc-a", "a1")
	commitSessionChange(t, h, "sess", "doc-a", "a2")
	commitSessionChange(t, h, "sess", "doc-b", "b1")

	snapshot := func() map[string]string {
		bodies := map[string]string{}
		for _, url := range []string{
			"/v1/devices/dev/documents/pending",
			"/v1/devices/dev/documents/pending?limit=1&offset=0",
			"/v1/devices/dev/documents/pending?limit=1&offset=1",
			"/v1/devices/dev/documents/pending?limit=1&offset=2",
			"/v1/devices/dev/documents/pending?offset=3",
		} {
			r := httptest.NewRequest(http.MethodGet, url, nil)
			w := serveRecorder(h, r)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s = %d", url, w.Code)
			}
			bodies[url] = w.Body.String()
		}
		return bodies
	}

	before := snapshot()
	if again := snapshot(); fmt.Sprint(again) != fmt.Sprint(before) {
		t.Fatalf("repeat read changed:\nbefore=%v\nafter=%v", before, again)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h, s = open(t)
	defer func() { _ = s.Close() }()
	after := snapshot()
	for url, body := range before {
		if after[url] != body {
			t.Fatalf("GET %s after restart = %q, want %q", url, after[url], body)
		}
	}
}
