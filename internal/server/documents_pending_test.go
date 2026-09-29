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

// pendingEntry is the decoded shape of one statistics item.
type pendingEntry struct {
	DocumentID  string `json:"documentId"`
	ChangeCount int64  `json:"changeCount"`
	MaxCursor   int64  `json:"maxCursor"`
}

// listPending GETs the pending statistics and decodes the pending array plus
// the count when the status is 200.
func listPending(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []pendingEntry, int) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		return w, nil, 0
	}
	var body struct {
		Pending []pendingEntry `json:"pending"`
		Count   int            `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("pending body is not JSON: %v body=%s", err, w.Body.String())
	}
	return w, body.Pending, body.Count
}

// A registered device's authored documents are listed once each, sorted by
// id, with the count of online changes and the maximum cursor among them;
// documents and changes other devices wrote never enter the numbers.
func TestListPendingContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-a", "sess-b")
	createSessions(t, h, "dev-2", "sess-foreign")

	// Two sessions of dev-1 write across three docs, out of order; doc-c gets
	// a second change (count 2, max cursor 2). dev-2 writes the shared doc-c
	// afterward (cursor 3) and its own doc: neither number of dev-1 moves.
	commitSessionChange(t, h, "sess-a", "doc-c", "c1")
	commitSessionChange(t, h, "sess-b", "doc-a", "c2")
	commitSessionChange(t, h, "sess-a", "doc-b", "c3")
	commitSessionChange(t, h, "sess-b", "doc-c", "c4")
	commitSessionChange(t, h, "sess-foreign", "doc-c", "c5")
	commitSessionChange(t, h, "sess-foreign", "doc-foreign", "c6")

	w, entries, count := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	want := []pendingEntry{
		{DocumentID: "doc-a", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "doc-b", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "doc-c", ChangeCount: 2, MaxCursor: 2},
	}
	if len(entries) != len(want) {
		t.Fatalf("entries = %v, want %v", entries, want)
	}
	for i := range want {
		if entries[i] != want[i] {
			t.Fatalf("entry %d = %+v, want %+v", i, entries[i], want[i])
		}
	}
	if count != len(entries) {
		t.Fatalf("count = %d, want %d", count, len(entries))
	}

	// Each item carries exactly the three fixed keys.
	var body struct {
		Pending []map[string]json.RawMessage `json:"pending"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, item := range body.Pending {
		if len(item) != 3 {
			t.Fatalf("item %v must have exactly three keys", item)
		}
		for _, key := range []string{"documentId", "changeCount", "maxCursor"} {
			if _, ok := item[key]; !ok {
				t.Fatalf("item %v must carry %s", item, key)
			}
		}
	}

	// The other device sees its own rows of doc-c (one change at cursor 3)
	// and its own document.
	_, entries, _ = listPending(t, h, "/v1/devices/dev-2/documents/pending")
	wantForeign := []pendingEntry{
		{DocumentID: "doc-c", ChangeCount: 1, MaxCursor: 3},
		{DocumentID: "doc-foreign", ChangeCount: 1, MaxCursor: 1},
	}
	if len(entries) != len(wantForeign) {
		t.Fatalf("dev-2 entries = %v, want %v", entries, wantForeign)
	}
	for i := range wantForeign {
		if entries[i] != wantForeign[i] {
			t.Fatalf("dev-2 entry %d = %+v, want %+v", i, entries[i], wantForeign[i])
		}
	}

	// A registered device that never wrote gets an empty array and zero.
	registerDevice(t, h, "dev-3")
	w, entries, count = listPending(t, h, "/v1/devices/dev-3/documents/pending")
	if w.Code != http.StatusOK || len(entries) != 0 || count != 0 {
		t.Fatalf("empty stats = %d %v count=%d", w.Code, entries, count)
	}
	if w.Body.String() != `{"pending":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", w.Body.String())
	}
}

// A document-level commit (deviceId in the body) authors the change the same
// way a session commit does, so its document appears with that count.
func TestListPendingIncludesDocumentLevelCommit(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	for _, id := range []string{"x1", "x2"} {
		w, _ := postJSON(t, h, "/v1/documents/doc-direct/changes", map[string]any{
			"deviceId": "dev",
			"changes":  []map[string]any{{"id": id, "payload": map[string]int{"n": 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	_, entries, count := listPending(t, h, "/v1/devices/dev/documents/pending")
	if count != 1 || len(entries) != 1 ||
		entries[0] != (pendingEntry{DocumentID: "doc-direct", ChangeCount: 2, MaxCursor: 2}) {
		t.Fatalf("stats = %v count=%d", entries, count)
	}
}

// Byte-level shape: one compact JSON line plus a trailing newline, top-level
// keys in the pending-then-count order and the three item keys fixed.
func TestListPendingBodyShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "s1")
	commitSessionChange(t, h, "s1", "d1", "c1")
	commitSessionChange(t, h, "s1", "d2", "c2")

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
	want := `{"pending":[{"documentId":"d1","changeCount":1,"maxCursor":1},{"documentId":"d2","changeCount":1,"maxCursor":1}],"count":2}` + "\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// limit/offset page the fixed order without repeats or gaps; defaults and the
// 1..1000 bounds behave like the document listing.
func TestListPendingPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	for _, doc := range []string{"d0", "d1", "d2", "d3", "d4"} {
		commitSessionChange(t, h, "sess", doc, "c-"+doc)
	}

	// Default limit is 100: one page holds everything in id order.
	_, entries, _ := listPending(t, h, "/v1/devices/dev/documents/pending")
	if got := pendingIDs(entries); !equalStrings(got, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("default page = %v", got)
	}

	// Walk the list in pages of two: no duplicates, no gaps.
	var seen []pendingEntry
	for offset := 0; offset < 5; offset += 2 {
		_, page, count := listPending(t, h, fmt.Sprintf("/v1/devices/dev/documents/pending?limit=2&offset=%d", offset))
		if count != len(page) {
			t.Fatalf("page at offset %d count = %d, len = %d", offset, count, len(page))
		}
		seen = append(seen, page...)
	}
	if got := pendingIDs(seen); !equalStrings(got, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("paged walk = %v", got)
	}

	// Partial last page and a page past the end.
	_, page, _ := listPending(t, h, "/v1/devices/dev/documents/pending?limit=4&offset=3")
	if got := pendingIDs(page); !equalStrings(got, []string{"d3", "d4"}) {
		t.Fatalf("last partial page = %v", got)
	}
	w, page, count := listPending(t, h, "/v1/devices/dev/documents/pending?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 || count != 0 {
		t.Fatalf("page past end = %d %v count=%d", w.Code, page, count)
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
func TestListPendingRejectsBadParams(t *testing.T) {
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
	if strings.Contains(w.Body.String(), "doc-1") || strings.Contains(w.Body.String(), "pending") {
		t.Fatalf("404 body leaks statistics: %s", w.Body.String())
	}

	// Shape errors win over the device lookup: 400 even for a ghost device.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/documents/pending?limit=0", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", rec.Code)
	}

	// None of the rejections changed the statistics.
	_, entries, _ := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if got := pendingIDs(entries); !equalStrings(got, []string{"doc-1"}) {
		t.Fatalf("stats after rejections = %v", entries)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET on the pending path are all 400 JSON errors — never a redirect or HTML —
// and change nothing.
func TestListPendingShapeContract(t *testing.T) {
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
		{"extra segment on pending", http.MethodGet, "/v1/devices/dev-1/documents/pending/doc-1"},
		{"post on the pending path", http.MethodPost, "/v1/devices/dev-1/documents/pending"},
		{"put on the pending path", http.MethodPut, "/v1/devices/dev-1/documents/pending"},
		{"patch on the pending path", http.MethodPatch, "/v1/devices/dev-1/documents/pending"},
		{"delete on the pending path", http.MethodDelete, "/v1/devices/dev-1/documents/pending"},
		{"unknown terminal segment", http.MethodGet, "/v1/devices/dev-1/documents/other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
			}
			assertJSONError(t, w)
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("malformed request produced a redirect to %q", loc)
			}
		})
	}

	// Zero writes: the statistics are unchanged and a GET body is ignored.
	_, entries, _ := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if got := pendingIDs(entries); !equalStrings(got, []string{"doc-1"}) {
		t.Fatalf("stats after rejected shapes = %v", entries)
	}
}

// A GET with an unexpected body still answers the read-only statistics and
// creates nothing.
func TestListPendingIgnoresBody(t *testing.T) {
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
	_, entries, count := listPending(t, h, "/v1/devices/dev/documents/pending")
	if count != 1 || entries[0] != (pendingEntry{DocumentID: "doc", ChangeCount: 1, MaxCursor: 1}) {
		t.Fatalf("stats changed after a GET: %v", entries)
	}
}

// Deleting a whole document removes it from the statistics; compacting
// changes out of the online log lowers the count and maximum cursor to what
// remains, and compacting everything removes the document. Other documents
// survive.
func TestListPendingReflectsDeleteAndCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc-a", "a1")
	for _, id := range []string{"b1", "b2", "b3"} {
		commitSessionChange(t, h, "sess", "doc-b", id)
	}

	// Whole-document delete removes doc-a.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc-a?deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, entries, _ := listPending(t, h, "/v1/devices/dev/documents/pending")
	if len(entries) != 1 || entries[0] != (pendingEntry{DocumentID: "doc-b", ChangeCount: 3, MaxCursor: 3}) {
		t.Fatalf("stats after document delete = %v", entries)
	}

	// Pin a snapshot at doc-b's first cursor and compact: one change leaves
	// the online log, so the count drops to two and the maximum cursor stays
	// at the surviving tail's cursor 3.
	w, _ = postJSON(t, h, "/v1/documents/doc-b/snapshots", map[string]any{
		"cursor": 1,
		"state":  map[string]int{"n": 1},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc-b/changes/compact", map[string]any{"deviceId": "dev"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, entries, _ = listPending(t, h, "/v1/devices/dev/documents/pending")
	if len(entries) != 1 || entries[0] != (pendingEntry{DocumentID: "doc-b", ChangeCount: 2, MaxCursor: 3}) {
		t.Fatalf("stats after partial compaction = %v", entries)
	}

	// Compact the rest away: the document no longer appears at all.
	w, _ = postJSON(t, h, "/v1/documents/doc-b/snapshots", map[string]any{
		"cursor": 3,
		"state":  map[string]int{"n": 3},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc-b/changes/compact", map[string]any{"deviceId": "dev"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rec, entries, count := listPending(t, h, "/v1/devices/dev/documents/pending")
	if rec.Code != http.StatusOK || count != 0 || len(entries) != 0 {
		t.Fatalf("stats after full compaction = %d %v count=%d", rec.Code, entries, count)
	}

	// A fresh change on the compacted document brings it back with fresh
	// numbers.
	commitSessionChange(t, h, "sess", "doc-b", "b4")
	_, entries, _ = listPending(t, h, "/v1/devices/dev/documents/pending")
	if len(entries) != 1 || entries[0].DocumentID != "doc-b" || entries[0].ChangeCount != 1 {
		t.Fatalf("stats after new change = %v", entries)
	}
}

// While deregistered a device's statistics answer the same 404 a
// never-registered device gets; other devices' statistics survive. The shared
// change log is preserved across deregistration, so after the same id
// re-registers its surviving authored changes are counted again.
func TestListPendingReflectsDeregister(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "s1")
	createSessions(t, h, "dev-2", "s2")
	commitSessionChange(t, h, "s1", "doc-a", "a1")
	commitSessionChange(t, h, "s2", "doc-b", "b1")

	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents/pending", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusNotFound {
		t.Fatalf("stats while deregistered = %d, want 404", rec.Code)
	}
	_, entries, _ := listPending(t, h, "/v1/devices/dev-2/documents/pending")
	if len(entries) != 1 || entries[0] != (pendingEntry{DocumentID: "doc-b", ChangeCount: 1, MaxCursor: 1}) {
		t.Fatalf("other device after deregister = %v", entries)
	}

	// The shared change log survived: re-registering the same id counts its
	// surviving authored change again.
	registerDevice(t, h, "dev-1")
	_, entries, count := listPending(t, h, "/v1/devices/dev-1/documents/pending")
	if count != 1 || entries[0] != (pendingEntry{DocumentID: "doc-a", ChangeCount: 1, MaxCursor: 1}) {
		t.Fatalf("re-registered device stats = %v count=%d", entries, count)
	}
}

// The keyword "pending" as a device id is an ordinary identifier; the word is
// an endpoint segment only in its fixed terminal position.
func TestListPendingKeywordAsDeviceID(t *testing.T) {
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
	_, entries, count := listPending(t, h, "/v1/devices/pending/documents/pending")
	if count != 1 || entries[0] != (pendingEntry{DocumentID: "doc", ChangeCount: 1, MaxCursor: 1}) {
		t.Fatalf("device id 'pending' stats = %v count=%d", entries, count)
	}
}

// Repeated reads and reads across a process restart return byte-identical
// bodies; the read changes nothing.
func TestListPendingDeterministicAndDurable(t *testing.T) {
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
	for _, doc := range []string{"doc-c", "doc-a", "doc-b"} {
		commitSessionChange(t, h, "sess", doc, "c-"+doc)
	}

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
	// An immediate repeat is byte-for-byte identical.
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

// pendingIDs extracts the document ids of a statistics page in page order.
func pendingIDs(entries []pendingEntry) []string {
	ids := make([]string, 0, len(entries))
	for _, e := range entries {
		ids = append(ids, e.DocumentID)
	}
	return ids
}
