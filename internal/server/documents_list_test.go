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

// listDocuments GETs the device document collection and decodes the documents
// array plus the count when the status is 200.
func listDocuments(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []string, int) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		return w, nil, 0
	}
	var body struct {
		Documents []struct {
			DocumentID string `json:"documentId"`
		} `json:"documents"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("list body is not JSON: %v body=%s", err, w.Body.String())
	}
	ids := make([]string, 0, len(body.Documents))
	for _, e := range body.Documents {
		ids = append(ids, e.DocumentID)
	}
	return w, ids, body.Count
}

// commitSessionChange writes one change to doc through session, so the change
// is authored by the session's owning device.
func commitSessionChange(t *testing.T, h http.Handler, session, doc, changeID string) {
	t.Helper()
	w, _ := postSessionChanges(t, h, session, doc, map[string]any{
		"changes": []map[string]any{{"id": changeID, "payload": map[string]int{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("commit change %q on %q = %d %s", changeID, doc, w.Code, w.Body.String())
	}
}

// A registered device's authored documents are listed, each once, sorted by
// id, regardless of write order or which session wrote them; documents other
// devices wrote never appear.
func TestListDocumentsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-a", "sess-b")
	createSessions(t, h, "dev-2", "sess-foreign")

	// Two sessions of dev-1 write across three docs, out of order; doc-c gets
	// a second change (it must still appear once).
	commitSessionChange(t, h, "sess-a", "doc-c", "c1")
	commitSessionChange(t, h, "sess-b", "doc-a", "c2")
	commitSessionChange(t, h, "sess-a", "doc-b", "c3")
	commitSessionChange(t, h, "sess-b", "doc-c", "c4")
	commitSessionChange(t, h, "sess-foreign", "doc-foreign", "c5")

	w, ids, count := listDocuments(t, h, "/v1/devices/dev-1/documents")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	want := []string{"doc-a", "doc-b", "doc-c"}
	if !equalStrings(ids, want) {
		t.Fatalf("order = %v, want %v", ids, want)
	}
	if count != len(ids) {
		t.Fatalf("count = %d, want %d", count, len(ids))
	}

	// Each item carries only the documentId key.
	var body struct {
		Documents []map[string]json.RawMessage `json:"documents"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, item := range body.Documents {
		if len(item) != 1 {
			t.Fatalf("item %v must have exactly one key", item)
		}
		if _, ok := item["documentId"]; !ok {
			t.Fatalf("item %v must carry documentId", item)
		}
	}

	// The other device sees only its own document.
	_, ids, _ = listDocuments(t, h, "/v1/devices/dev-2/documents")
	if !equalStrings(ids, []string{"doc-foreign"}) {
		t.Fatalf("dev-2 list = %v", ids)
	}

	// A registered device that never wrote gets an empty array and zero.
	registerDevice(t, h, "dev-3")
	w, ids, count = listDocuments(t, h, "/v1/devices/dev-3/documents")
	if w.Code != http.StatusOK || len(ids) != 0 || count != 0 {
		t.Fatalf("empty list = %d %v count=%d", w.Code, ids, count)
	}
	if w.Body.String() != `{"documents":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", w.Body.String())
	}
}

// A document-level commit (deviceId in the body) authors the change the same
// way a session commit does, so that document appears too.
func TestListDocumentsIncludesDocumentLevelCommit(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	w, _ := postJSON(t, h, "/v1/documents/doc-direct/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []map[string]any{{"id": "x1", "payload": map[string]int{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, ids, count := listDocuments(t, h, "/v1/devices/dev/documents")
	if count != 1 || !equalStrings(ids, []string{"doc-direct"}) {
		t.Fatalf("list = %v count=%d", ids, count)
	}
}

// Byte-level shape: one compact JSON line plus a trailing newline, top-level
// keys in the documents-then-count order.
func TestListDocumentsBodyShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "s1")
	commitSessionChange(t, h, "s1", "d1", "c1")
	commitSessionChange(t, h, "s1", "d2", "c2")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents", nil)
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
	want := `{"documents":[{"documentId":"d1"},{"documentId":"d2"}],"count":2}` + "\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// limit/offset page the fixed order without repeats or gaps; defaults and the
// 1..1000 bounds behave like the attachment listing.
func TestListDocumentsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	for _, doc := range []string{"d0", "d1", "d2", "d3", "d4"} {
		commitSessionChange(t, h, "sess", doc, "c-"+doc)
	}

	// Default limit is 100: one page holds everything in id order.
	_, ids, _ := listDocuments(t, h, "/v1/devices/dev/documents")
	if !equalStrings(ids, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("default page = %v", ids)
	}

	// Walk the list in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		_, page, count := listDocuments(t, h, fmt.Sprintf("/v1/devices/dev/documents?limit=2&offset=%d", offset))
		if count != len(page) {
			t.Fatalf("page at offset %d count = %d, len = %d", offset, count, len(page))
		}
		seen = append(seen, page...)
	}
	if !equalStrings(seen, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// Partial last page and a page past the end.
	_, page, _ := listDocuments(t, h, "/v1/devices/dev/documents?limit=4&offset=3")
	if !equalStrings(page, []string{"d3", "d4"}) {
		t.Fatalf("last partial page = %v", page)
	}
	w, page, count := listDocuments(t, h, "/v1/devices/dev/documents?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 || count != 0 {
		t.Fatalf("page past end = %d %v count=%d", w.Code, page, count)
	}

	// The accepted bounds.
	for _, query := range []string{"limit=1", "limit=1000", "offset=0"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents?"+query, nil)
		if rec := serveRecorder(h, r); rec.Code != http.StatusOK {
			t.Fatalf("?%s = %d", query, rec.Code)
		}
	}
}

// Illegal pagination values are a 400 JSON error checked before the device
// lookup; an unregistered device is a 404 JSON error with no listing content.
func TestListDocumentsRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess")
	commitSessionChange(t, h, "sess", "doc-1", "c1")

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Unregistered device: 404 JSON with no listing content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/documents", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "doc-1") || strings.Contains(w.Body.String(), "documents") {
		t.Fatalf("404 body leaks listing: %s", w.Body.String())
	}

	// Shape errors win over the device lookup: 400 even for a ghost device.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/documents?limit=0", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", rec.Code)
	}

	// None of the rejections changed the listing.
	_, ids, _ := listDocuments(t, h, "/v1/devices/dev-1/documents")
	if !equalStrings(ids, []string{"doc-1"}) {
		t.Fatalf("list after rejections = %v", ids)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET on the collection path are all 400 JSON errors — never a redirect or
// HTML — and change nothing.
func TestListDocumentsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess")
	commitSessionChange(t, h, "sess", "doc-1", "c1")

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"empty device segment", http.MethodGet, "/v1/devices//documents"},
		{"trailing slash on collection", http.MethodGet, "/v1/devices/dev-1/documents/"},
		{"extra segment on get", http.MethodGet, "/v1/devices/dev-1/documents/doc-1"},
		{"two extra segments on get", http.MethodGet, "/v1/devices/dev-1/documents/doc-1/extra"},
		{"post on the collection", http.MethodPost, "/v1/devices/dev-1/documents"},
		{"put on the collection", http.MethodPut, "/v1/devices/dev-1/documents"},
		{"patch on the collection", http.MethodPatch, "/v1/devices/dev-1/documents"},
		{"delete on the collection", http.MethodDelete, "/v1/devices/dev-1/documents"},
		{"post past the collection", http.MethodPost, "/v1/devices/dev-1/documents/doc-1"},
		{"delete past the collection", http.MethodDelete, "/v1/devices/dev-1/documents/doc-1"},
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

	// Zero writes: the listing is unchanged and a GET body is ignored by the
	// read-only route.
	_, ids, _ := listDocuments(t, h, "/v1/devices/dev-1/documents")
	if !equalStrings(ids, []string{"doc-1"}) {
		t.Fatalf("list after rejected shapes = %v", ids)
	}
}

// A GET with an unexpected body still answers the read-only listing and
// creates nothing.
func TestListDocumentsIgnoresBody(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc", "c1")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents", strings.NewReader(`{"documentId":"ignored"}`))
	r.Header.Set("Content-Type", "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET with body = %d %s", w.Code, w.Body.String())
	}
	_, ids, count := listDocuments(t, h, "/v1/devices/dev/documents")
	if count != 1 || !equalStrings(ids, []string{"doc"}) {
		t.Fatalf("listing changed after a GET: %v", ids)
	}
}

// Deleting a whole document removes it from the listing; compacting every
// change out of the online log removes it too. Other documents survive.
func TestListDocumentsReflectsDeleteAndCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")
	commitSessionChange(t, h, "sess", "doc-a", "a1")
	commitSessionChange(t, h, "sess", "doc-b", "b1")

	// Whole-document delete removes doc-a.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc-a?deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, ids, _ := listDocuments(t, h, "/v1/devices/dev/documents")
	if !equalStrings(ids, []string{"doc-b"}) {
		t.Fatalf("list after document delete = %v", ids)
	}

	// Pin a snapshot at doc-b's last cursor and compact: its change leaves
	// the online log, so the document no longer appears.
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
	rec, ids, count := listDocuments(t, h, "/v1/devices/dev/documents")
	if rec.Code != http.StatusOK || count != 0 || len(ids) != 0 {
		t.Fatalf("list after full compaction = %d %v count=%d", rec.Code, ids, count)
	}

	// A fresh change on the compacted document brings it back.
	commitSessionChange(t, h, "sess", "doc-b", "b2")
	_, ids, _ = listDocuments(t, h, "/v1/devices/dev/documents")
	if !equalStrings(ids, []string{"doc-b"}) {
		t.Fatalf("list after new change = %v", ids)
	}
}

// While deregistered a device's listing answers the same 404 a
// never-registered device gets; other devices' listings survive. The shared
// change log is deliberately preserved across deregistration (the baseline
// cascade keeps every authored change for collaborators), so after the same
// id re-registers the surviving online changes it authored still name it and
// their documents list again; only a deleted or fully compacted document is
// gone.
func TestListDocumentsReflectsDeregister(t *testing.T) {
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
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusNotFound {
		t.Fatalf("list while deregistered = %d, want 404", rec.Code)
	}
	_, ids, _ := listDocuments(t, h, "/v1/devices/dev-2/documents")
	if !equalStrings(ids, []string{"doc-b"}) {
		t.Fatalf("other device after deregister = %v", ids)
	}

	// The shared change log survived: re-registering the same id lets its
	// surviving authored change list doc-a again. New writes appear alongside
	// it like on any device.
	registerDevice(t, h, "dev-1")
	_, ids, count := listDocuments(t, h, "/v1/devices/dev-1/documents")
	if count != 1 || !equalStrings(ids, []string{"doc-a"}) {
		t.Fatalf("re-registered device list = %v count=%d", ids, count)
	}
	createSessions(t, h, "dev-1", "s1-new")
	commitSessionChange(t, h, "s1-new", "doc-c", "c1")
	_, ids, _ = listDocuments(t, h, "/v1/devices/dev-1/documents")
	if !equalStrings(ids, []string{"doc-a", "doc-c"}) {
		t.Fatalf("list after a new write = %v", ids)
	}
}

// The keyword "documents" as a device id is an ordinary identifier; the word
// is an endpoint segment only in its fixed position.
func TestListDocumentsKeywordAsDeviceID(t *testing.T) {
	h, _ := newTestHandler(t)

	// Unregistered device literally named "documents": same 404 as any other
	// unregistered id, reached on the ordinary collection route.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/documents/documents", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("device id 'documents' = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Once registered it behaves like every other device: writes and lists.
	registerDevice(t, h, "documents")
	createSessions(t, h, "documents", "sess")
	commitSessionChange(t, h, "sess", "doc", "c1")
	_, ids, count := listDocuments(t, h, "/v1/devices/documents/documents")
	if count != 1 || !equalStrings(ids, []string{"doc"}) {
		t.Fatalf("device id 'documents' list = %v count=%d", ids, count)
	}
}

// Repeated reads and reads across a process restart return byte-identical
// bodies; the read changes nothing.
func TestListDocumentsDeterministicAndDurable(t *testing.T) {
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
			"/v1/devices/dev/documents",
			"/v1/devices/dev/documents?limit=1&offset=0",
			"/v1/devices/dev/documents?limit=1&offset=1",
			"/v1/devices/dev/documents?limit=1&offset=2",
			"/v1/devices/dev/documents?offset=3",
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
