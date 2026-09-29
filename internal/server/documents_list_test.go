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

// listDeviceDocuments GETs the device document collection and decodes the
// documents array plus the count when the status is 200.
func listDeviceDocuments(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []string, int) {
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

// seedSessionDocument writes one change into doc through the device's
// session, the documented client path that stamps the stored change with the
// session's owning device.
func seedSessionDocument(t *testing.T, h http.Handler, session, doc string) {
	t.Helper()
	w, _ := postSessionChanges(t, h, session, doc, map[string]any{
		"changes": []any{map[string]any{"id": "c-" + doc, "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("seed %s via %s = %d %s", doc, session, w.Code, w.Body.String())
	}
}

// A registered device's documents are listed once each, sorted by id,
// regardless of write order, de-duplicated across sessions and batches;
// documents only other devices wrote never appear.
func TestListDeviceDocumentsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-1a", "sess-1b")
	createSessions(t, h, "dev-2", "sess-2")

	// Out of lexicographic order, one doc through each of two sessions, and a
	// repeat write into an already-seen doc to prove de-duplication.
	seedSessionDocument(t, h, "sess-1a", "doc-c")
	seedSessionDocument(t, h, "sess-1b", "doc-a")
	seedSessionDocument(t, h, "sess-1a", "doc-b")
	seedSessionDocument(t, h, "sess-1b", "doc-a")
	seedSessionDocument(t, h, "sess-2", "doc-foreign")

	w, ids, count := listDeviceDocuments(t, h, "/v1/devices/dev-1/documents")
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
	_, ids, _ = listDeviceDocuments(t, h, "/v1/devices/dev-2/documents")
	if !equalStrings(ids, []string{"doc-foreign"}) {
		t.Fatalf("dev-2 list = %v", ids)
	}

	// A registered device that has written nothing gets an empty array and
	// zero.
	registerDevice(t, h, "dev-3")
	w, ids, count = listDeviceDocuments(t, h, "/v1/devices/dev-3/documents")
	if w.Code != http.StatusOK || len(ids) != 0 || count != 0 {
		t.Fatalf("empty list = %d %v count=%d", w.Code, ids, count)
	}
	if w.Body.String() != `{"documents":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", w.Body.String())
	}
}

// Byte-level shape: one compact JSON line plus a trailing newline, top-level
// keys in the documents-then-count order.
func TestListDeviceDocumentsBodyShape(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSessionDocument(t, h, "sess", "d1")
	seedSessionDocument(t, h, "sess", "d2")

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
func TestListDeviceDocumentsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	for _, doc := range []string{"d0", "d1", "d2", "d3", "d4"} {
		seedSessionDocument(t, h, "sess", doc)
	}

	// Default limit is 100: one page holds everything in id order.
	_, ids, _ := listDeviceDocuments(t, h, "/v1/devices/dev/documents")
	if !equalStrings(ids, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("default page = %v", ids)
	}

	// Walk the list in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		_, page, count := listDeviceDocuments(t, h, fmt.Sprintf("/v1/devices/dev/documents?limit=2&offset=%d", offset))
		if count != len(page) {
			t.Fatalf("page at offset %d count = %d, len = %d", offset, count, len(page))
		}
		seen = append(seen, page...)
	}
	if !equalStrings(seen, []string{"d0", "d1", "d2", "d3", "d4"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// Partial last page and a page past the end.
	_, page, _ := listDeviceDocuments(t, h, "/v1/devices/dev/documents?limit=4&offset=3")
	if !equalStrings(page, []string{"d3", "d4"}) {
		t.Fatalf("last partial page = %v", page)
	}
	w, page, count := listDeviceDocuments(t, h, "/v1/devices/dev/documents?offset=5")
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
func TestListDeviceDocumentsRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess-1")
	seedSessionDocument(t, h, "sess-1", "doc-1")

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
	_, ids, _ := listDeviceDocuments(t, h, "/v1/devices/dev-1/documents")
	if !equalStrings(ids, []string{"doc-1"}) {
		t.Fatalf("list after rejections = %v", ids)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET on the collection path are all 400 JSON errors — never a redirect or
// HTML — and change nothing.
func TestListDeviceDocumentsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess-1")
	seedSessionDocument(t, h, "sess-1", "doc-1")

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
		{"delete with an extra segment", http.MethodDelete, "/v1/devices/dev-1/documents/doc-1"},
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
	_, ids, _ := listDeviceDocuments(t, h, "/v1/devices/dev-1/documents")
	if !equalStrings(ids, []string{"doc-1"}) {
		t.Fatalf("list after rejected shapes = %v", ids)
	}
}

// A GET with an unexpected body still answers the read-only listing and
// creates nothing.
func TestListDeviceDocumentsIgnoresBody(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSessionDocument(t, h, "sess", "doc")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/documents", strings.NewReader(`{"documentId":"ignored"}`))
	r.Header.Set("Content-Type", "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET with body = %d %s", w.Code, w.Body.String())
	}
	_, ids, count := listDeviceDocuments(t, h, "/v1/devices/dev/documents")
	if count != 1 || !equalStrings(ids, []string{"doc"}) {
		t.Fatalf("listing changed after a GET: %v", ids)
	}
}

// A wholly deleted document and a document whose online changes were all
// compacted away both leave the listing; the surviving documents stay.
func TestListDeviceDocumentsReflectsDeleteAndCompaction(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSessionDocument(t, h, "sess", "doc-keep")
	seedSessionDocument(t, h, "sess", "doc-del")
	seedSessionDocument(t, h, "sess", "doc-compact")

	// Wholly delete doc-del through the document cleanup entry.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc-del?deviceId=dev")
	if w.Code != http.StatusOK {
		t.Fatalf("delete document = %d %s", w.Code, w.Body.String())
	}

	// Compact doc-compact fully: pin a snapshot at its only cursor then trim.
	w, _ = postJSON(t, h, "/v1/documents/doc-compact/snapshots", `{"cursor":1,"state":{"k":1}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot = %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/sessions/sess/documents/doc-compact/changes/compact", `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	_, ids, count := listDeviceDocuments(t, h, "/v1/devices/dev/documents")
	if count != 1 || !equalStrings(ids, []string{"doc-keep"}) {
		t.Fatalf("list after delete+compaction = %v count=%d", ids, count)
	}

	// A page past the one survivor is empty, still 200.
	_, page, count := listDeviceDocuments(t, h, "/v1/devices/dev/documents?offset=1")
	if count != 0 || len(page) != 0 {
		t.Fatalf("past-end page = %v count=%d", page, count)
	}
}

// Deregistering the device turns its listing into the same 404 a
// never-registered device gets; another device's listing is untouched. The
// change log is shared and append-only, so a deregistration never deletes
// change records (document reads stay unchanged): when the same id registers
// again it is a live device and the surviving online changes stamped with it
// remain attributable to it, exactly as the document change reads do.
func TestListDeviceDocumentsDeregister(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess-1")
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	seedSessionDocument(t, h, "sess-1", "doc-a")
	seedSessionDocument(t, h, "sess-2", "doc-b")

	if w, _ := deregister(t, h, "dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// While the device is gone its listing is a 404, leaking no content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/documents", nil)
	rec := serveRecorder(h, r)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("list while deregistered = %d, want 404", rec.Code)
	}
	assertJSONError(t, rec)
	_, ids, _ := listDeviceDocuments(t, h, "/v1/devices/dev-2/documents")
	if !equalStrings(ids, []string{"doc-b"}) {
		t.Fatalf("other device after deregister = %v", ids)
	}

	// Re-registering the same id makes it a live device again; its surviving
	// online change history is still attributable to it (the deregistration
	// deleted no change row), so doc-a stays listed. A genuinely fresh id with
	// no history still gets the successful empty page.
	registerDevice(t, h, "dev-1")
	w, ids, count := listDeviceDocuments(t, h, "/v1/devices/dev-1/documents")
	if w.Code != http.StatusOK || count != 1 || !equalStrings(ids, []string{"doc-a"}) {
		t.Fatalf("re-registered device list = %d %v count=%d", w.Code, ids, count)
	}
	registerDevice(t, h, "dev-fresh")
	w, ids, count = listDeviceDocuments(t, h, "/v1/devices/dev-fresh/documents")
	if w.Code != http.StatusOK || count != 0 || len(ids) != 0 {
		t.Fatalf("fresh device list = %d %v count=%d", w.Code, ids, count)
	}
}

// Deleting the single session that wrote a change does not remove the change
// row, so the document stays listed: the listing derives from the durable
// change log, not from live sessions, and session deletion is not one of the
// listing's disappearance triggers.
func TestListDeviceDocumentsSurvivesSessionDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSessionDocument(t, h, "sess", "doc")

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess"); w.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", w.Code, w.Body.String())
	}
	_, ids, count := listDeviceDocuments(t, h, "/v1/devices/dev/documents")
	if count != 1 || !equalStrings(ids, []string{"doc"}) {
		t.Fatalf("list after session delete = %v count=%d", ids, count)
	}
}

// The keyword "documents" as a device id is an ordinary identifier; the word
// is an endpoint segment only in its fixed position.
func TestListDeviceDocumentsKeywordAsDeviceID(t *testing.T) {
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
	createSessionViaHTTP(t, h, "documents", "sess")
	seedSessionDocument(t, h, "sess", "doc")
	_, ids, count := listDeviceDocuments(t, h, "/v1/devices/documents/documents")
	if count != 1 || !equalStrings(ids, []string{"doc"}) {
		t.Fatalf("device id 'documents' list = %v count=%d", ids, count)
	}
}

// Repeated reads and reads across a process restart return byte-identical
// bodies; the read changes nothing.
func TestListDeviceDocumentsDeterministicAndDurable(t *testing.T) {
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
	createSessionViaHTTP(t, h, "dev", "sess")
	for _, doc := range []string{"doc-c", "doc-a", "doc-b"} {
		seedSessionDocument(t, h, "sess", doc)
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
