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

// getAttachmentList GETs the attachment collection path and decodes the
// "attachments" array when the status is 200.
func getAttachmentList(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	var body struct {
		Attachments []map[string]any `json:"attachments"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("list body is not JSON: %v body=%s", err, w.Body.String())
		}
		if body.Attachments == nil {
			t.Fatalf("attachments is null, want a (possibly empty) array: %s", w.Body.String())
		}
	}
	return w, body.Attachments
}

// listIDs returns the attachment ids of one listing page, in order.
func listIDs(items []map[string]any) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item["attachmentId"].(string))
	}
	return ids
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// The listing shows the device's own uploads and the uploads it was granted
// read access to, side by side, each once, in creation order, with the full
// metadata, the owned marker and the ascending chunk progress.
func TestListAttachmentsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")

	// dev-1 creates two uploads; dev-2 creates one and grants dev-1 read
	// access. dev-3 has nothing visible at all.
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)
	putChunk(t, h, "dev-1", "att-1", 2, []byte("rld"))
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	mustCreateAttachment(t, h, "dev-2", "att-2", []byte("shared content"), 8)
	putChunk(t, h, "dev-2", "att-2", 0, []byte("shared c"))
	putChunk(t, h, "dev-2", "att-2", 1, []byte("ontent"))
	if w, body := completeAttachment(t, h, "dev-2", "att-2"); w.Code != http.StatusOK || body["complete"] != true {
		t.Fatalf("complete att-2 = %d %v", w.Code, body)
	}
	mustCreateAttachment(t, h, "dev-1", "att-3", []byte("data"), 2)
	setAccess(t, h, "dev-2", "att-2", "dev-1", "grant")

	w, items := getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d body=%s", w.Code, w.Body.String())
	}
	if got := listIDs(items); !equalStrings(got, []string{"att-1", "att-2", "att-3"}) {
		t.Fatalf("list order = %v, want [att-1 att-2 att-3]", got)
	}

	// Own upload: owned=true, metadata and ascending progress.
	if items[0]["owned"] != true ||
		items[0]["totalBytes"] != float64(11) ||
		items[0]["chunkSize"] != float64(4) ||
		items[0]["sha256"] != sha256Hex([]byte("hello world")) ||
		items[0]["complete"] != false ||
		fmt.Sprint(items[0]["receivedChunks"]) != "[0 2]" {
		t.Fatalf("own entry = %v", items[0])
	}
	// Granted upload: owned=false, same metadata the creator sees, sealed.
	if items[1]["owned"] != false ||
		items[1]["totalBytes"] != float64(14) ||
		items[1]["chunkSize"] != float64(8) ||
		items[1]["sha256"] != sha256Hex([]byte("shared content")) ||
		items[1]["complete"] != true ||
		fmt.Sprint(items[1]["receivedChunks"]) != "[0 1]" {
		t.Fatalf("granted entry = %v", items[1])
	}
	// Fresh upload with no chunks yet: an empty (non-null) progress array.
	if items[2]["owned"] != true || fmt.Sprint(items[2]["receivedChunks"]) != "[]" {
		t.Fatalf("empty-progress entry = %v", items[2])
	}
	if !strings.Contains(w.Body.String(), `"receivedChunks":[]`) {
		t.Fatalf("empty progress must serialize as []: %s", w.Body.String())
	}

	// The creator of the granted attachment lists it as its own.
	w, items = getAttachmentList(t, h, "/v1/devices/dev-2/attachments")
	if w.Code != http.StatusOK || !equalStrings(listIDs(items), []string{"att-2"}) || items[0]["owned"] != true {
		t.Fatalf("creator list = %d %v", w.Code, items)
	}

	// A device with nothing visible gets an empty (non-null) array.
	w, items = getAttachmentList(t, h, "/v1/devices/dev-3/attachments")
	if w.Code != http.StatusOK || len(items) != 0 {
		t.Fatalf("empty list = %d %v", w.Code, items)
	}
	if !strings.Contains(w.Body.String(), `"attachments":[]`) {
		t.Fatalf("empty list body = %s", w.Body.String())
	}

	// A grant to the creator itself must not duplicate the entry.
	setAccess(t, h, "dev-1", "att-1", "dev-1", "grant")
	_, items = getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1", "att-2", "att-3"}) {
		t.Fatalf("list after self-grant = %v, want each attachment once", got)
	}
}

// Pagination: limit/offset page the stable creation order; with no
// intervening create or delete the pages neither repeat nor skip an entry.
func TestListAttachmentsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	for i, id := range []string{"att-1", "att-2", "att-3", "att-4", "att-5"} {
		mustCreateAttachment(t, h, "dev-1", id, []byte("x"), 1)
		_ = i
	}
	// One granted attachment from another device sorts by its own creation
	// time: it was created after att-5, so it pages last.
	mustCreateAttachment(t, h, "dev-2", "att-6", []byte("y"), 1)
	setAccess(t, h, "dev-2", "att-6", "dev-1", "grant")

	// Default limit is 100: one page holds everything, in creation order.
	_, items := getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1", "att-2", "att-3", "att-4", "att-5", "att-6"}) {
		t.Fatalf("default page = %v", got)
	}

	// Walk the list in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 6; offset += 2 {
		_, page := getAttachmentList(t, h, fmt.Sprintf("/v1/devices/dev-1/attachments?limit=2&offset=%d", offset))
		if len(page) != 2 {
			t.Fatalf("page at offset %d has %d entries, want 2", offset, len(page))
		}
		seen = append(seen, listIDs(page)...)
	}
	if !equalStrings(seen, []string{"att-1", "att-2", "att-3", "att-4", "att-5", "att-6"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// A partial last page and a page past the end.
	_, page := getAttachmentList(t, h, "/v1/devices/dev-1/attachments?limit=4&offset=4")
	if got := listIDs(page); !equalStrings(got, []string{"att-5", "att-6"}) {
		t.Fatalf("last partial page = %v", got)
	}
	w, page := getAttachmentList(t, h, "/v1/devices/dev-1/attachments?offset=6")
	if w.Code != http.StatusOK || len(page) != 0 {
		t.Fatalf("page past end = %d %v", w.Code, page)
	}

	// limit=1 and limit=1000 are both accepted bounds.
	if w, _ := getAttachmentList(t, h, "/v1/devices/dev-1/attachments?limit=1"); w.Code != http.StatusOK {
		t.Fatalf("limit=1 = %d", w.Code)
	}
	if w, _ := getAttachmentList(t, h, "/v1/devices/dev-1/attachments?limit=1000"); w.Code != http.StatusOK {
		t.Fatalf("limit=1000 = %d", w.Code)
	}
}

// Illegal pagination parameters are a 400 JSON error; an unregistered device
// is a 404 JSON error with no listing. Parameter shape is checked first.
func TestListAttachmentsRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("x"), 1)

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Unregistered device: 404 JSON, no listing content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/attachments", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "att-1") || strings.Contains(w.Body.String(), "attachments") {
		t.Fatalf("404 body leaks listing: %s", w.Body.String())
	}

	// Shape errors win over the device lookup: 400 even for a ghost device.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/attachments?limit=0", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", w.Code)
	}

	// None of the rejections changed the listing.
	_, items := getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1"}) {
		t.Fatalf("list after rejections = %v", got)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET on the collection path are all 400 JSON errors — never a redirect or
// HTML — and change nothing.
func TestListAttachmentsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("x"), 1)

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifiers and trailing slashes.
		{http.MethodGet, "/v1/devices//attachments"},
		{http.MethodGet, "/v1/devices/dev-1/attachments/"},
		{http.MethodGet, "/v1/devices/dev-1//attachments"},
		// Extra segments past the collection or item path.
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/extra"},
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/chunks/0/extra"},
		// Verbs other than GET/POST/DELETE on the collection path.
		{http.MethodPut, "/v1/devices/dev-1/attachments"},
		{http.MethodPatch, "/v1/devices/dev-1/attachments"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s content-type = %q, want application/json", tc.method, tc.path, ct)
		}
	}

	// Zero writes: the listing is unchanged.
	_, items := getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1"}) {
		t.Fatalf("list after rejected shapes = %v", got)
	}
}

// A deleted attachment vanishes from every listing at once — the creator's
// and every granted device's — and re-creating the id lists a brand-new
// record at the end of the creation order with its progress back to zero.
func TestListAttachmentsDeleteAndRecreate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	mustCreateAttachment(t, h, "dev-1", "att-2", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	deleteAttachment(t, h, "dev-1", "att-1")

	// Gone from the creator's and the granted device's listings at once.
	_, items := getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-2"}) {
		t.Fatalf("creator list after delete = %v", got)
	}
	w, items := getAttachmentList(t, h, "/v1/devices/dev-2/attachments")
	if w.Code != http.StatusOK || len(items) != 0 {
		t.Fatalf("granted list after delete = %d %v", w.Code, items)
	}

	// Re-created under the same id: a new record at the end of the order,
	// with the chunk progress starting from zero again.
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)
	_, items = getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-2", "att-1"}) {
		t.Fatalf("list after recreate = %v, want [att-2 att-1]", got)
	}
	if fmt.Sprint(items[1]["receivedChunks"]) != "[]" || items[1]["complete"] != false {
		t.Fatalf("recreated entry = %v, want zero progress", items[1])
	}
	// The old grant was deleted with the record: dev-2 still sees nothing.
	w, items = getAttachmentList(t, h, "/v1/devices/dev-2/attachments")
	if w.Code != http.StatusOK || len(items) != 0 {
		t.Fatalf("granted list after recreate = %d %v", w.Code, items)
	}
}

// Revoking a device's read access removes the attachment from its listing at
// once; the creator's listing is unaffected.
func TestListAttachmentsRevoke(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)
	mustCreateAttachment(t, h, "dev-2", "att-2", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	_, items := getAttachmentList(t, h, "/v1/devices/dev-2/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1", "att-2"}) {
		t.Fatalf("granted list = %v, want [att-1 att-2]", got)
	}

	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")

	_, items = getAttachmentList(t, h, "/v1/devices/dev-2/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-2"}) {
		t.Fatalf("list after revoke = %v, want [att-2]", got)
	}
	_, items = getAttachmentList(t, h, "/v1/devices/dev-1/attachments")
	if got := listIDs(items); !equalStrings(got, []string{"att-1"}) {
		t.Fatalf("creator list after revoke = %v, want [att-1]", got)
	}
}

// The listing is a read-only view: content, order and pagination are
// byte-for-byte identical across a process restart.
func TestListAttachmentsSurvivesRestart(t *testing.T) {
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
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("hello world"), 4)
	putChunk(t, h, "dev-1", "att-1", 0, []byte("hell"))
	mustCreateAttachment(t, h, "dev-2", "att-2", []byte("shared content"), 8)
	setAccess(t, h, "dev-2", "att-2", "dev-1", "grant")

	snapshot := func(t *testing.T, h http.Handler) map[string]string {
		t.Helper()
		bodies := map[string]string{}
		for _, url := range []string{
			"/v1/devices/dev-1/attachments",
			"/v1/devices/dev-1/attachments?limit=1&offset=0",
			"/v1/devices/dev-1/attachments?limit=1&offset=1",
			"/v1/devices/dev-2/attachments",
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

	before := snapshot(t, h)
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	after := snapshot(t, h)
	for url, body := range before {
		if after[url] != body {
			t.Fatalf("GET %s after restart = %s, want %s", url, after[url], body)
		}
	}
}
