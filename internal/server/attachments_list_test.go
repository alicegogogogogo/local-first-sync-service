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

// listAttachments issues GET on the collection path and returns the recorder
// plus the decoded body.
func listAttachments(t *testing.T, h http.Handler, deviceID, query string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	url := "/v1/devices/" + deviceID + "/attachments"
	if query != "" {
		url += "?" + query
	}
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	var body map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &body)
	}
	return w, body
}

// mustList returns the decoded attachments array of a 200 listing.
func mustList(t *testing.T, h http.Handler, deviceID, query string) []any {
	t.Helper()
	w, body := listAttachments(t, h, deviceID, query)
	if w.Code != http.StatusOK {
		t.Fatalf("list %q ?%s = %d body=%s", deviceID, query, w.Code, w.Body.String())
	}
	items, ok := body["attachments"].([]any)
	if !ok {
		t.Fatalf("list %q ?%s: attachments = %v, want an array", deviceID, query, body["attachments"])
	}
	return items
}

// listedIDs extracts the attachmentId sequence of a decoded listing page.
func listedIDs(t *testing.T, items []any) []string {
	t.Helper()
	ids := make([]string, 0, len(items))
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			t.Fatalf("list item = %v, want an object", it)
		}
		ids = append(ids, m["attachmentId"].(string))
	}
	return ids
}

func TestListAttachmentsOwnAndGranted(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")

	contentA := []byte("alpha-content")
	contentB := []byte("beta-content!")
	contentC := []byte("gamma")
	mustCreateAttachment(t, h, "dev-1", "att-a", contentA, 8)
	mustCreateAttachment(t, h, "dev-2", "att-b", contentB, 4)
	mustCreateAttachment(t, h, "dev-3", "att-c", contentC, 8)

	// dev-1 receives chunks out of order; the listing reports them ascending.
	putChunk(t, h, "dev-1", "att-a", 1, contentA[8:])
	putChunk(t, h, "dev-1", "att-a", 0, contentA[:8])

	// dev-2 grants dev-1 read access to att-b; dev-3 grants nobody.
	setAccess(t, h, "dev-2", "att-b", "dev-1", "grant")

	items := mustList(t, h, "dev-1", "")
	ids := listedIDs(t, items)
	if len(ids) != 2 || ids[0] != "att-a" || ids[1] != "att-b" {
		t.Fatalf("dev-1 listing ids = %v, want [att-a att-b]", ids)
	}

	own := items[0].(map[string]any)
	if own["owned"] != true ||
		own["totalBytes"] != float64(len(contentA)) ||
		own["chunkSize"] != float64(8) ||
		own["sha256"] != sha256Hex(contentA) ||
		own["complete"] != false {
		t.Fatalf("own item = %v", own)
	}
	chunks, ok := own["receivedChunks"].([]any)
	if !ok || len(chunks) != 2 || chunks[0] != float64(0) || chunks[1] != float64(1) {
		t.Fatalf("own receivedChunks = %v, want [0 1]", own["receivedChunks"])
	}

	granted := items[1].(map[string]any)
	if granted["owned"] != false || granted["attachmentId"] != "att-b" {
		t.Fatalf("granted item = %v", granted)
	}
	if chunks, ok := granted["receivedChunks"].([]any); !ok || len(chunks) != 0 {
		t.Fatalf("granted receivedChunks = %v, want []", granted["receivedChunks"])
	}

	// The grantee's view of the granted item matches the creator's metadata.
	creatorItems := mustList(t, h, "dev-2", "")
	if len(creatorItems) != 1 {
		t.Fatalf("dev-2 listing = %v, want one item", creatorItems)
	}
	creatorView := creatorItems[0].(map[string]any)
	for _, k := range []string{"attachmentId", "totalBytes", "chunkSize", "sha256", "complete"} {
		if creatorView[k] != granted[k] {
			t.Fatalf("field %s: creator %v != grantee %v", k, creatorView[k], granted[k])
		}
	}
	if creatorView["owned"] != true {
		t.Fatalf("creator owned = %v, want true", creatorView["owned"])
	}

	// dev-3 sees only its own attachment.
	if ids := listedIDs(t, mustList(t, h, "dev-3", "")); len(ids) != 1 || ids[0] != "att-c" {
		t.Fatalf("dev-3 listing ids = %v, want [att-c]", ids)
	}
}

func TestListAttachmentsCreationOrderAndDedup(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// Interleave creators; the listing follows creation order, not id order.
	content := []byte("x")
	for i, id := range []string{"z-first", "a-second", "m-third"} {
		device := "dev-1"
		if i == 1 {
			device = "dev-2"
		}
		mustCreateAttachment(t, h, device, id, content, 1)
	}
	setAccess(t, h, "dev-2", "a-second", "dev-1", "grant")
	// A self-grant must not duplicate the creator's own entry.
	setAccess(t, h, "dev-1", "z-first", "dev-1", "grant")

	ids := listedIDs(t, mustList(t, h, "dev-1", ""))
	want := []string{"z-first", "a-second", "m-third"}
	if len(ids) != len(want) {
		t.Fatalf("ids = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("ids = %v, want %v", ids, want)
		}
	}
	seen := map[string]int{}
	for _, id := range ids {
		seen[id]++
		if seen[id] > 1 {
			t.Fatalf("id %q appears more than once in %v", id, ids)
		}
	}
}

func TestListAttachmentsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	content := []byte("x")
	for i := 0; i < 5; i++ {
		mustCreateAttachment(t, h, "dev-1", fmt.Sprintf("att-%d", i), content, 1)
	}

	// Default limit is 100: one page holds everything.
	if ids := listedIDs(t, mustList(t, h, "dev-1", "")); len(ids) != 5 {
		t.Fatalf("default page ids = %v, want 5 entries", ids)
	}

	// Walking the pages with limit=2 yields every entry exactly once, in
	// creation order.
	var walked []string
	for offset := 0; ; offset += 2 {
		ids := listedIDs(t, mustList(t, h, "dev-1", fmt.Sprintf("limit=2&offset=%d", offset)))
		if len(ids) == 0 {
			break
		}
		walked = append(walked, ids...)
		if offset > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	want := []string{"att-0", "att-1", "att-2", "att-3", "att-4"}
	if len(walked) != len(want) {
		t.Fatalf("walked = %v, want %v", walked, want)
	}
	for i := range want {
		if walked[i] != want[i] {
			t.Fatalf("walked = %v, want %v", walked, want)
		}
	}

	// offset beyond the end is an empty page, not an error.
	if ids := listedIDs(t, mustList(t, h, "dev-1", "offset=5")); len(ids) != 0 {
		t.Fatalf("offset=5 ids = %v, want []", ids)
	}
	// limit boundaries are accepted.
	if ids := listedIDs(t, mustList(t, h, "dev-1", "limit=1")); len(ids) != 1 || ids[0] != "att-0" {
		t.Fatalf("limit=1 ids = %v, want [att-0]", ids)
	}
	if ids := listedIDs(t, mustList(t, h, "dev-1", "limit=1000")); len(ids) != 5 {
		t.Fatalf("limit=1000 ids = %v, want 5 entries", ids)
	}
}

func TestListAttachmentsRejectsBadQuery(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("x"), 1)

	for _, q := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=abc", "limit=1.5",
		"offset=-1", "offset=abc", "offset=1.5",
	} {
		w, _ := listAttachments(t, h, "dev-1", q)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("query %q = %d, want 400", q, w.Code)
		}
		assertJSONError(t, w)
	}
	// A rejected query changes nothing: the listing is still intact.
	if ids := listedIDs(t, mustList(t, h, "dev-1", "")); len(ids) != 1 || ids[0] != "att-1" {
		t.Fatalf("listing after rejected queries = %v, want [att-1]", ids)
	}
}

func TestListAttachmentsUnknownDevice(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("x"), 1)

	w, _ := listAttachments(t, h, "ghost", "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
}

func TestListAttachmentsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifier or empty segments.
		{http.MethodGet, "/v1/devices//attachments"},
		{http.MethodGet, "/v1/devices/dev-1//attachments"},
		{http.MethodGet, "/v1/devices/dev-1/attachments/"},
		// Verbs other than GET/POST on the collection path.
		{http.MethodPut, "/v1/devices/dev-1/attachments"},
		{http.MethodPatch, "/v1/devices/dev-1/attachments"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400; body=%s", tc.method, tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("%s %s redirected to %q", tc.method, tc.path, loc)
			}
		})
	}
}

func TestListAttachmentsDeleteAndRecreate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	content := []byte("hello world")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 5)
	mustCreateAttachment(t, h, "dev-1", "att-2", content, 5)
	putChunk(t, h, "dev-1", "att-1", 0, content[:5])
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// Delete removes the record from every listing at once.
	w, body := deleteAttachment(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || body["deleted"] != true {
		t.Fatalf("delete = %d %v", w.Code, body)
	}
	for _, device := range []string{"dev-1", "dev-2"} {
		ids := listedIDs(t, mustList(t, h, device, ""))
		for _, id := range ids {
			if id == "att-1" {
				t.Fatalf("%s still lists deleted att-1: %v", device, ids)
			}
		}
	}

	// Re-creating the id is a new record: it sorts after the survivors and
	// its progress starts from zero.
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 5)
	ids := listedIDs(t, mustList(t, h, "dev-1", ""))
	if len(ids) != 2 || ids[0] != "att-2" || ids[1] != "att-1" {
		t.Fatalf("ids after recreate = %v, want [att-2 att-1]", ids)
	}
	item := mustList(t, h, "dev-1", "")[1].(map[string]any)
	if chunks := item["receivedChunks"].([]any); len(chunks) != 0 {
		t.Fatalf("recreated receivedChunks = %v, want []", chunks)
	}
	// The old grant died with the delete, so dev-2 no longer sees att-1.
	if ids := listedIDs(t, mustList(t, h, "dev-2", "")); len(ids) != 0 {
		t.Fatalf("dev-2 listing after recreate = %v, want []", ids)
	}
}

func TestListAttachmentsRevoke(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("x"), 1)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if ids := listedIDs(t, mustList(t, h, "dev-2", "")); len(ids) != 1 {
		t.Fatalf("dev-2 listing after grant = %v, want [att-1]", ids)
	}

	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if ids := listedIDs(t, mustList(t, h, "dev-2", "")); len(ids) != 0 {
		t.Fatalf("dev-2 listing after revoke = %v, want []", ids)
	}
	// The creator's listing is unaffected.
	if ids := listedIDs(t, mustList(t, h, "dev-1", "")); len(ids) != 1 || ids[0] != "att-1" {
		t.Fatalf("dev-1 listing after revoke = %v, want [att-1]", ids)
	}
}

func TestListAttachmentsIsReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	content := []byte("readonly!")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	putChunk(t, h, "dev-1", "att-1", 0, content[:4])
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	before := mustList(t, h, "dev-1", "")
	// Repeated reads, including the grantee's, change nothing.
	mustList(t, h, "dev-1", "")
	mustList(t, h, "dev-2", "limit=10&offset=0")
	after := mustList(t, h, "dev-1", "")

	b, _ := json.Marshal(before)
	a, _ := json.Marshal(after)
	if string(b) != string(a) {
		t.Fatalf("listing changed across reads:\nbefore=%s\nafter=%s", b, a)
	}
	// The single-attachment view is untouched as well.
	w := getAttachmentMeta(t, h, "dev-1", "att-1")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"receivedChunks":[0]`) {
		t.Fatalf("meta after listings = %d %s", w.Code, w.Body.String())
	}
}

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
	content := []byte("restart-safe")
	mustCreateAttachment(t, h, "dev-1", "att-1", content, 4)
	mustCreateAttachment(t, h, "dev-2", "att-2", content, 4)
	putChunk(t, h, "dev-1", "att-1", 2, content[8:])
	putChunk(t, h, "dev-1", "att-1", 0, content[:4])
	setAccess(t, h, "dev-2", "att-2", "dev-1", "grant")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments?limit=1&offset=1", nil)
	before := serveRecorder(h, r).Body.String()
	full := serveRecorder(h, httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments", nil)).Body.String()
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()

	after := serveRecorder(h, httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments?limit=1&offset=1", nil)).Body.String()
	if after != before {
		t.Fatalf("page differs across restart:\nbefore=%s\nafter=%s", before, after)
	}
	if fullAfter := serveRecorder(h, httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments", nil)).Body.String(); fullAfter != full {
		t.Fatalf("listing differs across restart:\nbefore=%s\nafter=%s", full, fullAfter)
	}
}
