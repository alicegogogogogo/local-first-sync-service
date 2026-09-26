package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// getPermissionsLedger GETs a document's permission ledger and decodes the
// permissions array together with count when the status is 200.
func getPermissionsLedger(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []map[string]any, int) {
	t.Helper()
	w, body := doRequest(t, h, http.MethodGet, url)
	var entries []map[string]any
	count := -1
	if w.Code == http.StatusOK {
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("content type = %q", ct)
		}
		raw, ok := body["permissions"].([]any)
		if !ok {
			t.Fatalf("permissions is not an array: %s", w.Body.String())
		}
		for _, item := range raw {
			e, ok := item.(map[string]any)
			if !ok {
				t.Fatalf("entry is not an object: %v", item)
			}
			entries = append(entries, e)
		}
		count = int(body["count"].(float64))
	}
	return w, entries, count
}

// The ledger reports every registered device once, in device-id ascending
// order, with its current authorization: default true, false after a revoke,
// true again after a grant. The body is one compact line with keys ordered
// permissions,count and item keys ordered deviceId,authorized, ending in one
// newline.
func TestPermissionLedgerHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-c")
	registerDevice(t, h, "dev-a")
	registerDevice(t, h, "dev-b")

	// Defaults: all authorized even with no permission rows.
	w, entries, count := getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if w.Code != http.StatusOK || count != 3 {
		t.Fatalf("default ledger = %d count=%d body=%s", w.Code, count, w.Body.String())
	}
	want := `{"permissions":[` +
		`{"deviceId":"dev-a","authorized":true},` +
		`{"deviceId":"dev-b","authorized":true},` +
		`{"deviceId":"dev-c","authorized":true}` +
		`],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// Revoke the middle device; a repeat revoke and a grant on another device
	// move only what they should.
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-b", "action": "revoke"})
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-b", "action": "revoke"})
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-a", "action": "grant"})
	w, entries, count = getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if w.Code != http.StatusOK || count != 3 || len(entries) != 3 {
		t.Fatalf("ledger after revoke = %d %d %v", w.Code, count, entries)
	}
	if entries[0]["deviceId"] != "dev-a" || entries[0]["authorized"] != true {
		t.Fatalf("entry 0 = %v", entries[0])
	}
	if entries[1]["deviceId"] != "dev-b" || entries[1]["authorized"] != false {
		t.Fatalf("entry 1 = %v", entries[1])
	}
	if entries[2]["deviceId"] != "dev-c" || entries[2]["authorized"] != true {
		t.Fatalf("entry 2 = %v", entries[2])
	}

	// A revoke on another document never leaks into this ledger.
	postJSON(t, h, "/v1/documents/other/permissions", map[string]any{"deviceId": "dev-a", "action": "revoke"})
	_, entries, _ = getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if entries[0]["authorized"] != true {
		t.Fatalf("per-document revoke leaked: %v", entries)
	}
}

// No registered devices, or an unknown document: a 200 with an empty
// non-null array and zero count — not an error. A newly registered device
// appears authorized on every document at once.
func TestPermissionLedgerHTTPEmptyAndUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)

	for _, url := range []string{
		"/v1/documents/doc/permissions",
		"/v1/documents/ghost/permissions",
	} {
		w, _ := doRequest(t, h, http.MethodGet, url)
		if w.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", url, w.Code)
		}
		if got := w.Body.String(); got != `{"permissions":[],"count":0}`+"\n" {
			t.Fatalf("%s body = %q", url, got)
		}
	}

	registerDevice(t, h, "dev-1")
	w, entries, count := getPermissionsLedger(t, h, "/v1/documents/ghost/permissions")
	if w.Code != http.StatusOK || count != 1 || len(entries) != 1 {
		t.Fatalf("unknown doc with device = %d %d %v", w.Code, count, entries)
	}
	if entries[0]["deviceId"] != "dev-1" || entries[0]["authorized"] != true {
		t.Fatalf("entry = %v", entries[0])
	}
}

// limit/offset page the stable lexicographic order with the attachment
// listing's exact contract, including defaults and bounds.
func TestPermissionLedgerHTTPPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, id := range []string{"d0", "d1", "d2", "d3", "d4"} {
		registerDevice(t, h, id)
	}
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "d2", "action": "revoke"})

	// Default limit 100: one page holds everything in id order.
	_, entries, count := getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if count != 5 {
		t.Fatalf("default count = %d", count)
	}
	for i, e := range entries {
		if e["deviceId"] != fmt.Sprintf("d%d", i) {
			t.Fatalf("default order = %v", entries)
		}
	}

	// Walk the ledger in pages of two: no duplicate, no gap, flags travel
	// with their device.
	var seen []map[string]any
	for offset := 0; offset < 5; offset += 2 {
		_, page, pageCount := getPermissionsLedger(t, h,
			fmt.Sprintf("/v1/documents/doc/permissions?limit=2&offset=%d", offset))
		if pageCount != len(page) {
			t.Fatalf("count %d != page len %d", pageCount, len(page))
		}
		seen = append(seen, page...)
	}
	if len(seen) != 5 {
		t.Fatalf("paged walk = %v", seen)
	}
	for i, e := range seen {
		if e["deviceId"] != fmt.Sprintf("d%d", i) {
			t.Fatalf("paged order = %v", seen)
		}
	}
	if seen[2]["authorized"] != false {
		t.Fatalf("revoked flag lost across the page boundary: %v", seen[2])
	}

	// A partial last page and a page past the end.
	w, page, _ := getPermissionsLedger(t, h, "/v1/documents/doc/permissions?limit=4&offset=3")
	if w.Code != http.StatusOK || len(page) != 2 || page[0]["deviceId"] != "d3" {
		t.Fatalf("last partial page = %d %v", w.Code, page)
	}
	w, page, count = getPermissionsLedger(t, h, "/v1/documents/doc/permissions?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 || count != 0 {
		t.Fatalf("page past end = %d %v count=%d", w.Code, page, count)
	}

	// Bounds and explicit-empty parameters match the attachment listing.
	for _, query := range []string{"limit=1", "limit=1000", "limit=", "offset=", "limit=&offset="} {
		u := "/v1/documents/doc/permissions?" + query
		if rec := serveRecorder(h, httptest.NewRequest(http.MethodGet, u, nil)); rec.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", u, rec.Code)
		}
	}
}

// Illegal pagination parameters are a 400 JSON error checked before anything
// else — including on an unknown document — and write nothing.
func TestPermissionLedgerHTTPRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5", "limit=1e3",
		"offset=-1", "offset=x", "offset=1.5",
		"limit=0&offset=0", "limit=2&offset=-2",
	} {
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/permissions?"+query)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400, body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		if body["permissions"] != nil {
			t.Fatalf("?%s leaked a ledger: %v", query, body)
		}
	}

	// Shape errors win even on a document that does not exist.
	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/ghost/permissions?limit=0")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad params on unknown doc = %d, want 400", w.Code)
	}

	// Zero writes: the revoked state is intact and count is still one.
	_, entries, count := getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if count != 1 || entries[0]["authorized"] != false {
		t.Fatalf("ledger after rejected params = %v %d", entries, count)
	}
}

// Empty identifiers, missing or extra segments and verbs other than GET are
// all 400 JSON errors — never a redirect or HTML — and change nothing. The
// POST on the exact path stays the existing grant/revoke entry.
func TestPermissionLedgerHTTPShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})

	cases := []struct {
		method string
		path   string
	}{
		// Empty document id.
		{http.MethodGet, "/v1/documents//permissions"},
		// Trailing slash, extra segments, doubled slashes in the tail.
		{http.MethodGet, "/v1/documents/doc/permissions/"},
		{http.MethodGet, "/v1/documents/doc/permissions/extra"},
		{http.MethodGet, "/v1/documents/doc/permissions/extra/more"},
		{http.MethodGet, "/v1/documents/doc/permissions//extra"},
		{http.MethodGet, "/v1/documents/doc/permissions/extra//"},
		// A stray segment after POST too.
		{http.MethodPost, "/v1/documents/doc/permissions/extra"},
		// Verbs other than GET/POST on the exact path.
		{http.MethodPut, "/v1/documents/doc/permissions"},
		{http.MethodDelete, "/v1/documents/doc/permissions"},
		{http.MethodPatch, "/v1/documents/doc/permissions"},
		{http.MethodOptions, "/v1/documents/doc/permissions"},
		{http.MethodDelete, "/v1/documents/doc/permissions/"},
	}
	for _, tc := range cases {
		r := newJSONRequest(tc.method, tc.path, `{"deviceId":"dev-1","action":"grant"}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, body = %q", tc.method, tc.path, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") ||
			strings.Contains(w.Body.String(), "Method Not Allowed") {
			t.Fatalf("%s %s leaked a non-JSON body: %q", tc.method, tc.path, w.Body.String())
		}
	}

	// Zero writes: dev-1 is still revoked, and the write path still drives
	// changes with its existing idempotency.
	_, entries, _ := getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if len(entries) != 1 || entries[0]["authorized"] != false {
		t.Fatalf("ledger after rejected shapes = %v", entries)
	}
	w, body := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("idempotent revoke after shape rejects = %d %v", w.Code, body)
	}
	w, body = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-1", "action": "grant"})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("grant after shape rejects = %d %v", w.Code, body)
	}
}

// A deregistered device disappears from every document's ledger; the same id
// re-registered later is a brand-new device that starts authorized.
func TestPermissionLedgerHTTPDeregistration(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-a")
	registerDevice(t, h, "dev-b")
	registerDevice(t, h, "dev-c")
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-b", "action": "revoke"})
	postJSON(t, h, "/v1/documents/other/permissions", map[string]any{"deviceId": "dev-b", "action": "revoke"})

	w := serveRecorder(h, newJSONRequest(http.MethodDelete, "/v1/devices/dev-b", "", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("deregister = %d %s", w.Code, w.Body.String())
	}
	for _, doc := range []string{"doc", "other"} {
		_, entries, count := getPermissionsLedger(t, h, "/v1/documents/"+doc+"/permissions")
		if count != 2 {
			t.Fatalf("doc %s ledger = %v", doc, entries)
		}
		for _, e := range entries {
			if e["deviceId"] == "dev-b" {
				t.Fatalf("deregistered device still listed for %s: %v", doc, entries)
			}
		}
	}

	// Re-register: back in the ledger as authorized, on every document.
	registerDevice(t, h, "dev-b")
	_, entries, _ := getPermissionsLedger(t, h, "/v1/documents/doc/permissions")
	if len(entries) != 3 {
		t.Fatalf("ledger after re-register = %v", entries)
	}
	for _, e := range entries {
		if e["deviceId"] == "dev-b" && e["authorized"] != true {
			t.Fatalf("re-registered device inherited a revoke: %v", e)
		}
	}
}

// Ledger content and order are durable: the same paged request returns the
// same bytes after a process restart.
func TestPermissionLedgerHTTPRestartPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "permissions-ledger.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	for _, id := range []string{"dev-a", "dev-b", "dev-c"} {
		registerDevice(t, h, id)
	}
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-a", "action": "revoke"})
	postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-c", "action": "revoke"})

	snapshot := func(t *testing.T, h http.Handler) map[string]string {
		t.Helper()
		bodies := map[string]string{}
		for _, query := range []string{"", "?limit=2&offset=0", "?limit=2&offset=1", "?offset=3"} {
			w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/permissions"+query)
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s = %d", query, w.Code)
			}
			bodies[query] = w.Body.String()
		}
		return bodies
	}

	before := snapshot(t, h)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	after := snapshot(t, NewHandler(s2))
	for query, body := range before {
		if after[query] != body {
			t.Fatalf("GET %s after restart = %q, want %q", query, after[query], body)
		}
	}
}

// The ledger is a read-only view: reading it neither consumes a change cursor
// nor pushes a frame to a live subscription. The push channel still works —
// a real commit afterwards is delivered — proving the silence is the read's,
// not a dead subscription.
func TestPermissionLedgerHTTPReadOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "seed", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Assertion 1: the read does not occupy the change cursor. Repeated reads
	// leave the document's high-water cursor exactly where the seed put it.
	for i := 0; i < 3; i++ {
		if rw, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/permissions"); rw.Code != http.StatusOK {
			t.Fatalf("ledger read %d = %d", i, rw.Code)
		}
	}
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if list["nextCursor"].(float64) != 1 {
		t.Fatalf("nextCursor = %v, want 1: the ledger read moved the cursor", list["nextCursor"])
	}

	// Assertion 2: the read pushes nothing over a live subscription. Run the
	// subscription over a real TCP listener (hijacking does not work with the
	// in-process recorder).
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("subscribe = %d", resp.StatusCode)
	}
	defer conn.close()

	// Caught up at cursor 1: nothing is due until a commit lands.
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived before the ledger read")
	}
	conn.clearReadDeadline()

	respGet, err := srv.Client().Get(srv.URL + "/v1/documents/doc/permissions")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(respGet.Body)
	_ = respGet.Body.Close()
	if respGet.StatusCode != http.StatusOK {
		t.Fatalf("ledger over TCP = %d %s", respGet.StatusCode, raw)
	}
	var ledger struct {
		Permissions []struct {
			DeviceID   string `json:"deviceId"`
			Authorized bool   `json:"authorized"`
		} `json:"permissions"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(raw, &ledger); err != nil || ledger.Count != 1 {
		t.Fatalf("ledger body = %s err = %v", raw, err)
	}

	conn.setReadDeadline(500 * time.Millisecond)
	if _, opcode, payload, ok := conn.readFrameMaybe(); ok {
		t.Fatalf("the ledger read pushed a frame (opcode %d): %s", opcode, payload)
	}
	conn.clearReadDeadline()

	// The subscription is alive: the next genuine commit is delivered at once.
	respPost, err := srv.Client().Post(srv.URL+"/v1/documents/doc/changes",
		"application/json", strings.NewReader(`{"deviceId":"dev-1","changes":[{"id":"live","payload":{"n":2}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = respPost.Body.Close()
	frame := conn.readChange()
	if frame.Cursor != 2 || frame.ID != "live" {
		t.Fatalf("live frame after ledger read = %+v", frame)
	}
}
