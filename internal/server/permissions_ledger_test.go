package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// getPermissionLedger GETs a document's permission ledger and decodes the
// permissions array when the status is 200.
func getPermissionLedger(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	var body struct {
		Permissions []map[string]any `json:"permissions"`
		Count       int              `json:"count"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("ledger body is not JSON: %v body=%s", err, w.Body.String())
		}
		if body.Permissions == nil {
			t.Fatalf("permissions is null, want a (possibly empty) array: %s", w.Body.String())
		}
		if body.Count != len(body.Permissions) {
			t.Fatalf("count = %d, want %d (the page length): %s", body.Count, len(body.Permissions), w.Body.String())
		}
	}
	return w, body.Permissions
}

// ledgerPairs renders a ledger page as deviceId/authorized pairs, in order.
func ledgerPairs(entries []map[string]any) []string {
	pairs := make([]string, 0, len(entries))
	for _, e := range entries {
		pairs = append(pairs, fmt.Sprintf("%s:%t", e["deviceId"], e["authorized"]))
	}
	return pairs
}

// The ledger lists every registered device once with its current verdict,
// sorted by device id: untouched devices show authorized=true, a revoked one
// false. The body is one compact JSON line ending in a newline with the keys
// permissions then count.
func TestListPermissionsLedgerContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-3")

	// An idempotent grant on the default writes nothing and must not add or
	// duplicate an entry.
	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1", "action": "grant",
	})
	if w.Code != http.StatusOK || w.Body == nil {
		t.Fatalf("default grant = %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke = %d %s", w.Code, w.Body.String())
	}

	w, entries := getPermissionLedger(t, h, "/v1/documents/doc1/permissions")
	if w.Code != http.StatusOK {
		t.Fatalf("ledger = %d body=%s", w.Code, w.Body.String())
	}
	if got := ledgerPairs(entries); !equalStrings(got, []string{
		"dev-1:true", "dev-2:false", "dev-3:true",
	}) {
		t.Fatalf("ledger = %v", got)
	}
	want := `{"permissions":[{"deviceId":"dev-1","authorized":true},{"deviceId":"dev-2","authorized":false},{"deviceId":"dev-3","authorized":true}],"count":3}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("ledger body = %q, want %q", w.Body.String(), want)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}

	// Re-granting flips the verdict in place; the device is still listed once.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-2", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("re-grant = %d", w.Code)
	}
	_, entries = getPermissionLedger(t, h, "/v1/documents/doc1/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{
		"dev-1:true", "dev-2:true", "dev-3:true",
	}) {
		t.Fatalf("ledger after re-grant = %v", got)
	}

	// The ledger is per document: a revoke on another document does not move
	// this one.
	w, _ = postJSON(t, h, "/v1/documents/other/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("other revoke = %d", w.Code)
	}
	_, entries = getPermissionLedger(t, h, "/v1/documents/doc1/permissions")
	if got := ledgerPairs(entries); got[1] != "dev-2:true" {
		t.Fatalf("per-document isolation violated: %v", got)
	}
}

// Ordering is bytewise lexicographic, not numeric: dev-10 precedes dev-2.
func TestListPermissionsLedgerLexicographic(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, id := range []string{"dev-2", "dev-10", "dev-1"} {
		registerDevice(t, h, id)
	}
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	_, entries := getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{
		"dev-1:true", "dev-10:true", "dev-2:false",
	}) {
		t.Fatalf("lexicographic ledger = %v", got)
	}
}

// An unknown document is not an error: every registered device shows its
// default authorized verdict. With no registered devices the body is an empty
// array and count zero, still a 200.
func TestListPermissionsUnknownDocumentAndEmptyLedger(t *testing.T) {
	h, _ := newTestHandler(t)

	w := serveRecorder(h, httptest.NewRequest(http.MethodGet, "/v1/documents/ghost/permissions", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("empty ledger = %d body=%s", w.Code, w.Body.String())
	}
	if w.Body.String() != `{"permissions":[],"count":0}`+"\n" {
		t.Fatalf("empty ledger body = %q", w.Body.String())
	}

	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	w, entries := getPermissionLedger(t, h, "/v1/documents/ghost/permissions")
	if w.Code != http.StatusOK {
		t.Fatalf("unknown document = %d body=%s", w.Code, w.Body.String())
	}
	if got := ledgerPairs(entries); !equalStrings(got, []string{"dev-1:true", "dev-2:true"}) {
		t.Fatalf("unknown document ledger = %v", got)
	}
}

// Pagination shares the attachment listing's limit/offset semantics and slices
// the one lexicographic ordering, so pages neither repeat nor skip an entry.
func TestListPermissionsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, id := range []string{"dev-5", "dev-2", "dev-4", "dev-1", "dev-3"} {
		registerDevice(t, h, id)
	}

	// Default limit is 100: one page holds everything, in id order.
	_, entries := getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{
		"dev-1:true", "dev-2:true", "dev-3:true", "dev-4:true", "dev-5:true",
	}) {
		t.Fatalf("default page = %v", got)
	}

	// Walk in pages of two: no duplicates, no gaps (the last page is partial).
	var seen []string
	pages := []struct {
		offset  int
		wantLen int
	}{{0, 2}, {2, 2}, {4, 1}}
	for _, pg := range pages {
		w, page := getPermissionLedger(t, h, fmt.Sprintf("/v1/documents/doc/permissions?limit=2&offset=%d", pg.offset))
		if w.Code != http.StatusOK {
			t.Fatalf("page at offset %d = %d", pg.offset, w.Code)
		}
		if len(page) != pg.wantLen {
			t.Fatalf("page at offset %d has %d entries, want %d", pg.offset, len(page), pg.wantLen)
		}
		seen = append(seen, ledgerPairs(page)...)
	}
	if !equalStrings(seen, []string{
		"dev-1:true", "dev-2:true", "dev-3:true", "dev-4:true", "dev-5:true",
	}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// A page past the end.
	w, body := getPermissionLedger(t, h, "/v1/documents/doc/permissions?offset=5")
	if w.Code != http.StatusOK || len(body) != 0 || !strings.Contains(w.Body.String(), `"permissions":[]`) {
		t.Fatalf("page past end = %d %s", w.Code, w.Body.String())
	}

	// limit=1 and limit=1000 are both accepted bounds.
	if w, _ := getPermissionLedger(t, h, "/v1/documents/doc/permissions?limit=1"); w.Code != http.StatusOK {
		t.Fatalf("limit=1 = %d", w.Code)
	}
	if w, _ := getPermissionLedger(t, h, "/v1/documents/doc/permissions?limit=1000"); w.Code != http.StatusOK {
		t.Fatalf("limit=1000 = %d", w.Code)
	}
}

// Illegal pagination parameters are a 400 JSON error, checked before anything
// else — even with no devices registered or an empty document id the verdict
// is the same 400 — and change nothing.
func TestListPermissionsRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/documents/doc/permissions?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Shape and parameter validation do not depend on any device or document.
	r := httptest.NewRequest(http.MethodGet, "/v1/documents//permissions?limit=0", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("empty doc with bad limit = %d, want 400", w.Code)
	}

	// None of the rejections changed the ledger.
	_, entries := getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{"dev-1:false"}) {
		t.Fatalf("ledger after rejections = %v", got)
	}
}

// Empty identifiers, a missing segment, a trailing slash, extra segments and
// verbs other than GET/POST on the permission path are all 400 JSON errors —
// never a redirect, 404 or HTML — and change nothing.
func TestListPermissionsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	cases := []struct {
		method string
		path   string
	}{
		// Empty document identifier and trailing slashes.
		{http.MethodGet, "/v1/documents//permissions"},
		{http.MethodGet, "/v1/documents/doc1/permissions/"},
		// A missing documentID segment and segments past the resource: 400, not
		// 404, for every verb.
		{http.MethodGet, "/v1/documents/permissions"},
		{http.MethodGet, "/v1/documents/permissions/"},
		{http.MethodGet, "/v1/documents/doc1/permissions/extra"},
		{http.MethodGet, "/v1/documents/doc1/permissions/extra/more"},
		{http.MethodPost, "/v1/documents/doc1/permissions/extra"},
		{http.MethodDelete, "/v1/documents/doc1/permissions/extra"},
		// Verbs other than GET (ledger) and POST (grant/revoke) on the exact
		// path.
		{http.MethodPut, "/v1/documents/doc1/permissions"},
		{http.MethodDelete, "/v1/documents/doc1/permissions"},
		{http.MethodPatch, "/v1/documents/doc1/permissions"},
		{http.MethodOptions, "/v1/documents/doc1/permissions"},
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
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
			t.Fatalf("%s %s emitted HTML: %s", tc.method, tc.path, w.Body.String())
		}
	}

	// Zero writes: the ledger still shows the one revoked entry.
	_, entries := getPermissionLedger(t, h, "/v1/documents/doc1/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{"dev-1:false"}) {
		t.Fatalf("ledger after rejected shapes = %v", got)
	}

	// A document literally named "permissions" keeps its ordinary routes.
	w, _ = postJSON(t, h, "/v1/documents/permissions/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("changes on document named permissions = %d %s", w.Code, w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/permissions/changes")
	if w.Code != http.StatusOK || len(body["changes"].([]any)) != 1 {
		t.Fatalf("document named permissions changes read = %d %v", w.Code, body)
	}
}

// A deregistered device vanishes from the ledger at once, and the same id
// registering again starts fresh with the default authorized verdict.
func TestListPermissionsDeregisteredDevice(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	r := httptest.NewRequest(http.MethodDelete, "/v1/devices/dev-1", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusOK {
		t.Fatalf("deregister = %d body=%s", w.Code, w.Body.String())
	}

	_, entries := getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{"dev-2:true"}) {
		t.Fatalf("ledger after deregister = %v", got)
	}

	// The freed id re-registers as a brand-new device: default authorized.
	registerDevice(t, h, "dev-1")
	_, entries = getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if got := ledgerPairs(entries); !equalStrings(got, []string{"dev-1:true", "dev-2:true"}) {
		t.Fatalf("ledger after re-register = %v", got)
	}
	// The inherited state is gone: the first revoke reports a real change.
	w, body := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("revoke after re-register = %d %v", w.Code, body)
	}
}

// A revoked session still reads 403 while the ledger openly reports the
// revocation; the query is not itself a gated read.
func TestListPermissionsAlongsideRevokedSession(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// The session view is gated as before.
	w, body := doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/doc/changes")
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("session read = %d %v, want 403", w.Code, body)
	}

	// The ledger query is unaffected and still succeeds.
	w2, entries := getPermissionLedger(t, h, "/v1/documents/doc/permissions")
	if w2.Code != http.StatusOK || ledgerPairs(entries)[0] != "dev-1:false" {
		t.Fatalf("ledger while revoked = %d %v", w2.Code, entries)
	}
}

// The ledger and its ordering are durable: after a restart every page body is
// byte-for-byte what it was before.
func TestListPermissionsSurvivesRestart(t *testing.T) {
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
	for _, id := range []string{"dev-3", "dev-1", "dev-2"} {
		registerDevice(t, h, id)
	}
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	snapshot := func(t *testing.T, h http.Handler) map[string]string {
		t.Helper()
		bodies := map[string]string{}
		for _, url := range []string{
			"/v1/documents/doc/permissions",
			"/v1/documents/doc/permissions?limit=1&offset=0",
			"/v1/documents/doc/permissions?limit=1&offset=1",
			"/v1/documents/doc/permissions?limit=1&offset=2",
			"/v1/documents/ghost/permissions",
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

// The ledger query is read-only with respect to both push channels and the
// change cursor: querying it while a subscription is live pushes no frame, and
// the change log's high-water cursor does not move. The revocation-driven 4403
// close still happens afterwards exactly as without the query.
func TestListPermissionsDoesNotPushOrConsumeCursor(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("upgrade = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.Cursor != 1 {
		t.Fatalf("seed frame = %+v", c)
	}

	// Issue the ledger query several times while the subscription is parked.
	for i := 0; i < 3; i++ {
		code, body := httpGet(t, srv, "/v1/documents/doc/permissions")
		if code != http.StatusOK || body["count"] == nil {
			t.Fatalf("ledger %d = %d %v", i, code, body)
		}
	}

	// No push frame is produced by the reads.
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("the ledger query pushed a frame to the subscription")
	}
	conn.clearReadDeadline()

	// The change cursor is untouched: the log's high-water mark is still 1 and
	// the next post takes cursor 2.
	code, body := httpGet(t, srv, "/v1/documents/doc/changes")
	if code != http.StatusOK || body["nextCursor"].(float64) != 1 {
		t.Fatalf("cursor after ledger reads = %d nextCursor=%v", code, body["nextCursor"])
	}
	postDocChange(t, srv, "dev-1", "doc", "after-ledger", map[string]any{"x": 1})
	if c := conn.readChange(); c.Cursor != 2 || c.ID != "after-ledger" {
		t.Fatalf("post-query frame = %+v, want after-ledger/2", c)
	}

	// The query did not pre-trigger the revocation verdict: the live
	// subscription still ends with 4403 on the first revoke.
	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}
	conn.clearReadDeadline()
}
