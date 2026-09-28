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

// listSessions GETs the device session collection and decodes the sessions
// array plus the count when the status is 200.
func listSessions(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []string, int) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		return w, nil, 0
	}
	var body struct {
		Sessions []struct {
			SessionID string `json:"sessionId"`
		} `json:"sessions"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("list body is not JSON: %v body=%s", err, w.Body.String())
	}
	ids := make([]string, 0, len(body.Sessions))
	for _, e := range body.Sessions {
		ids = append(ids, e.SessionID)
	}
	return w, ids, body.Count
}

// createSessions is a small helper that creates one session per id.
func createSessions(t *testing.T, h http.Handler, device string, ids ...string) {
	t.Helper()
	for _, id := range ids {
		w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": id})
		if w.Code != http.StatusOK {
			t.Fatalf("create session %q = %d %s", id, w.Code, w.Body.String())
		}
	}
}

// A registered device's sessions are listed, each once, sorted by id,
// regardless of creation order; other devices' sessions never appear.
func TestListSessionsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-c", "sess-a", "sess-b", "sess-a2")
	createSessions(t, h, "dev-2", "sess-foreign")

	w, ids, count := listSessions(t, h, "/v1/devices/dev-1/sessions")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", w.Code, w.Body.String())
	}
	want := []string{"sess-a", "sess-a2", "sess-b", "sess-c"}
	if !equalStrings(ids, want) {
		t.Fatalf("order = %v, want %v", ids, want)
	}
	if count != len(ids) {
		t.Fatalf("count = %d, want %d", count, len(ids))
	}

	// Each item carries only the sessionId key.
	var body struct {
		Sessions []map[string]json.RawMessage `json:"sessions"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, item := range body.Sessions {
		if len(item) != 1 {
			t.Fatalf("item %v must have exactly one key", item)
		}
		if _, ok := item["sessionId"]; !ok {
			t.Fatalf("item %v must carry sessionId", item)
		}
	}

	// The other device sees only its own session.
	_, ids, _ = listSessions(t, h, "/v1/devices/dev-2/sessions")
	if !equalStrings(ids, []string{"sess-foreign"}) {
		t.Fatalf("dev-2 list = %v", ids)
	}

	// A registered device with no sessions gets an empty array and zero.
	registerDevice(t, h, "dev-3")
	w, ids, count = listSessions(t, h, "/v1/devices/dev-3/sessions")
	if w.Code != http.StatusOK || len(ids) != 0 || count != 0 {
		t.Fatalf("empty list = %d %v count=%d", w.Code, ids, count)
	}
	if w.Body.String() != `{"sessions":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q, want the fixed compact line", w.Body.String())
	}
}

// Byte-level shape: one compact JSON line plus a trailing newline, top-level
// keys in the sessions-then-count order.
func TestListSessionsBodyShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "s1", "s2")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/sessions", nil)
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
	want := `{"sessions":[{"sessionId":"s1"},{"sessionId":"s2"}],"count":2}` + "\n"
	if body != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}

// limit/offset page the fixed order without repeats or gaps; defaults and the
// 1..1000 bounds behave like the attachment listing.
func TestListSessionsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "s0", "s1", "s2", "s3", "s4")

	// Default limit is 100: one page holds everything in id order.
	_, ids, _ := listSessions(t, h, "/v1/devices/dev/sessions")
	if !equalStrings(ids, []string{"s0", "s1", "s2", "s3", "s4"}) {
		t.Fatalf("default page = %v", ids)
	}

	// Walk the list in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		_, page, count := listSessions(t, h, fmt.Sprintf("/v1/devices/dev/sessions?limit=2&offset=%d", offset))
		if count != len(page) {
			t.Fatalf("page at offset %d count = %d, len = %d", offset, count, len(page))
		}
		seen = append(seen, page...)
	}
	if !equalStrings(seen, []string{"s0", "s1", "s2", "s3", "s4"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// Partial last page and a page past the end.
	_, page, _ := listSessions(t, h, "/v1/devices/dev/sessions?limit=4&offset=3")
	if !equalStrings(page, []string{"s3", "s4"}) {
		t.Fatalf("last partial page = %v", page)
	}
	w, page, count := listSessions(t, h, "/v1/devices/dev/sessions?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 || count != 0 {
		t.Fatalf("page past end = %d %v count=%d", w.Code, page, count)
	}

	// The accepted bounds.
	for _, query := range []string{"limit=1", "limit=1000", "offset=0"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/sessions?"+query, nil)
		if rec := serveRecorder(h, r); rec.Code != http.StatusOK {
			t.Fatalf("?%s = %d", query, rec.Code)
		}
	}
}

// Illegal pagination values are a 400 JSON error checked before the device
// lookup; an unregistered device is a 404 JSON error with no listing content.
func TestListSessionsRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess-1")

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/sessions?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Unregistered device: 404 JSON with no listing content.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/sessions", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d, want 404", w.Code)
	}
	assertJSONError(t, w)
	if strings.Contains(w.Body.String(), "sess-1") || strings.Contains(w.Body.String(), "sessions") {
		t.Fatalf("404 body leaks listing: %s", w.Body.String())
	}

	// Shape errors win over the device lookup: 400 even for a ghost device.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/sessions?limit=0", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", rec.Code)
	}

	// None of the rejections changed the listing.
	_, ids, _ := listSessions(t, h, "/v1/devices/dev-1/sessions")
	if !equalStrings(ids, []string{"sess-1"}) {
		t.Fatalf("list after rejections = %v", ids)
	}
}

// Empty identifiers, trailing slashes, extra segments and verbs other than
// GET/POST on the collection path are all 400 JSON errors — never a redirect
// or HTML — and change nothing.
func TestListSessionsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSessions(t, h, "dev-1", "sess-1")

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"empty device segment", http.MethodGet, "/v1/devices//sessions"},
		{"trailing slash on collection", http.MethodGet, "/v1/devices/dev-1/sessions/"},
		{"extra segment on get", http.MethodGet, "/v1/devices/dev-1/sessions/sess-1"},
		{"two extra segments on get", http.MethodGet, "/v1/devices/dev-1/sessions/sess-1/extra"},
		{"put on the collection", http.MethodPut, "/v1/devices/dev-1/sessions"},
		{"patch on the collection", http.MethodPatch, "/v1/devices/dev-1/sessions"},
		{"delete short of the session id", http.MethodDelete, "/v1/devices/dev-1/sessions"},
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
	_, ids, _ := listSessions(t, h, "/v1/devices/dev-1/sessions")
	if !equalStrings(ids, []string{"sess-1"}) {
		t.Fatalf("list after rejected shapes = %v", ids)
	}
}

// A GET with an unexpected body still answers the read-only listing and
// creates nothing.
func TestListSessionsIgnoresBody(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	createSessions(t, h, "dev", "sess")

	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev/sessions", strings.NewReader(`{"sessionId":"ignored"}`))
	r.Header.Set("Content-Type", "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusOK {
		t.Fatalf("GET with body = %d %s", w.Code, w.Body.String())
	}
	_, ids, count := listSessions(t, h, "/v1/devices/dev/sessions")
	if count != 1 || !equalStrings(ids, []string{"sess"}) {
		t.Fatalf("listing changed after a GET: %v", ids)
	}
}

// A deleted session vanishes from the listing; deregistering the device
// removes every session it owns and turns its listing into the same 404 a
// never-registered device gets. Other devices' sessions survive both.
func TestListSessionsReflectsDeleteAndDeregister(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSessions(t, h, "dev-1", "sess-a", "sess-b")
	createSessions(t, h, "dev-2", "sess-c")

	// Owner delete removes just that session.
	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess-a")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, ids, _ := listSessions(t, h, "/v1/devices/dev-1/sessions")
	if !equalStrings(ids, []string{"sess-b"}) {
		t.Fatalf("list after session delete = %v", ids)
	}

	// Re-creating the deleted id lists it again, sorted by id.
	createSessions(t, h, "dev-1", "sess-a")
	_, ids, _ = listSessions(t, h, "/v1/devices/dev-1/sessions")
	if !equalStrings(ids, []string{"sess-a", "sess-b"}) {
		t.Fatalf("list after recreate = %v", ids)
	}

	// Deregister dev-1: its whole listing is gone (404), and dev-2 is
	// untouched.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/sessions", nil)
	if rec := serveRecorder(h, r); rec.Code != http.StatusNotFound {
		t.Fatalf("list after deregister = %d, want 404", rec.Code)
	}
	_, ids, _ = listSessions(t, h, "/v1/devices/dev-2/sessions")
	if !equalStrings(ids, []string{"sess-c"}) {
		t.Fatalf("other device after deregister = %v", ids)
	}

	// A re-registered same-named device starts with an empty listing.
	registerDevice(t, h, "dev-1")
	w, ids, count := listSessions(t, h, "/v1/devices/dev-1/sessions")
	if w.Code != http.StatusOK || count != 0 || len(ids) != 0 {
		t.Fatalf("re-registered device list = %d %v count=%d", w.Code, ids, count)
	}
}

// The keyword "sessions" as a device id is an ordinary identifier; the word
// is an endpoint segment only in its fixed position.
func TestListSessionsKeywordAsDeviceID(t *testing.T) {
	h, _ := newTestHandler(t)

	// Unregistered device literally named "sessions": same 404 as any other
	// unregistered id, reached on the ordinary collection route.
	r := httptest.NewRequest(http.MethodGet, "/v1/devices/sessions/sessions", nil)
	w := serveRecorder(h, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("device id 'sessions' = %d, want 404", w.Code)
	}
	assertJSONError(t, w)

	// Once registered it behaves like every other device: creates and lists.
	registerDevice(t, h, "sessions")
	createSessions(t, h, "sessions", "sess")
	_, ids, count := listSessions(t, h, "/v1/devices/sessions/sessions")
	if count != 1 || !equalStrings(ids, []string{"sess"}) {
		t.Fatalf("device id 'sessions' list = %v count=%d", ids, count)
	}

	// And a session id literally named "sessions" is an ordinary id too.
	createSessions(t, h, "sessions", "sessions")
	_, ids, _ = listSessions(t, h, "/v1/devices/sessions/sessions")
	if !equalStrings(ids, []string{"sess", "sessions"}) {
		t.Fatalf("session id 'sessions' list = %v", ids)
	}
}

// Repeated reads and reads across a process restart return byte-identical
// bodies; the read changes nothing.
func TestListSessionsDeterministicAndDurable(t *testing.T) {
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
	createSessions(t, h, "dev", "sess-c", "sess-a", "sess-b")

	snapshot := func() map[string]string {
		bodies := map[string]string{}
		for _, url := range []string{
			"/v1/devices/dev/sessions",
			"/v1/devices/dev/sessions?limit=1&offset=0",
			"/v1/devices/dev/sessions?limit=1&offset=1",
			"/v1/devices/dev/sessions?limit=1&offset=2",
			"/v1/devices/dev/sessions?offset=3",
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
