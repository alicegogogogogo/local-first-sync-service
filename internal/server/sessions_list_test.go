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

// getSessionList GETs a device's session collection and decodes the "sessions"
// array. It returns the raw recorder so callers can assert status and bytes.
func getSessionList(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []map[string]any) {
	t.Helper()
	w := serveRecorder(h, httptest.NewRequest(http.MethodGet, url, nil))
	var body struct {
		Sessions []map[string]any `json:"sessions"`
		Count    int              `json:"count"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("list body is not JSON: %v body=%s", err, w.Body.String())
		}
		if body.Sessions == nil {
			t.Fatalf("sessions is null, want a (possibly empty) array: %s", w.Body.String())
		}
		if body.Count != len(body.Sessions) {
			t.Fatalf("count=%d but array has %d entries: %s", body.Count, len(body.Sessions), w.Body.String())
		}
	}
	return w, body.Sessions
}

func sessionIDs(items []map[string]any) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item["sessionId"].(string))
	}
	return ids
}

func createSession(t *testing.T, h http.Handler, device, session string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": session})
	if w.Code != http.StatusOK {
		t.Fatalf("create session %q for %q = %d %s", session, device, w.Code, w.Body.String())
	}
}

// The listing answers every session the device owns, each once, in ascending
// lexicographic order regardless of creation order; each item carries only the
// session id and the body is one compact JSON line ending in a newline with the
// sessions array before the count.
func TestListSessionsContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")

	// Created deliberately out of lexicographic order.
	for _, id := range []string{"s-3", "s-1", "s-10", "s-2"} {
		createSession(t, h, "dev-1", id)
	}
	// Another device's session never leaks into dev-1's list.
	createSession(t, h, "dev-2", "s-0")

	w, items := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d body=%s", w.Code, w.Body.String())
	}
	if got := sessionIDs(items); !equalStrings(got, []string{"s-1", "s-10", "s-2", "s-3"}) {
		t.Fatalf("list order = %v, want lexicographic [s-1 s-10 s-2 s-3]", got)
	}
	for _, item := range items {
		if len(item) != 1 || item["sessionId"] == "" {
			t.Fatalf("item must carry only sessionId, got %v", item)
		}
	}
	// Compact one-line body, array key before count, trailing newline.
	want := `{"sessions":[{"sessionId":"s-1"},{"sessionId":"s-10"},{"sessionId":"s-2"},{"sessionId":"s-3"}],"count":4}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q, want %q", w.Body.String(), want)
	}

	// A device with no sessions gets an empty (non-null) array and zero count.
	w, items = getSessionList(t, h, "/v1/devices/dev-2/sessions")
	// dev-2 owns s-0, so use a fresh third device for the truly empty case.
	registerDevice(t, h, "dev-3")
	w, items = getSessionList(t, h, "/v1/devices/dev-3/sessions")
	if w.Code != http.StatusOK || len(items) != 0 {
		t.Fatalf("empty list = %d %v", w.Code, items)
	}
	if w.Body.String() != `{"sessions":[],"count":0}`+"\n" {
		t.Fatalf("empty body = %q", w.Body.String())
	}

	// dev-2 sees only its own session.
	w, items = getSessionList(t, h, "/v1/devices/dev-2/sessions")
	if w.Code != http.StatusOK || !equalStrings(sessionIDs(items), []string{"s-0"}) {
		t.Fatalf("dev-2 list = %d %v", w.Code, items)
	}
}

// Pagination slices the fixed lexicographic order: pages neither repeat nor
// skip, a partial last page is fine, a page past the end is empty with count 0,
// and limit bounds 1 and 1000 are both accepted.
func TestListSessionsPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	for _, id := range []string{"s-3", "s-1", "s-4", "s-2", "s-5"} {
		createSession(t, h, "dev-1", id)
	}

	// Default limit (100) returns everything in one ordered page.
	_, items := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if got := sessionIDs(items); !equalStrings(got, []string{"s-1", "s-2", "s-3", "s-4", "s-5"}) {
		t.Fatalf("default page = %v", got)
	}

	// Walk in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		_, page := getSessionList(t, h, fmt.Sprintf("/v1/devices/dev-1/sessions?limit=2&offset=%d", offset))
		seen = append(seen, sessionIDs(page)...)
	}
	if !equalStrings(seen, []string{"s-1", "s-2", "s-3", "s-4", "s-5"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// Partial last page and a page past the end.
	_, page := getSessionList(t, h, "/v1/devices/dev-1/sessions?limit=4&offset=3")
	if got := sessionIDs(page); !equalStrings(got, []string{"s-4", "s-5"}) {
		t.Fatalf("last partial page = %v", got)
	}
	w, page := getSessionList(t, h, "/v1/devices/dev-1/sessions?offset=5")
	if w.Code != http.StatusOK || len(page) != 0 {
		t.Fatalf("page past end = %d %v", w.Code, page)
	}
	if w.Body.String() != `{"sessions":[],"count":0}`+"\n" {
		t.Fatalf("past-end body = %q", w.Body.String())
	}

	if w, _ := getSessionList(t, h, "/v1/devices/dev-1/sessions?limit=1"); w.Code != http.StatusOK {
		t.Fatalf("limit=1 = %d", w.Code)
	}
	if w, _ := getSessionList(t, h, "/v1/devices/dev-1/sessions?limit=1000"); w.Code != http.StatusOK {
		t.Fatalf("limit=1000 = %d", w.Code)
	}
}

// Illegal pagination parameters are a 400 JSON error before any device lookup,
// and an unregistered device is a 404 JSON error with no listing content.
func TestListSessionsRejectsBadParamsAndUnknownDevice(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSession(t, h, "dev-1", "s-1")

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
	if strings.Contains(w.Body.String(), "s-1") || strings.Contains(w.Body.String(), "sessions") {
		t.Fatalf("404 body leaks listing: %s", w.Body.String())
	}

	// Shape wins over device existence: a bad query against a ghost device is 400.
	r = httptest.NewRequest(http.MethodGet, "/v1/devices/ghost/sessions?limit=0", nil)
	w = serveRecorder(h, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ghost with bad limit = %d, want 400", w.Code)
	}

	// None of the failed reads changed anything.
	_, items := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if got := sessionIDs(items); !equalStrings(got, []string{"s-1"}) {
		t.Fatalf("list after rejections = %v", got)
	}
}

// Empty identifiers, trailing slashes, missing or extra segments and a method
// mismatch are all 400 JSON errors — never a redirect or HTML. A device or
// session literally named "sessions" is an ordinary identifier; the keyword is
// only the endpoint word at the endpoint segment position.
func TestListSessionsShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	createSession(t, h, "dev-1", "s-1")
	createSession(t, h, "dev-1", "sessions")

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifiers and trailing slashes.
		{http.MethodGet, "/v1/devices//sessions"},
		{http.MethodGet, "/v1/devices/dev-1/sessions/"},
		{http.MethodGet, "/v1/devices/dev-1//sessions"},
		// Extra segments past the collection/item path.
		{http.MethodGet, "/v1/devices/dev-1/sessions/s-1/extra"},
		// Method mismatch on the collection path.
		{http.MethodPost, "/v1/devices/dev-1/sessions"},
		{http.MethodPut, "/v1/devices/dev-1/sessions"},
		{http.MethodPatch, "/v1/devices/dev-1/sessions"},
		{http.MethodDelete, "/v1/devices/dev-1/sessions"},
		// Method mismatch on the item path (the item path accepts only DELETE).
		{http.MethodGet, "/v1/devices/dev-1/sessions/s-1"},
		{http.MethodPost, "/v1/devices/dev-1/sessions/s-1"},
		{http.MethodPut, "/v1/devices/dev-1/sessions/s-1"},
	}
	for _, tc := range cases {
		w := serveRecorder(h, newJSONRequest(tc.method, tc.path, "", ""))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400", tc.method, tc.path, w.Code)
		}
		assertJSONError(t, w)
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %s content-type = %q, want application/json", tc.method, tc.path, ct)
		}
	}

	// A session named "sessions" is an ordinary id: it appears in the list and
	// keeps its item routes (DELETE) rather than being treated as an endpoint.
	_, items := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if got := sessionIDs(items); !equalStrings(got, []string{"s-1", "sessions"}) {
		t.Fatalf("keyword-named session missing: %v", got)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sessions"); w.Code != http.StatusOK {
		t.Fatalf("deleting the keyword-named session = %d, want 200", w.Code)
	}

	// A device named "sessions" is likewise an ordinary identifier: its
	// collection list resolves to that device, not the /v1/sessions subtree.
	registerDevice(t, h, "sessions")
	createSession(t, h, "sessions", "s-9")
	w, items := getSessionList(t, h, "/v1/devices/sessions/sessions")
	if w.Code != http.StatusOK || !equalStrings(sessionIDs(items), []string{"s-9"}) {
		t.Fatalf("keyword-named device list = %d %v", w.Code, items)
	}

	// The over-long session DELETE keeps its pre-existing unknown-path 404.
	w2 := serveRecorder(h, newJSONRequest(http.MethodDelete, "/v1/devices/dev-1/sessions/s-1/extra", "", ""))
	if w2.Code != http.StatusNotFound {
		t.Fatalf("over-long DELETE = %d, want 404", w2.Code)
	}
}

// The listing is read-only: a deleted session vanishes, deregistering the
// device removes every session from the list in the same cascade, and a
// same-named re-registration starts from an empty list.
func TestListSessionsDeleteAndDeregister(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	createSession(t, h, "dev-1", "s-1")
	createSession(t, h, "dev-1", "s-2")
	createSession(t, h, "dev-2", "s-3")

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/s-1"); w.Code != http.StatusOK {
		t.Fatalf("delete s-1 = %d", w.Code)
	}
	_, items := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if got := sessionIDs(items); !equalStrings(got, []string{"s-2"}) {
		t.Fatalf("list after session delete = %v", got)
	}

	// Deregister dev-1: its sessions disappear from the listing (now a 404),
	// while dev-2's sessions are untouched.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1"); w.Code != http.StatusOK {
		t.Fatalf("deregister = %d", w.Code)
	}
	w, _ := getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if w.Code != http.StatusNotFound {
		t.Fatalf("deregistered device list = %d, want 404", w.Code)
	}
	_, items = getSessionList(t, h, "/v1/devices/dev-2/sessions")
	if got := sessionIDs(items); !equalStrings(got, []string{"s-3"}) {
		t.Fatalf("other device list changed = %v", got)
	}

	// Re-registering the same id yields a brand-new device: 200, empty list.
	registerDevice(t, h, "dev-1")
	w, items = getSessionList(t, h, "/v1/devices/dev-1/sessions")
	if w.Code != http.StatusOK || len(items) != 0 {
		t.Fatalf("re-registered device list = %d %v", w.Code, items)
	}
}

// Repeated reads return byte-for-byte identical bodies and create nothing; the
// same holds across a process restart.
func TestListSessionsIdempotentAndSurvivesRestart(t *testing.T) {
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
	for _, id := range []string{"s-2", "s-1", "s-3"} {
		createSession(t, h, "dev-1", id)
	}

	snapshot := func(t *testing.T, h http.Handler) map[string]string {
		t.Helper()
		bodies := map[string]string{}
		for _, url := range []string{
			"/v1/devices/dev-1/sessions",
			"/v1/devices/dev-1/sessions?limit=2&offset=0",
			"/v1/devices/dev-1/sessions?limit=2&offset=2",
			"/v1/devices/dev-1/sessions?offset=9",
		} {
			w := serveRecorder(h, httptest.NewRequest(http.MethodGet, url, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("GET %s = %d", url, w.Code)
			}
			bodies[url] = w.Body.String()
		}
		return bodies
	}

	before := snapshot(t, h)
	// A repeat request in the same process is byte-for-byte identical.
	for url, body := range snapshot(t, h) {
		if before[url] != body {
			t.Fatalf("repeat GET %s = %q, want %q", url, body, before[url])
		}
	}
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	after := snapshot(t, h)
	for url, body := range before {
		if after[url] != body {
			t.Fatalf("GET %s after restart = %q, want %q", url, after[url], body)
		}
	}
}
