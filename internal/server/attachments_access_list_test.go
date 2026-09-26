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

// getAccessRoster GETs an attachment's access subresource and decodes the
// "devices" array when the status is 200.
func getAccessRoster(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	var body struct {
		Devices []string `json:"devices"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("roster body is not JSON: %v body=%s", err, w.Body.String())
		}
		if body.Devices == nil {
			t.Fatalf("devices is null, want a (possibly empty) array: %s", w.Body.String())
		}
	}
	return w, body.Devices
}

func rosterURL(caller, attachment, query string) string {
	u := fmt.Sprintf("/v1/devices/%s/attachments/%s/access", caller, attachment)
	if query != "" {
		u += "?" + query
	}
	return u
}

// The roster lists only currently-authorized devices, each once, by the time
// their latest grant took effect: a revoke removes them at once and a
// re-grant moves them to the newest position.
func TestAttachmentAccessRosterContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")
	registerDevice(t, h, "dev-4")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)

	// Before any grant the roster is an empty (non-null) array.
	w, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if w.Code != http.StatusOK || len(devices) != 0 {
		t.Fatalf("empty roster = %d %v", w.Code, devices)
	}
	if !strings.Contains(w.Body.String(), `"devices":[]`) {
		t.Fatalf("empty roster body = %s", w.Body.String())
	}

	// Grants list in the order they took effect.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-3", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-4", "grant")
	w, devices = getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if w.Code != http.StatusOK || !equalStrings(devices, []string{"dev-2", "dev-3", "dev-4"}) {
		t.Fatalf("roster = %d %v", w.Code, devices)
	}

	// A grant to the creator itself never appears in its own roster.
	setAccess(t, h, "dev-1", "att-1", "dev-1", "grant")
	_, devices = getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2", "dev-3", "dev-4"}) {
		t.Fatalf("roster after self-grant = %v", devices)
	}

	// Revoking dev-3 removes it at once; the rest keep their positions and
	// the creator's other authorizations are untouched.
	setAccess(t, h, "dev-1", "att-1", "dev-3", "revoke")
	_, devices = getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2", "dev-4"}) {
		t.Fatalf("roster after revoke = %v, want [dev-2 dev-4]", devices)
	}

	// Re-granting dev-3 puts it at the position of the latest grant and still
	// exactly once.
	setAccess(t, h, "dev-1", "att-1", "dev-3", "grant")
	_, devices = getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("roster after regrant = %v, want [dev-2 dev-4 dev-3]", devices)
	}

	// Repeating a grant is idempotent and must not move the device.
	setAccess(t, h, "dev-1", "att-1", "dev-4", "grant")
	_, devices = getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("roster after idempotent grant = %v", devices)
	}
}

// limit/offset page the stable grant order with the exact same contract as
// the device attachment listing.
func TestAttachmentAccessRosterPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	for _, id := range []string{"dev-2", "dev-3", "dev-4", "dev-5", "dev-6"} {
		registerDevice(t, h, id)
	}
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	for _, id := range []string{"dev-2", "dev-3", "dev-4", "dev-5", "dev-6"} {
		setAccess(t, h, "dev-1", "att-1", id, "grant")
	}

	// Default limit is 100: one page holds everything in grant order.
	_, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2", "dev-3", "dev-4", "dev-5", "dev-6"}) {
		t.Fatalf("default page = %v", devices)
	}

	// Walk the roster in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := 0; offset < 5; offset += 2 {
		_, page := getAccessRoster(t, h, rosterURL("dev-1", "att-1", fmt.Sprintf("limit=2&offset=%d", offset)))
		seen = append(seen, page...)
	}
	if !equalStrings(seen, []string{"dev-2", "dev-3", "dev-4", "dev-5", "dev-6"}) {
		t.Fatalf("paged walk = %v", seen)
	}

	// A partial last page and a page past the end.
	_, page := getAccessRoster(t, h, rosterURL("dev-1", "att-1", "limit=4&offset=3"))
	if !equalStrings(page, []string{"dev-5", "dev-6"}) {
		t.Fatalf("last partial page = %v", page)
	}
	w, page := getAccessRoster(t, h, rosterURL("dev-1", "att-1", "offset=5"))
	if w.Code != http.StatusOK || len(page) != 0 {
		t.Fatalf("page past end = %d %v", w.Code, page)
	}

	// Bounds match the attachment listing.
	if w, _ := getAccessRoster(t, h, rosterURL("dev-1", "att-1", "limit=1")); w.Code != http.StatusOK {
		t.Fatalf("limit=1 = %d", w.Code)
	}
	if w, _ := getAccessRoster(t, h, rosterURL("dev-1", "att-1", "limit=1000")); w.Code != http.StatusOK {
		t.Fatalf("limit=1000 = %d", w.Code)
	}
}

// Illegal pagination parameters are a 400 checked before the attachment
// lookup, matching the attachment listing exactly.
func TestAttachmentAccessRosterRejectsBadParams(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)

	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=x", "limit=1.5",
		"offset=-1", "offset=x", "offset=1.5",
	} {
		w, _ := getAccessRoster(t, h, rosterURL("dev-1", "att-1", query))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("?%s = %d, want 400 body=%s", query, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Shape errors win over the attachment lookup: 400 even for a missing
	// attachment.
	w, _ := getAccessRoster(t, h, rosterURL("dev-1", "nope", "limit=0"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing attachment with bad limit = %d, want 400", w.Code)
	}

	// The rejected queries changed nothing.
	_, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if len(devices) != 0 {
		t.Fatalf("roster after rejected params = %v, want empty", devices)
	}
}

// Unknown or deleted attachments are a 404 for every caller; non-creators get
// a 403 that neither leaks the roster nor changes the grants. The two
// verdicts are judged independently and never merge into one error.
func TestAttachmentAccessRosterNotFoundAndForbidden(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// An unknown attachment is a 404 for the creator and for a non-creator
	// alike: existence is not hidden behind the ownership verdict.
	for _, caller := range []string{"dev-1", "dev-2", "dev-3"} {
		w, _ := getAccessRoster(t, h, rosterURL(caller, "nope", ""))
		if w.Code != http.StatusNotFound {
			t.Fatalf("roster on unknown attachment as %s = %d, want 404", caller, w.Code)
		}
		assertJSONError(t, w)
	}

	// A non-creator gets a 403 for an existing attachment — even dev-2,
	// which is currently granted read access — and the body leaks no device
	// ids.
	for _, caller := range []string{"dev-2", "dev-3"} {
		w, _ := getAccessRoster(t, h, rosterURL(caller, "att-1", ""))
		if w.Code != http.StatusForbidden {
			t.Fatalf("roster as %s = %d, want 403", caller, w.Code)
		}
		assertJSONError(t, w)
		if strings.Contains(w.Body.String(), "dev-") {
			t.Fatalf("403 body leaks roster: %s", w.Body.String())
		}
	}

	// The 403s changed no state: dev-2 still reads and still appears.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusOK {
		t.Fatalf("granted read after 403 roster = %d, want 200", w.Code)
	}
	_, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2"}) {
		t.Fatalf("roster after 403s = %v, want [dev-2]", devices)
	}

	// Deleting the attachment turns the roster into a stable 404 for
	// everyone, leaving no roster content behind.
	deleteAttachment(t, h, "dev-1", "att-1")
	for _, caller := range []string{"dev-1", "dev-2"} {
		w, _ := getAccessRoster(t, h, rosterURL(caller, "att-1", ""))
		if w.Code != http.StatusNotFound {
			t.Fatalf("roster after delete as %s = %d, want 404", caller, w.Code)
		}
		assertJSONError(t, w)
		if strings.Contains(w.Body.String(), "dev-") {
			t.Fatalf("post-delete 404 body leaks roster: %s", w.Body.String())
		}
	}

	// Re-creating the same id starts a brand-new roster: the old grants were
	// deleted with the attachment and never come back.
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	w, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if w.Code != http.StatusOK || len(devices) != 0 {
		t.Fatalf("roster after recreate = %d %v, want empty 200", w.Code, devices)
	}
	// The old grant does not read either.
	if w := getAttachmentMeta(t, h, "dev-2", "att-1"); w.Code != http.StatusForbidden {
		t.Fatalf("old grantee read after recreate = %d, want 403", w.Code)
	}
}

// Empty identifiers, missing or extra segments and verbs other than GET are
// all 400 JSON errors — never a redirect or HTML — and change nothing.
func TestAttachmentAccessRosterShapeContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	cases := []struct {
		method string
		path   string
	}{
		// Empty identifiers and trailing slashes.
		{http.MethodGet, "/v1/devices//attachments/att-1/access"},
		{http.MethodGet, "/v1/devices/dev-1/attachments//access"},
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/access/"},
		// Missing or extra segments.
		{http.MethodGet, "/v1/devices/dev-1/attachments/att-1/access/extra"},
		{http.MethodPost, "/v1/devices/dev-1/attachments/att-1/access/extra"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments/att-1/access/extra"},
		// Verbs other than GET/POST on the access subresource.
		{http.MethodPut, "/v1/devices/dev-1/attachments/att-1/access"},
		{http.MethodDelete, "/v1/devices/dev-1/attachments/att-1/access"},
		{http.MethodPatch, "/v1/devices/dev-1/attachments/att-1/access"},
		{http.MethodOptions, "/v1/devices/dev-1/attachments/att-1/access"},
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

	// Zero writes: the grant is intact and POST still drives access changes.
	_, devices := getAccessRoster(t, h, rosterURL("dev-1", "att-1", ""))
	if !equalStrings(devices, []string{"dev-2"}) {
		t.Fatalf("roster after rejected shapes = %v", devices)
	}
	w, body := setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	if w.Code != http.StatusOK || body["changed"] != true {
		t.Fatalf("POST access after shape rejects = %d %v", w.Code, body)
	}
}

// The roster is a read-only view: content and order are byte-for-byte
// identical across a process restart.
func TestAttachmentAccessRosterSurvivesRestart(t *testing.T) {
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
	registerDevice(t, h, "dev-3")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-3", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	snapshot := func(t *testing.T, h http.Handler) map[string]string {
		t.Helper()
		bodies := map[string]string{}
		for _, query := range []string{"", "limit=1&offset=0", "limit=1&offset=1", "offset=2"} {
			r := httptest.NewRequest(http.MethodGet, rosterURL("dev-1", "att-1", query), nil)
			w := serveRecorder(h, r)
			if w.Code != http.StatusOK {
				t.Fatalf("GET ?%q = %d", query, w.Code)
			}
			bodies[query] = w.Body.String()
		}
		return bodies
	}

	before := snapshot(t, h)
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	after := snapshot(t, h)
	for query, body := range before {
		if after[query] != body {
			t.Fatalf("GET ?%q after restart = %s, want %s", query, after[query], body)
		}
	}
}
