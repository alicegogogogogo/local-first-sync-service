package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// getAccessList GETs the access list of one attachment and decodes the
// "devices" array when the status is 200.
func getAccessList(t *testing.T, h http.Handler, url string) (*httptest.ResponseRecorder, []string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, url, nil)
	w := serveRecorder(h, r)
	var body struct {
		Devices []string `json:"devices"`
	}
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatalf("access list body is not JSON: %v body=%s", err, w.Body.String())
		}
		if body.Devices == nil {
			t.Fatalf("devices is null, want a (possibly empty) array: %s", w.Body.String())
		}
	}
	return w, body.Devices
}

// The access list shows exactly the currently authorized devices, each once,
// in the order their most recent grant took effect. A revoke removes the
// device at once; a re-grant lists it again, once, at its new position. The
// creator's other grants and the attachment content are untouched.
func TestListAttachmentAccessContract(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-3")
	registerDevice(t, h, "dev-4")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)

	// No grants yet: the list is an empty array, not null.
	w, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if w.Code != http.StatusOK || len(devices) != 0 {
		t.Fatalf("empty list = %d %v", w.Code, devices)
	}

	// Grants list in the order they took effect.
	setAccess(t, h, "dev-1", "att-1", "dev-3", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	setAccess(t, h, "dev-1", "att-1", "dev-4", "grant")
	w, devices = getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if w.Code != http.StatusOK || !equalStrings(devices, []string{"dev-3", "dev-2", "dev-4"}) {
		t.Fatalf("list = %d %v", w.Code, devices)
	}

	// A revoke removes the device at once; the other grants keep their order.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "revoke")
	_, devices = getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(devices, []string{"dev-3", "dev-4"}) {
		t.Fatalf("list after revoke = %v", devices)
	}

	// A re-grant lists the device exactly once, at the position of its most
	// recent grant.
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	_, devices = getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(devices, []string{"dev-3", "dev-4", "dev-2"}) {
		t.Fatalf("list after re-grant = %v", devices)
	}

	// The listing is read-only: repeating it changes neither the list nor the
	// idempotency of the access ledger.
	_, again := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(again, devices) {
		t.Fatalf("repeat read = %v, want %v", again, devices)
	}
	w, body := setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")
	if w.Code != http.StatusOK || body["changed"] != false {
		t.Fatalf("grant idempotency moved by listing: %d %v", w.Code, body)
	}
}

// A non-creator caller gets a 403 JSON error and no list content; an unknown
// or deleted attachment is a 404 JSON error for everyone. The two judgments
// are independent and neither changes the access state.
func TestListAttachmentAccessForbiddenAndNotFound(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// A non-creator — even a granted reader — cannot read the list.
	w, _ := getAccessList(t, h, "/v1/devices/dev-2/attachments/att-1/access")
	if w.Code != http.StatusForbidden {
		t.Fatalf("non-creator = %d, want 403", w.Code)
	}
	assertJSONError(t, w)

	// An unknown attachment is a 404, for the creator and for anyone else.
	for _, device := range []string{"dev-1", "dev-2"} {
		w, _ := getAccessList(t, h, fmt.Sprintf("/v1/devices/%s/attachments/nope/access", device))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unknown attachment as %s = %d, want 404", device, w.Code)
		}
		assertJSONError(t, w)
	}

	// The failed reads changed nothing: the grant is still in effect.
	_, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(devices, []string{"dev-2"}) {
		t.Fatalf("list after failed reads = %v", devices)
	}

	// Deleting the attachment turns the entry into a stable 404 for everyone.
	r := httptest.NewRequest(http.MethodDelete, "/v1/devices/dev-1/attachments/att-1", nil)
	if w := serveRecorder(h, r); w.Code != http.StatusOK {
		t.Fatalf("delete = %d", w.Code)
	}
	for _, device := range []string{"dev-1", "dev-2"} {
		w, _ := getAccessList(t, h, fmt.Sprintf("/v1/devices/%s/attachments/att-1/access", device))
		if w.Code != http.StatusNotFound {
			t.Fatalf("deleted attachment as %s = %d, want 404", device, w.Code)
		}
		assertJSONError(t, w)
	}
}

// The access list pages with the same limit/offset rules as the attachment
// listing: defaults, bounds and 400 JSON errors on illegal values, and pages
// that neither repeat nor skip entries while the grants are unchanged.
func TestListAttachmentAccessPagination(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	for _, target := range []string{"dev-2", "dev-3", "dev-4", "dev-5"} {
		registerDevice(t, h, target)
	}
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	for _, target := range []string{"dev-2", "dev-3", "dev-4", "dev-5"} {
		setAccess(t, h, "dev-1", "att-1", target, "grant")
	}

	// Pages tile the full order without repeats or gaps.
	var seen []string
	for offset := 0; offset < 4; offset += 2 {
		_, devices := getAccessList(t, h, fmt.Sprintf("/v1/devices/dev-1/attachments/att-1/access?limit=2&offset=%d", offset))
		seen = append(seen, devices...)
	}
	if !equalStrings(seen, []string{"dev-2", "dev-3", "dev-4", "dev-5"}) {
		t.Fatalf("paged list = %v", seen)
	}

	// The default limit lists everything; an offset past the end is empty.
	_, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(devices, []string{"dev-2", "dev-3", "dev-4", "dev-5"}) {
		t.Fatalf("default page = %v", devices)
	}
	w, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access?offset=4")
	if w.Code != http.StatusOK || len(devices) != 0 {
		t.Fatalf("offset past end = %d %v", w.Code, devices)
	}

	// Illegal pagination values are a 400 JSON error.
	for _, query := range []string{
		"limit=0", "limit=-1", "limit=1001", "limit=1.5", "limit=x",
		"offset=-1", "offset=1.5", "offset=x",
	} {
		r := httptest.NewRequest(http.MethodGet, "/v1/devices/dev-1/attachments/att-1/access?"+query, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", query, w.Code)
		}
		assertJSONError(t, w)
	}
}

// Non-GET verbs, extra or missing path segments and empty identifiers on the
// access list path are all 400 JSON errors — never a redirect or HTML — and
// write nothing.
func TestListAttachmentAccessMalformedRequests(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	mustCreateAttachment(t, h, "dev-1", "att-1", []byte("data"), 2)
	setAccess(t, h, "dev-1", "att-1", "dev-2", "grant")

	// Every verb but GET (and the POST grant/revoke entry) is a 400.
	for _, method := range []string{http.MethodPut, http.MethodPatch, http.MethodDelete} {
		r := httptest.NewRequest(method, "/v1/devices/dev-1/attachments/att-1/access", nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", method, w.Code)
		}
	}

	// Extra segments, a trailing slash and empty identifiers are a 400.
	for _, path := range []string{
		"/v1/devices/dev-1/attachments/att-1/access/extra",
		"/v1/devices/dev-1/attachments/att-1/access/",
		"/v1/devices//attachments/att-1/access",
		"/v1/devices/dev-1/attachments//access",
	} {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("GET %s = %d, want 400", path, w.Code)
		}
		assertJSONError(t, w)
	}

	// None of the rejections touched the access state.
	_, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if !equalStrings(devices, []string{"dev-2"}) {
		t.Fatalf("list after malformed requests = %v", devices)
	}
}

// The list content and order are persisted with the access state: a restart
// answers the same request byte-for-byte.
func TestListAttachmentAccessSurvivesRestart(t *testing.T) {
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
	w, devices := getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if w.Code != http.StatusOK || !equalStrings(devices, []string{"dev-3", "dev-2"}) {
		t.Fatalf("list before restart = %d %v", w.Code, devices)
	}
	before := w.Body.String()
	_ = s.Close()

	h, s = open(t)
	defer func() { _ = s.Close() }()
	w, devices = getAccessList(t, h, "/v1/devices/dev-1/attachments/att-1/access")
	if w.Code != http.StatusOK || !equalStrings(devices, []string{"dev-3", "dev-2"}) || w.Body.String() != before {
		t.Fatalf("list after restart = %d %q, want %q", w.Code, w.Body.String(), before)
	}
}
