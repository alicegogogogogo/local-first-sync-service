package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// Session-scoped named snapshot version path builders.
func sessionVersionsCollection(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshots/versions"
}

func sessionVersionItem(session, doc, name string) string {
	return sessionVersionsCollection(session, doc) + "/" + name
}

func sessionVersionRestore(session, doc, name string) string {
	return sessionVersionItem(session, doc, name) + "/restore"
}

// seedSessionVersionFixture registers the owning device/session and seeds the
// doc history reused by the version tests: two changes and two snapshots
// (cursor 1 -> {"v":"one"}, cursor 2 -> {"v":"two"}).
func seedSessionVersionFixture(t *testing.T, h http.Handler, device, session, doc string) {
	t.Helper()
	createSessionViaHTTP(t, h, device, session)
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h, fmt.Sprintf("/v1/documents/%s/changes", doc), map[string]any{
			"deviceId": device,
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatalf("seed change %d = %d %s", i+1, w.Code, w.Body.String())
		}
		w, _ = postJSON(t, h, fmt.Sprintf("/v1/documents/%s/snapshots", doc), map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("seed snapshot %d = %d %s", i+1, w.Code, w.Body.String())
		}
	}
}

// seedSessionDocHistory seeds just the two-change/two-snapshot history on a doc
// whose owning device is already registered.
func seedSessionDocHistory(t *testing.T, h http.Handler, device, doc string) {
	t.Helper()
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h, fmt.Sprintf("/v1/documents/%s/changes", doc), map[string]any{
			"deviceId": device,
			"changes":  []any{map[string]any{"id": fmt.Sprintf("h%d", i+1), "payload": map[string]any{"n": i + 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatalf("seed change %d = %d %s", i+1, w.Code, w.Body.String())
		}
		w, _ = postJSON(t, h, fmt.Sprintf("/v1/documents/%s/snapshots", doc), map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("seed snapshot %d = %d %s", i+1, w.Code, w.Body.String())
		}
	}
}

// sessionRegisterVersion registers a name through the session view and returns
// the trimmed one-line success body.
func sessionRegisterVersion(t *testing.T, h http.Handler, session, doc, name string, cursor int) string {
	t.Helper()
	w, _ := postJSON(t, h, sessionVersionsCollection(session, doc), map[string]any{
		"name": name, "snapshotCursor": cursor,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("session register %q -> %d = %d %s", name, cursor, w.Code, w.Body.String())
	}
	return strings.TrimSpace(w.Body.String())
}

// A first registration through the session view answers one compact JSON line
// plus a trailing newline, naming the version and cursor in that fixed order.
func TestSessionSnapshotVersionRegisterShape(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")

	w := serveRecorder(h, newJSONRequest(http.MethodPost,
		sessionVersionsCollection("sess", "doc"),
		`{"name":"v1","snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","snapshotCursor":1}`+"\n" {
		t.Fatalf("body = %q", got)
	}

	// A stray deviceId is decoded and ignored like any other unknown field;
	// the session's owning device is the caller.
	w = serveRecorder(h, newJSONRequest(http.MethodPost,
		sessionVersionsCollection("sess", "doc"),
		`{"name":"v-stray","snapshotCursor":2,"deviceId":"someone-else"}`, "application/json"))
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"name":"v-stray","snapshotCursor":2}` {
		t.Fatalf("stray deviceId not ignored: %d %s", w.Code, w.Body.String())
	}
}

// Same name + same cursor is idempotent with the identical body and zero extra
// rows; a different cursor is a 409 and leaves the binding untouched.
func TestSessionSnapshotVersionRegisterIdempotentAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")

	first := sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)
	w, _ := postJSON(t, h, sessionVersionsCollection("sess", "doc"), map[string]any{
		"name": "v1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != first {
		t.Fatalf("idempotent body = %d %q, want %q", w.Code, w.Body.String(), first)
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess", "doc"))
	if strings.Count(w.Body.String(), `"name":"v1"`) != 1 {
		t.Fatalf("idempotent registration added a row: %s", w.Body.String())
	}

	// Rebind to another cursor: 409, structured conflictName, zero writes.
	w, _ = postJSON(t, h, sessionVersionsCollection("sess", "doc"), map[string]any{
		"name": "v1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("rebind = %d, want 409, %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"conflictName":"v1","error":"snapshot version is already bound to another cursor"}` {
		t.Fatalf("conflict body = %q", got)
	}
	wg, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "doc", "v1"))
	if !strings.Contains(wg.Body.String(), `"cursor":1`) {
		t.Fatalf("conflict moved the binding: %s", wg.Body.String())
	}
}

// The list is ascending by name with name,snapshotCursor entries; an unknown
// document and an empty view read as [] with count 0.
func TestSessionSnapshotVersionListOrderAndEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "zeta", 1)
	sessionRegisterVersion(t, h, "sess", "doc", "alpha", 2)
	sessionRegisterVersion(t, h, "sess", "doc", "mid", 1)

	w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess", "doc"))
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got !=
		`{"versions":[{"name":"alpha","snapshotCursor":2},{"name":"mid","snapshotCursor":1},{"name":"zeta","snapshotCursor":1}],"count":3}` {
		t.Fatalf("list body = %q", got)
	}

	// Unknown document: empty view, still 200.
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess", "never"))
	if got := w.Body.String(); got != `{"versions":[],"count":0}`+"\n" {
		t.Fatalf("unknown-doc list = %q", got)
	}
}

// The session by-name read is byte-for-byte the document by-cursor read (and
// the document by-name read), state presented verbatim.
func TestSessionSnapshotVersionReadByteIdentical(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v2", 2)

	byNameSession := serveRecorder(h, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess", "doc", "v2"), "", ""))
	byNameDoc := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/versions/v2?deviceId=dev-1", "", ""))
	byCursor := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/2", "", ""))
	for _, w := range []*httptest.ResponseRecorder{byNameSession, byNameDoc, byCursor} {
		if w.Code != http.StatusOK {
			t.Fatalf("read status = %d %s", w.Code, w.Body.String())
		}
	}
	if !bytes.Equal(byNameSession.Body.Bytes(), byCursor.Body.Bytes()) {
		t.Fatalf("session by-name %q != by-cursor %q", byNameSession.Body.String(), byCursor.Body.String())
	}
	if !bytes.Equal(byNameSession.Body.Bytes(), byNameDoc.Body.Bytes()) {
		t.Fatalf("session by-name %q != document by-name %q", byNameSession.Body.String(), byNameDoc.Body.String())
	}
	if got := byNameSession.Body.String(); got != `{"cursor":2,"state":{"v":"two"}}`+"\n" {
		t.Fatalf("state not verbatim: %q", got)
	}
}

// Rename moves a name onto another snapshot; onto the same cursor is
// idempotent; the snapshot itself is untouched and a missing target is 404.
func TestSessionSnapshotVersionRename(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)

	put := func(cursor int) *httptest.ResponseRecorder {
		return serveRecorder(h, newJSONRequest(http.MethodPut,
			sessionVersionItem("sess", "doc", "v1"),
			fmt.Sprintf(`{"snapshotCursor":%d}`, cursor), "application/json"))
	}
	if w := put(1); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"name":"v1","snapshotCursor":1}` {
		t.Fatalf("idempotent rename = %d %q", w.Code, w.Body.String())
	}
	if w := put(2); w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"name":"v1","snapshotCursor":2}` {
		t.Fatalf("rename = %d %q", w.Code, w.Body.String())
	}
	if got := sessionVersionCursor(t, h, "sess", "doc", "v1"); got != 2 {
		t.Fatalf("after rename v1 -> %d, want 2", got)
	}
	// Snapshot 1 unchanged.
	if w := serveRecorder(h, newJSONRequest(http.MethodGet, "/v1/documents/doc/snapshots/1", "", "")); strings.TrimSpace(w.Body.String()) != `{"cursor":1,"state":{"v":"one"}}` {
		t.Fatalf("rename modified snapshot: %s", w.Body.String())
	}
	// Missing target snapshot: 404, binding unchanged.
	if w := put(99); w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
	if got := sessionVersionCursor(t, h, "sess", "doc", "v1"); got != 2 {
		t.Fatalf("failed rename moved marker to %d", got)
	}
}

// sessionVersionCursor reads a version item through the session view and
// returns the snapshot cursor.
func sessionVersionCursor(t *testing.T, h http.Handler, session, doc, name string) int {
	t.Helper()
	w := serveRecorder(h, newJSONRequest(http.MethodGet,
		sessionVersionItem(session, doc, name), "", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("read version %q = %d %s", name, w.Code, w.Body.String())
	}
	var body struct {
		Cursor int `json:"cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Cursor
}

// Delete hard-deletes the marker (404 afterward, snapshot untouched), a repeat
// delete is 404, and the freed name can bind to another cursor.
func TestSessionSnapshotVersionDeleteAndReuse(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)

	w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess", "doc", "v1"))
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"deleted":true,"name":"v1"}` {
		t.Fatalf("delete = %d %q", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1"); w.Code != http.StatusOK {
		t.Fatalf("delete removed the snapshot: %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d, want 404", w.Code)
	}
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 2)
	if got := sessionVersionCursor(t, h, "sess", "doc", "v1"); got != 2 {
		t.Fatalf("reused name -> %d, want 2", got)
	}
}

// By-name restore appends the named snapshot's state as an ordinary change
// (cursor +1) with the cursor-based restore body; idempotency repeats and an
// ordinary-change id conflict follow the existing restore rules.
func TestSessionSnapshotVersionRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v2", 2)

	restore := func(changeID string) *httptest.ResponseRecorder {
		w, _ := postJSON(t, h, sessionVersionRestore("sess", "doc", "v2"), map[string]any{"changeId": changeID})
		return w
	}

	if w := restore("r1"); w.Code != http.StatusOK ||
		strings.TrimSpace(w.Body.String()) != `{"id":"r1","created":true,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("restore = %d %q", w.Code, w.Body.String())
	}
	if w := restore("r1"); w.Code != http.StatusOK ||
		strings.TrimSpace(w.Body.String()) != `{"id":"r1","created":false,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("idempotent restore = %d %q", w.Code, w.Body.String())
	}
	// Ordinary change id occupied: 409 zero writes.
	if w := restore("c1"); w.Code != http.StatusConflict {
		t.Fatalf("restore onto ordinary change id = %d, want 409, %s", w.Code, w.Body.String())
	}
	// Missing name: 404.
	w, _ := postJSON(t, h, sessionVersionRestore("sess", "doc", "nope"), map[string]any{"changeId": "r9"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("restore unknown name = %d, want 404", w.Code)
	}
	// The cursor advanced exactly once for the one genuine restore.
	wl, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if !strings.Contains(wl.Body.String(), `"nextCursor":3`) {
		t.Fatalf("cursor after restore = %s", wl.Body.String())
	}
}

// A marker registered through one view is the same marker in the other: the
// session view adds no separate namespace.
func TestSessionSnapshotVersionSharedWithDocumentView(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")

	// Register through the document view.
	registerVersion(t, h, "doc", "dev-1", "shared", 1)
	// Read, list and rename through the session view.
	if got := sessionVersionCursor(t, h, "sess", "doc", "shared"); got != 1 {
		t.Fatalf("session view did not see document marker: %d", got)
	}
	w := serveRecorder(h, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess", "doc", "shared"), `{"snapshotCursor":2}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("session rename of document marker = %d %s", w.Code, w.Body.String())
	}
	if got := registerVersionMust(t, h, "doc", "dev-1", "shared"); got != 2 {
		t.Fatalf("document view did not see session rename: %d", got)
	}
	// Delete through the session view; gone from the document view too.
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess", "doc", "shared")); w.Code != http.StatusOK {
		t.Fatalf("session delete = %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions/shared?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("marker survived in document view: %d", w.Code)
	}
}

// Fixed verdict order: request shape (400) -> session existence (404) ->
// document permission (403) -> version/snapshot existence (404) -> conflict
// (409). An earlier failure masks later ones and exposes no content.
func TestSessionSnapshotVersionFailureOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)
	createSessionViaHTTP(t, h, "dev-rev", "sess-rev")
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-rev", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	collection := sessionVersionsCollection("sess", "doc")
	item := sessionVersionItem("sess", "doc", "v1")
	restore := sessionVersionRestore("sess", "doc", "v1")

	shapeCases := []struct {
		name   string
		method string
		path   string
		body   string
		ct     string
		delCT  bool
	}{
		{"register wrong content type", http.MethodPost, collection, `{"name":"x","snapshotCursor":1}`, "text/plain", false},
		{"register bad json", http.MethodPost, collection, `{not json`, "application/json", false},
		{"register trailing data", http.MethodPost, collection, `{"name":"x","snapshotCursor":1} x`, "application/json", false},
		{"register empty name", http.MethodPost, collection, `{"name":"","snapshotCursor":1}`, "application/json", false},
		{"register missing cursor", http.MethodPost, collection, `{"name":"x"}`, "application/json", false},
		{"register zero cursor", http.MethodPost, collection, `{"name":"x","snapshotCursor":0}`, "application/json", false},
		{"register string cursor", http.MethodPost, collection, `{"name":"x","snapshotCursor":"1"}`, "application/json", false},
		{"register float cursor", http.MethodPost, collection, `{"name":"x","snapshotCursor":1.5}`, "application/json", false},
		{"register null cursor", http.MethodPost, collection, `{"name":"x","snapshotCursor":null}`, "application/json", false},
		{"rename missing cursor", http.MethodPut, item, `{}`, "application/json", false},
		{"rename bad cursor", http.MethodPut, item, `{"snapshotCursor":0}`, "application/json", false},
		{"restore missing changeId", http.MethodPost, restore, `{}`, "application/json", false},
		{"restore empty changeId", http.MethodPost, restore, `{"changeId":""}`, "application/json", false},
		{"wrong method GET on restore", http.MethodGet, restore, "", "", true},
		{"wrong method POST on item", http.MethodPost, item, "", "", true},
		{"wrong method PUT on collection", http.MethodPut, collection, "", "", true},
		{"wrong method DELETE on collection", http.MethodDelete, collection, "", "", true},
		{"wrong method PATCH on item", http.MethodPatch, item, "", "", true},
		{"extra segment past item", http.MethodGet, item + "/extra", "", "", true},
		{"extra segment past restore", http.MethodPost, restore + "/x", "", "", true},
		{"versions item on collection as segment", http.MethodGet, collection + "/", "", "", true},
	}
	for _, tc := range shapeCases {
		t.Run("shape/"+tc.name, func(t *testing.T) {
			r := newJSONRequest(tc.method, tc.path, tc.body, tc.ct)
			if tc.delCT {
				r.Header.Del("Content-Type")
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400, body = %s", tc.method, tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Shape precedes session existence: malformed bodies/params against a
	// never-created session are still 400.
	for _, c := range []struct{ method, path, body string }{
		{http.MethodPost, sessionVersionsCollection("ghost", "doc"), `{"name":"","snapshotCursor":1}`},
		{http.MethodPost, sessionVersionsCollection("ghost", "doc"), `{"name":"x"}`},
		{http.MethodPut, sessionVersionItem("ghost", "doc", "v1"), `{"snapshotCursor":0}`},
		{http.MethodPost, sessionVersionRestore("ghost", "doc", "v1"), `{"changeId":""}`},
	} {
		w := serveRecorder(h, newJSONRequest(c.method, c.path, c.body, "application/json"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("shape vs missing session %s = %d, want 400", c.path, w.Code)
		}
	}

	// Session existence precedes permission and content on every verb.
	if w, _ := postJSON(t, h, sessionVersionsCollection("ghost", "doc"), map[string]any{"name": "g", "snapshotCursor": 1}); w.Code != http.StatusNotFound {
		t.Fatalf("register missing session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("ghost", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read missing session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("ghost", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("list missing session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("ghost", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("delete missing session = %d, want 404", w.Code)
	}
	if w, _ := postJSON(t, h, sessionVersionRestore("ghost", "doc", "v1"), map[string]any{"changeId": "r"}); w.Code != http.StatusNotFound {
		t.Fatalf("restore missing session = %d, want 404", w.Code)
	}

	// Permission precedes version/snapshot existence, on every verb — even a
	// missing name or target snapshot answers 403 for a revoked caller.
	if w, _ := postJSON(t, h, sessionVersionsCollection("sess-rev", "doc"), map[string]any{"name": "anything", "snapshotCursor": 1}); w.Code != http.StatusForbidden {
		t.Fatalf("register revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-rev", "doc", "v1")); w.Code != http.StatusForbidden {
		t.Fatalf("read revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-rev", "doc")); w.Code != http.StatusForbidden {
		t.Fatalf("list revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-rev", "doc", "missing")); w.Code != http.StatusForbidden {
		t.Fatalf("delete missing-name revoked = %d, want 403", w.Code)
	}
	if w, _ := postJSON(t, h, sessionVersionRestore("sess-rev", "doc", "v1"), map[string]any{"changeId": "r"}); w.Code != http.StatusForbidden {
		t.Fatalf("restore revoked = %d, want 403", w.Code)
	}
	// Session existence precedes permission: unknown session on a revoked doc.
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("ghost", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	// Content existence is last: an authorized caller gets 404 for a missing
	// name or target snapshot.
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "doc", "missing")); w.Code != http.StatusNotFound {
		t.Fatalf("read missing name = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess", "doc", "missing")); w.Code != http.StatusNotFound {
		t.Fatalf("delete missing name = %d, want 404", w.Code)
	}
	if w, _ := postJSON(t, h, sessionVersionsCollection("sess", "doc"), map[string]any{"name": "fresh", "snapshotCursor": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("register missing snapshot = %d, want 404", w.Code)
	}
	w := serveRecorder(h, newJSONRequest(http.MethodPut, sessionVersionItem("sess", "doc", "v1"),
		`{"snapshotCursor":99}`, "application/json"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename missing snapshot = %d, want 404", w.Code)
	}
}

// A deleted session (and a never-created one) answers 404 with no version
// content on every endpoint.
func TestSessionSnapshotVersionSessionMissing(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess")
	seedSessionDocHistory(t, h, "dev-1", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)

	check := func(t *testing.T, session string) {
		if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection(session, "doc")); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), `"error"`) {
			t.Fatalf("list %s = %d %s", session, w.Code, w.Body.String())
		}
		if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem(session, "doc", "v1")); w.Code != http.StatusNotFound || strings.Contains(w.Body.String(), `"state"`) {
			t.Fatalf("read %s = %d %s", session, w.Code, w.Body.String())
		}
	}
	check(t, "ghost")

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1/sessions/sess"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	check(t, "sess")
}

// Revoking the session device's document permission answers 403 with no
// content; re-granting restores the endpoints.
func TestSessionSnapshotVersionPermissionRevoked(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)
	sessionRegisterVersion(t, h, "sess", "doc", "v2", 2)

	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-1", "action": "revoke"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, sessionVersionsCollection("sess", "doc")},
		{http.MethodGet, sessionVersionItem("sess", "doc", "v1")},
		{http.MethodDelete, sessionVersionItem("sess", "doc", "v1")},
	} {
		w, body := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusForbidden || body["error"] == nil {
			t.Fatalf("%s %s revoked = %d %v", c.method, c.path, w.Code, body)
		}
		if body["versions"] != nil || body["state"] != nil || body["cursor"] != nil {
			t.Fatalf("403 leaked content: %s", w.Body.String())
		}
	}
	if w, _ := postJSON(t, h, sessionVersionsCollection("sess", "doc"),
		map[string]any{"name": "v3", "snapshotCursor": 1}); w.Code != http.StatusForbidden {
		t.Fatalf("register revoked = %d", w.Code)
	}

	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions",
		map[string]any{"deviceId": "dev-1", "action": "grant"}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess", "doc"))
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"name":`) != 2 {
		t.Fatalf("re-grant list = %d %s", w.Code, w.Body.String())
	}
}

// Wrong verbs and malformed shapes answer a JSON 400 with no HTML; a keyword
// used as a version name (including a name of "restore") keeps its routes.
func TestSessionSnapshotVersionMethodAndPathGuards(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)

	for _, c := range []struct{ method, path string }{
		{http.MethodPut, sessionVersionsCollection("sess", "doc")},
		{http.MethodDelete, sessionVersionsCollection("sess", "doc")},
		{http.MethodPatch, sessionVersionsCollection("sess", "doc")},
		{http.MethodPost, sessionVersionItem("sess", "doc", "v1")},
		{http.MethodPatch, sessionVersionItem("sess", "doc", "v1")},
		{http.MethodGet, sessionVersionRestore("sess", "doc", "v1")},
		{http.MethodPut, sessionVersionRestore("sess", "doc", "v1")},
		{http.MethodDelete, sessionVersionRestore("sess", "doc", "v1")},
		{http.MethodGet, sessionVersionItem("sess", "doc", "v1") + "/extra"},
		{http.MethodGet, sessionVersionsCollection("sess", "doc") + "/v1/extra"},
		{http.MethodPost, sessionVersionRestore("sess", "doc", "v1") + "/x"},
		{http.MethodGet, sessionVersionsCollection("sess", "doc") + "/"},
		{http.MethodGet, "/v1/sessions//documents/doc/snapshots/versions"},
		{http.MethodGet, "/v1/sessions/sess/documents//snapshots/versions"},
	} {
		w, _ := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, %q", c.method, c.path, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content-type %q", c.method, c.path, ct)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") || strings.Contains(w.Body.String(), "Method Not Allowed") {
			t.Fatalf("%s %s non-JSON body %q", c.method, c.path, w.Body.String())
		}
	}

	// Keyword-named versions keep their routes through the session view.
	for _, name := range []string{"merge", "restore", "versions", "poll", "subscribe", "compact", "query", "replay"} {
		sessionRegisterVersion(t, h, "sess", "doc", name, 1)
		if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "doc", name)); w.Code != http.StatusOK {
			t.Fatalf("read keyword version %q = %d %s", name, w.Code, w.Body.String())
		}
	}
	// A version literally named "restore" restores by name via the nested
	// path; move it onto snapshot cursor 2 with a rename first.
	wb := serveRecorder(h, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess", "doc", "restore"), `{"snapshotCursor":2}`, "application/json"))
	if wb.Code != http.StatusOK {
		t.Fatalf("rename keyword version restore = %d %s", wb.Code, wb.Body.String())
	}
	w, _ := postJSON(t, h, sessionVersionRestore("sess", "doc", "restore"), map[string]any{"changeId": "rr1"})
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"id":"rr1","created":true,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("nested restore of keyword version = %d %s", w.Code, w.Body.String())
	}

	// A document literally named "snapshots" works in the versions subtree.
	seedSessionDocHistory(t, h, "dev-1", "snapshots")
	sessionRegisterVersion(t, h, "sess", "snapshots", "k", 1)
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "snapshots", "k")); w.Code != http.StatusOK {
		t.Fatalf("document named snapshots version read = %d %s", w.Code, w.Body.String())
	}
}

// Markers are part of the document: deleting the document clears them, and the
// same id recreated afterwards inherits no version through the session view.
func TestSessionSnapshotVersionCascadesOnDocumentDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("delete document = %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess", "doc")); strings.TrimSpace(w.Body.String()) != `{"versions":[],"count":0}` {
		t.Fatalf("markers survived delete: %s", w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("marker readable after delete = %d", w.Code)
	}

	// Recreate the document history; the name binds afresh to cursor 1.
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("n%d", i+1), "payload": map[string]any{"n": i + 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
		w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatal(w.Body.String())
		}
	}
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)
	if got := sessionVersionCursor(t, h, "sess", "doc", "v1"); got != 1 {
		t.Fatalf("recreated doc inherited stale marker: %d", got)
	}
}

// Registrations, renames, deletes and the verdicts survive a process restart;
// the bodies and the idempotent/conflict judgments are unchanged.
func TestSessionSnapshotVersionPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-versions-restart.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedSessionVersionFixture(t, h, "dev-1", "sess", "doc")
	sessionRegisterVersion(t, h, "sess", "doc", "v1", 1)
	sessionRegisterVersion(t, h, "sess", "doc", "v2", 2)
	w := serveRecorder(h, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess", "doc", "v1"), `{"snapshotCursor":2}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("rename before restart = %d %s", w.Code, w.Body.String())
	}
	before := serveRecorder(h, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess", "doc", "v1"), "", ""))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	wl, _ := doRequest(t, h2, http.MethodGet, sessionVersionsCollection("sess", "doc"))
	if got := strings.TrimSpace(wl.Body.String()); got !=
		`{"versions":[{"name":"v1","snapshotCursor":2},{"name":"v2","snapshotCursor":2}],"count":2}` {
		t.Fatalf("list after restart = %q", got)
	}
	// Idempotent re-bind still 200; rebind to a different cursor still 409.
	if w := serveRecorder(h2, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess", "doc", "v1"), `{"snapshotCursor":2}`, "application/json")); w.Code != http.StatusOK {
		t.Fatalf("idempotent rename after restart = %d", w.Code)
	}
	if w, _ := postJSON(t, h2, sessionVersionsCollection("sess", "doc"),
		map[string]any{"name": "v1", "snapshotCursor": 1}); w.Code != http.StatusConflict {
		t.Fatalf("conflict verdict after restart = %d, want 409", w.Code)
	}
	after := serveRecorder(h2, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess", "doc", "v1"), "", ""))
	if !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) {
		t.Fatalf("read changed across restart: %q vs %q", before.Body.String(), after.Body.String())
	}
	// Delete persists too.
	if w, _ := doRequest(t, h2, http.MethodDelete, sessionVersionItem("sess", "doc", "v2")); w.Code != http.StatusOK {
		t.Fatalf("delete after restart = %d", w.Code)
	}
}
