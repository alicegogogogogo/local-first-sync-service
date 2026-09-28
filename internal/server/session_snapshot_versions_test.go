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

// seedSessionVersionFixture registers a device with a live session, then
// creates two changes (through the session commit entry) and two snapshots
// (cursor 1 state {"v":"one"}, cursor 2 state {"v":"two"}) on doc, so the
// session-scoped version tests start from a known history.
func seedSessionVersionFixture(t *testing.T, h http.Handler, device, session, doc string) {
	t.Helper()
	createSessionViaHTTP(t, h, device, session)
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h,
			fmt.Sprintf("/v1/sessions/%s/documents/%s/changes", session, doc),
			map[string]any{
				"changes": []any{map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}}},
			})
		if w.Code != http.StatusOK {
			t.Fatalf("seed session change %d = %d %s", i+1, w.Code, w.Body.String())
		}
		// Snapshot creation is only exposed on the document-level collection;
		// the session view reads and names those snapshots but does not create
		// them.
		w, _ = postJSON(t, h, fmt.Sprintf("/v1/documents/%s/snapshots", doc), map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("seed snapshot %d = %d %s", i+1, w.Code, w.Body.String())
		}
	}
}

func sessionVersionsCollection(session, doc string) string {
	return fmt.Sprintf("/v1/sessions/%s/documents/%s/snapshots/versions", session, doc)
}

func sessionVersionItem(session, doc, name string) string {
	return sessionVersionsCollection(session, doc) + "/" + name
}

func sessionVersionRestorePath(session, doc, name string) string {
	return sessionVersionItem(session, doc, name) + "/restore"
}

func registerSessionVersion(t *testing.T, h http.Handler, session, doc, name string, cursor int) string {
	t.Helper()
	w, _ := postJSON(t, h, sessionVersionsCollection(session, doc), map[string]any{
		"name":           name,
		"snapshotCursor": cursor,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("register session version %q -> %d = %d %s", name, cursor, w.Code, w.Body.String())
	}
	return strings.TrimSpace(w.Body.String())
}

// sessionVersionPointsAt returns the cursor the named session version points
// at, failing the test on any error.
func sessionVersionPointsAt(t *testing.T, h http.Handler, session, doc, name string) int {
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

// A first registration answers one compact JSON line plus a trailing newline
// naming the version and the snapshot cursor, in that fixed key order — the
// very body the document-level registration emits.
func TestSessionSnapshotVersionRegisterShape(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")

	w := serveRecorder(h, newJSONRequest(http.MethodPost,
		sessionVersionsCollection("sess-1", "doc"),
		`{"name":"v1","snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","snapshotCursor":1}`+"\n" {
		t.Fatalf("body = %q", got)
	}
}

// The calling device is the session's owning device: a stray deviceId in the
// body is decoded and ignored and can never override it.
func TestSessionSnapshotVersionIdentityComesFromSession(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	createSessionViaHTTP(t, h, "dev-2", "sess-2")

	// A stray deviceId naming an unrelated device is ignored; the bind lands
	// as dev-1, the owner of sess-1.
	w, _ := postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{
		"name": "a", "snapshotCursor": 1, "deviceId": "ghost",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("stray deviceId changed the verdict = %d %s", w.Code, w.Body.String())
	}

	// Revoke dev-2 only. A registration through sess-2 is then 403 while the
	// same call through sess-1 still succeeds — proof the device is taken from
	// the session, not the body (which carries none).
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-2", "action": "revoke",
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, sessionVersionsCollection("sess-2", "doc"), map[string]any{
		"name": "b", "snapshotCursor": 1,
	})
	if w.Code != http.StatusForbidden {
		t.Fatalf("register as revoked session owner = %d, want 403, %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{
		"name": "b", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("register as authorized session owner = %d %s", w.Code, w.Body.String())
	}
}

// Binding the same name to the same cursor through the session is idempotent:
// the second call has the identical body and writes nothing new.
func TestSessionSnapshotVersionRegisterIdempotent(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")

	first := registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	w, _ := postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{
		"name": "v1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent status = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != first {
		t.Fatalf("idempotent body = %q, want %q", got, first)
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-1", "doc"))
	if strings.Count(w.Body.String(), `"name":"v1"`) != 1 {
		t.Fatalf("idempotent registration added a row: %s", w.Body.String())
	}
}

// Rebinding an existing name to another cursor through the collection POST is
// a 409 with zero writes; the name stays on its original cursor.
func TestSessionSnapshotVersionRegisterConflictRebind(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)

	w, _ := postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{
		"name": "v1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("rebind status = %d, want 409, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)
	if strings.TrimSpace(w.Body.String()) != `{"conflictName":"v1","error":"snapshot version is already bound to another cursor"}` {
		t.Fatalf("rebind conflict body = %q", w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-1", "doc"))
	if !strings.Contains(w.Body.String(), `"name":"v1","snapshotCursor":1`) {
		t.Fatalf("conflict changed the binding: %s", w.Body.String())
	}
}

// The list is ascending by name with the versions/count shape; an unknown
// document (or an empty view) is an empty array with count 0, still 200.
func TestSessionSnapshotVersionListOrderAndEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "zeta", 1)
	registerSessionVersion(t, h, "sess-1", "doc", "alpha", 2)
	registerSessionVersion(t, h, "sess-1", "doc", "mid", 1)

	w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-1", "doc"))
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got !=
		`{"versions":[{"name":"alpha","snapshotCursor":2},{"name":"mid","snapshotCursor":1},{"name":"zeta","snapshotCursor":1}],"count":3}` {
		t.Fatalf("list body = %q", got)
	}

	// Empty view: an unknown document but an existing, authorized session.
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-1", "never"))
	if got := strings.TrimSpace(w.Body.String()); got != `{"versions":[],"count":0}` {
		t.Fatalf("unknown-doc list = %q", got)
	}
}

// A by-name session read returns the exact bytes of the by-cursor document
// read, with the state presented verbatim.
func TestSessionSnapshotVersionReadByteIdentical(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v2", 2)

	byName := serveRecorder(h, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess-1", "doc", "v2"), "", ""))
	byCursor := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/2", "", ""))
	if byName.Code != http.StatusOK || byCursor.Code != http.StatusOK {
		t.Fatalf("reads = %d / %d", byName.Code, byCursor.Code)
	}
	if !bytes.Equal(byName.Body.Bytes(), byCursor.Body.Bytes()) {
		t.Fatalf("by-name %q != by-cursor %q", byName.Body.String(), byCursor.Body.String())
	}
	if got := byName.Body.String(); got != `{"cursor":2,"state":{"v":"two"}}`+"\n" {
		t.Fatalf("state not presented verbatim: %q", got)
	}
}

// Rename (PUT) moves a name onto another existing snapshot; moving it onto the
// cursor it already points at is idempotent. The snapshot itself is untouched.
func TestSessionSnapshotVersionRename(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)

	item := sessionVersionItem("sess-1", "doc", "v1")
	// Idempotent rename onto the same cursor: same body, still one marker.
	w := serveRecorder(h, newJSONRequest(http.MethodPut, item,
		`{"snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent rename = %d %s", w.Code, w.Body.String())
	}
	// Actual rename onto cursor 2.
	w = serveRecorder(h, newJSONRequest(http.MethodPut, item,
		`{"snapshotCursor":2}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("rename = %d %s", w.Code, w.Body.String())
	}
	if got := sessionVersionPointsAt(t, h, "sess-1", "doc", "v1"); got != 2 {
		t.Fatalf("after rename v1 points at %d, want 2", got)
	}
	// Snapshot 1 is still readable unchanged.
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1"); strings.TrimSpace(w.Body.String()) != `{"cursor":1,"state":{"v":"one"}}` {
		t.Fatalf("rename modified the snapshot: %s", w.Body.String())
	}

	// Rename a missing name: 404.
	w = serveRecorder(h, newJSONRequest(http.MethodPut, sessionVersionItem("sess-1", "doc", "ghost"),
		`{"snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename missing name = %d, want 404", w.Code)
	}
	// Rename onto a missing snapshot: 404, binding unchanged.
	w = serveRecorder(h, newJSONRequest(http.MethodPut, item,
		`{"snapshotCursor":99}`, "application/json"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
	if got := sessionVersionPointsAt(t, h, "sess-1", "doc", "v1"); got != 2 {
		t.Fatalf("failed rename moved the marker to %d", got)
	}
}

// Delete hard-deletes the marker (404 afterward, snapshot untouched) and frees
// the name to bind again; a repeat delete misses with 404.
func TestSessionSnapshotVersionDeleteAndReuse(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)

	w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-1", "doc", "v1"))
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"deleted":true,"name":"v1"}` {
		t.Fatalf("delete body = %q", got)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-1", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1"); w.Code != http.StatusOK {
		t.Fatalf("delete removed the snapshot: %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-1", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d, want 404", w.Code)
	}
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 2)
	if got := sessionVersionPointsAt(t, h, "sess-1", "doc", "v1"); got != 2 {
		t.Fatalf("reused name points at %d, want 2", got)
	}
}

// By-name session restore appends the named snapshot's state as an ordinary
// change (cursor +1) with the exact cursor-based restore body; the existing
// restore idempotency and conflict rules carry over.
func TestSessionSnapshotVersionRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v2", 2)

	restore := func(changeID string) *httptest.ResponseRecorder {
		w, _ := postJSON(t, h, sessionVersionRestorePath("sess-1", "doc", "v2"), map[string]any{
			"changeId": changeID,
		})
		return w
	}
	w := restore("r1")
	if w.Code != http.StatusOK {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"id":"r1","created":true,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("restore body = %q", got)
	}
	// Idempotent repeat: same first cursor, created=false.
	if w := restore("r1"); w.Code != http.StatusOK ||
		strings.TrimSpace(w.Body.String()) != `{"id":"r1","created":false,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("idempotent restore = %d %s", w.Code, w.Body.String())
	}
	// An id occupied by an ordinary change is a 409 with zero writes.
	if w := restore("c1"); w.Code != http.StatusConflict {
		t.Fatalf("restore onto an ordinary change id = %d, want 409, %s", w.Code, w.Body.String())
	}
	// The appended ordinary change carries the snapshot state and is owned by
	// the session's device.
	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=2")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	row := list["changes"].([]any)[0].(map[string]any)
	if row["id"] != "r1" || row["deviceId"] != "dev-1" {
		t.Fatalf("restored change = %v", row)
	}
	if row["payload"].(map[string]any)["v"] != "two" {
		t.Fatalf("restored payload = %v", row["payload"])
	}
	// A missing version name is 404.
	w, _ = postJSON(t, h, sessionVersionRestorePath("sess-1", "doc", "nope"), map[string]any{
		"changeId": "r2",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("restore unknown name = %d, want 404", w.Code)
	}
}

// Registering, renaming or deleting a marker never allocates a change cursor:
// the document high-water mark is unchanged by the version metadata calls.
func TestSessionSnapshotVersionMarkersDoNotMoveCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	w := serveRecorder(h, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess-1", "doc", "v1"),
		`{"snapshotCursor":2}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-1", "doc", "v1")); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["nextCursor"].(float64) != 2 {
		t.Fatalf("version metadata moved the cursor: nextCursor = %v", body["nextCursor"])
	}
}

// Fixed verdict order: request shape (400) -> session existence (404) ->
// document permission (403) -> version/snapshot existence (404). An earlier
// failure masks every later one and exposes no version content.
func TestSessionSnapshotVersionFailureOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	// createSessionViaHTTP registers dev-rev and opens sess-rev.
	createSessionViaHTTP(t, h, "dev-rev", "sess-rev")
	if w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-rev", "action": "revoke",
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	collection := sessionVersionsCollection("sess-1", "doc")
	item := sessionVersionItem("sess-1", "doc", "v1")
	restore := sessionVersionRestorePath("sess-1", "doc", "v1")

	shapeCases := []struct {
		name   string
		method string
		path   string
		ct     string
		body   string
	}{
		{"register wrong content type", http.MethodPost, collection, "text/plain", `{"name":"x","snapshotCursor":1}`},
		{"register bad json", http.MethodPost, collection, "application/json", `{not json`},
		{"register trailing data", http.MethodPost, collection, "application/json", `{"name":"x","snapshotCursor":1} garbage`},
		{"register empty name", http.MethodPost, collection, "application/json", `{"name":"","snapshotCursor":1}`},
		{"register missing name", http.MethodPost, collection, "application/json", `{"snapshotCursor":1}`},
		{"register missing cursor", http.MethodPost, collection, "application/json", `{"name":"x"}`},
		{"register zero cursor", http.MethodPost, collection, "application/json", `{"name":"x","snapshotCursor":0}`},
		{"register string cursor", http.MethodPost, collection, "application/json", `{"name":"x","snapshotCursor":"1"}`},
		{"register float cursor", http.MethodPost, collection, "application/json", `{"name":"x","snapshotCursor":1.5}`},
		{"register null cursor", http.MethodPost, collection, "application/json", `{"name":"x","snapshotCursor":null}`},
		{"rename missing cursor", http.MethodPut, item, "application/json", `{}`},
		{"rename zero cursor", http.MethodPut, item, "application/json", `{"snapshotCursor":0}`},
		{"rename string cursor", http.MethodPut, item, "application/json", `{"snapshotCursor":"2"}`},
		{"restore missing changeId", http.MethodPost, restore, "application/json", `{}`},
		{"restore empty changeId", http.MethodPost, restore, "application/json", `{"changeId":""}`},
		{"wrong method GET on restore", http.MethodGet, restore, "", ""},
		{"wrong method DELETE on restore", http.MethodDelete, restore, "", ""},
		{"wrong method POST on item", http.MethodPost, item, "application/json", `{}`},
		{"wrong method PUT on collection", http.MethodPut, collection, "application/json", `{}`},
		{"wrong method PATCH on item", http.MethodPatch, item, "application/json", `{}`},
		{"extra segment", http.MethodGet, item + "/extra", "", ""},
		{"restore extra segment", http.MethodPost, restore + "/x", "application/json", `{"changeId":"z"}`},
	}
	for _, tc := range shapeCases {
		t.Run("shape/"+tc.name, func(t *testing.T) {
			r := newJSONRequest(tc.method, tc.path, tc.body, tc.ct)
			if tc.method == http.MethodGet || tc.method == http.MethodDelete {
				r.Header.Del("Content-Type")
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400, body = %s", tc.method, tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// The session snapshot namespace has no single-snapshot item: a cursor
	// segment directly under snapshots is a malformed 400, not the read.
	if w, _ := doRequest(t, h, http.MethodGet,
		"/v1/sessions/sess-1/documents/doc/snapshots/9"); w.Code != http.StatusBadRequest {
		t.Fatalf("session single-snapshot item = %d, want 400", w.Code)
	}

	// A malformed body against an unknown session is still a 400: shape is
	// judged before session existence.
	w := serveRecorder(h, newJSONRequest(http.MethodPost,
		sessionVersionsCollection("ghost", "doc"),
		`{"name":"","snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape must precede session existence: %d, want 400", w.Code)
	}

	// Session existence precedes permission and content, on every verb.
	if w, _ := postJSON(t, h, sessionVersionsCollection("ghost", "doc"), map[string]any{"name": "g", "snapshotCursor": 1}); w.Code != http.StatusNotFound {
		t.Fatalf("register unknown session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("ghost", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read unknown session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("ghost", "doc")); w.Code != http.StatusNotFound {
		t.Fatalf("list unknown session = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("ghost", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown session = %d, want 404", w.Code)
	}

	// Permission precedes version/snapshot existence: a revoked session owner
	// is 403 even against a name or snapshot that exists (or one that does not).
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
		t.Fatalf("delete missing-name revoked = %d, want 403 (permission precedes existence)", w.Code)
	}

	// Content existence is last: an authorized session gets a 404 for a missing
	// name or a missing target snapshot.
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-1", "doc", "missing")); w.Code != http.StatusNotFound {
		t.Fatalf("read missing name = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-1", "doc", "missing")); w.Code != http.StatusNotFound {
		t.Fatalf("delete missing name = %d, want 404", w.Code)
	}
	if w, _ := postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{"name": "fresh", "snapshotCursor": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("register missing snapshot = %d, want 404", w.Code)
	}
	w = serveRecorder(h, newJSONRequest(http.MethodPut, sessionVersionItem("sess-1", "doc", "v1"),
		`{"snapshotCursor":99}`, "application/json"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
}

// A deleted session, or one whose owning device was deregistered (the cascade
// removes the session too), is a 404 that exposes no version content.
func TestSessionSnapshotVersionSessionGone(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	createSessionViaHTTP(t, h, "dev-2", "sess-2")

	// Delete sess-2 outright.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-2/sessions/sess-2"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-2", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read with deleted session = %d, want 404", w.Code)
	}

	// Deregister dev-1: its cascade deletes sess-1.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev-1"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-1", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("read with deregistered owner = %d, want 404", w.Code)
	}
	w, _ := postJSON(t, h, sessionVersionsCollection("sess-1", "doc"), map[string]any{"name": "x", "snapshotCursor": 1})
	if w.Code != http.StatusNotFound {
		t.Fatalf("register with deregistered owner = %d, want 404", w.Code)
	}
}

// A version name that collides with an endpoint keyword ("restore", "query",
// "subscribe", ...) is an ordinary identifier in the name segment and keeps
// the version item routes, including the by-name restore nested under it.
func TestSessionSnapshotVersionKeywordNamedVersions(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")

	for _, name := range []string{"merge", "restore", "versions", "poll", "subscribe", "query", "compact", "replay"} {
		registerSessionVersion(t, h, "sess-1", "doc", name, 1)
		w := serveRecorder(h, newJSONRequest(http.MethodGet,
			sessionVersionItem("sess-1", "doc", name), "", ""))
		if w.Code != http.StatusOK {
			t.Fatalf("read version named %q = %d %s", name, w.Code, w.Body.String())
		}
		w, _ = doRequest(t, h, http.MethodDelete, sessionVersionItem("sess-1", "doc", name))
		if w.Code != http.StatusOK {
			t.Fatalf("delete version named %q = %d %s", name, w.Code, w.Body.String())
		}
	}

	// A version literally named "restore" restored by name via the nested
	// restore segment.
	registerSessionVersion(t, h, "sess-1", "doc", "restore", 2)
	w, _ := postJSON(t, h, sessionVersionRestorePath("sess-1", "doc", "restore"), map[string]any{
		"changeId": "rr1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("by-name restore of version named restore = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"id":"rr1","created":true,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("nested restore body = %q", got)
	}
}

// Version markers are part of the document: deleting the document removes them
// together with its snapshots, and the same document id recreated afterwards
// inherits no version and binds names anew — observable through the session.
func TestSessionSnapshotVersionCascadesOnDocumentDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("delete document = %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionsCollection("sess-1", "doc")); strings.TrimSpace(w.Body.String()) != `{"versions":[],"count":0}` {
		t.Fatalf("versions survived delete: %s", w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, sessionVersionItem("sess-1", "doc", "v1")); w.Code != http.StatusNotFound {
		t.Fatalf("marker readable after delete = %d", w.Code)
	}

	// Recreate the document history (device still registered) and rebind.
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h,
			fmt.Sprintf("/v1/sessions/sess-1/documents/doc/changes"),
			map[string]any{"changes": []any{map[string]any{"id": fmt.Sprintf("n%d", i+1), "payload": map[string]any{"n": i + 1}}}})
		if w.Code != http.StatusOK {
			t.Fatalf("reseed change %d = %d %s", i+1, w.Code, w.Body.String())
		}
		w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("reseed snapshot %d = %d %s", i+1, w.Code, w.Body.String())
		}
	}
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	if got := sessionVersionPointsAt(t, h, "sess-1", "doc", "v1"); got != 1 {
		t.Fatalf("recreated doc inherited a stale marker: v1 -> %d", got)
	}
}

// Registrations, renames, deletes and the verdicts survive a process restart;
// the bodies are byte-for-byte unchanged when read through the session.
func TestSessionSnapshotVersionPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-versions-restart.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedSessionVersionFixture(t, h, "dev-1", "sess-1", "doc")
	registerSessionVersion(t, h, "sess-1", "doc", "v1", 1)
	registerSessionVersion(t, h, "sess-1", "doc", "v2", 2)
	before := serveRecorder(h, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess-1", "doc", "v2"), "", ""))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	w, _ := doRequest(t, h2, http.MethodGet, sessionVersionsCollection("sess-1", "doc"))
	if got := strings.TrimSpace(w.Body.String()); got !=
		`{"versions":[{"name":"v1","snapshotCursor":1},{"name":"v2","snapshotCursor":2}],"count":2}` {
		t.Fatalf("list after restart = %q", got)
	}
	// Idempotency still holds; a rebind is still a 409 after restart.
	if w, _ := postJSON(t, h2, sessionVersionsCollection("sess-1", "doc"), map[string]any{
		"name": "v1", "snapshotCursor": 2,
	}); w.Code != http.StatusConflict {
		t.Fatalf("conflict verdict after restart = %d, want 409", w.Code)
	}
	// Read body unchanged.
	after := serveRecorder(h2, newJSONRequest(http.MethodGet,
		sessionVersionItem("sess-1", "doc", "v2"), "", ""))
	if !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) {
		t.Fatalf("read changed across restart: %q vs %q", before.Body.String(), after.Body.String())
	}
	// A rename and delete committed before the first close also survive (they
	// were durable); verify a fresh rename across a second restart instead.
	wRen := serveRecorder(h2, newJSONRequest(http.MethodPut,
		sessionVersionItem("sess-1", "doc", "v1"),
		`{"snapshotCursor":2}`, "application/json"))
	if wRen.Code != http.StatusOK {
		t.Fatalf("rename after restart = %d %s", wRen.Code, wRen.Body.String())
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	h3 := NewHandler(s3)
	if got := sessionVersionPointsAt(t, h3, "sess-1", "doc", "v1"); got != 2 {
		t.Fatalf("rename did not survive restart: v1 -> %d", got)
	}
}
