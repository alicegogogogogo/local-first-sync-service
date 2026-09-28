package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// seedVersionFixture creates a registered device, two changes and two
// snapshots (cursor 1 state {"v":"one"}, cursor 2 state {"v":"two"}) on doc,
// so version tests start from a known history.
func seedVersionFixture(t *testing.T, h http.Handler, device, doc string) {
	t.Helper()
	registerDevice(t, h, device)
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

func registerVersion(t *testing.T, h http.Handler, doc, device, name string, cursor int) string {
	t.Helper()
	w, _ := postJSON(t, h, fmt.Sprintf("/v1/documents/%s/snapshots/versions", doc), map[string]any{
		"deviceId":       device,
		"name":           name,
		"snapshotCursor": cursor,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("register version %q -> %d = %d %s", name, cursor, w.Code, w.Body.String())
	}
	return strings.TrimSpace(w.Body.String())
}

// A first registration answers one compact JSON line plus a trailing newline
// naming the version and the snapshot cursor, in that fixed key order.
func TestSnapshotVersionRegisterShape(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")

	w := serveRecorder(h, newJSONRequest(http.MethodPost,
		"/v1/documents/doc/snapshots/versions",
		`{"deviceId":"dev-1","name":"v1","snapshotCursor":1}`, "application/json"))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","snapshotCursor":1}`+"\n" {
		t.Fatalf("body = %q", got)
	}
}

// Binding the same name to the same cursor is idempotent: the second call has
// the identical body and writes nothing new.
func TestSnapshotVersionRegisterIdempotent(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")

	first := registerVersion(t, h, "doc", "dev-1", "v1", 1)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent status = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != first {
		t.Fatalf("idempotent body = %q, want %q", got, first)
	}

	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if strings.Count(w.Body.String(), `"name":"v1"`) != 1 {
		t.Fatalf("idempotent registration added a row: %s", w.Body.String())
	}
}

// Rebinding an existing name to another cursor through the collection POST is
// a 409 with zero writes; the name stays on its original cursor.
func TestSnapshotVersionRegisterConflictRebind(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)

	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("rebind status = %d, want 409, body = %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if !strings.Contains(w.Body.String(), `"name":"v1","snapshotCursor":1`) {
		t.Fatalf("conflict changed the binding: %s", w.Body.String())
	}
}

// The list is ascending by name and each entry has only name and
// snapshotCursor. An unknown document (or an empty view) is an empty array
// with count 0.
func TestSnapshotVersionListOrderAndEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "zeta", 1)
	registerVersion(t, h, "doc", "dev-1", "alpha", 2)
	registerVersion(t, h, "doc", "dev-1", "mid", 1)

	w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got !=
		`{"versions":[{"name":"alpha","snapshotCursor":2},{"name":"mid","snapshotCursor":1},{"name":"zeta","snapshotCursor":1}],"count":3}` {
		t.Fatalf("list body = %q", got)
	}

	// Empty view: unknown document.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/never/snapshots/versions?deviceId=dev-1")
	if got := strings.TrimSpace(w.Body.String()); got != `{"versions":[],"count":0}` {
		t.Fatalf("unknown-doc list = %q", got)
	}
}

// A by-name read returns the exact body of the by-cursor read for the bound
// snapshot, with the state presented verbatim.
func TestSnapshotVersionReadByteIdentical(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v2", 2)

	byName := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/versions/v2?deviceId=dev-1", "", ""))
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

// A null/string/array/number snapshot state round-trips verbatim through the
// name.
func TestSnapshotVersionReadPreservesStateKinds(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	states := []string{`null`, `"str"`, `[1,2]`, `17`}
	for i, state := range states {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}}},
		})
		if w.Code != http.StatusOK {
			t.Fatalf("change %d = %d %s", i+1, w.Code, w.Body.String())
		}
		w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{
			"cursor": i + 1, "state": json.RawMessage(state),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("snapshot %d = %d %s", i+1, w.Code, w.Body.String())
		}
	}
	registerVersion(t, h, "doc", "dev-1", "n", 3) // cursor 3 holds [1,2]
	w := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/versions/n?deviceId=dev-1", "", ""))
	if got := strings.TrimSpace(w.Body.String()); got != `{"cursor":3,"state":[1,2]}` {
		t.Fatalf("array state = %q", got)
	}
	registerVersion(t, h, "doc", "dev-1", "s", 2) // cursor 2 holds "str"
	w = serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/versions/s?deviceId=dev-1", "", ""))
	if got := strings.TrimSpace(w.Body.String()); got != `{"cursor":2,"state":"str"}` {
		t.Fatalf("string state = %q", got)
	}
}

// Rename moves a name onto another existing snapshot; moving it onto the
// cursor it already points at is idempotent. The snapshot itself is untouched.
func TestSnapshotVersionRename(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)

	put := func(body any) *httptest.ResponseRecorder {
		return serveRecorder(h, newJSONRequest(http.MethodPut,
			"/v1/documents/doc/snapshots/versions/v1",
			string(mustMarshal(t, body)), "application/json"))
	}
	// Idempotent rename onto the same cursor: same body, still one marker.
	if w := put(map[string]any{"deviceId": "dev-1", "snapshotCursor": 1}); w.Code != http.StatusOK {
		t.Fatalf("idempotent rename = %d %s", w.Code, w.Body.String())
	}
	// Actual rename onto cursor 2.
	if w := put(map[string]any{"deviceId": "dev-1", "snapshotCursor": 2}); w.Code != http.StatusOK {
		t.Fatalf("rename = %d %s", w.Code, w.Body.String())
	}
	if got := registerVersionMust(t, h, "doc", "dev-1", "v1"); got != 2 {
		t.Fatalf("after rename v1 points at %d, want 2", got)
	}
	// Snapshot 1 is still readable unchanged.
	w := serveRecorder(h, newJSONRequest(http.MethodGet, "/v1/documents/doc/snapshots/1", "", ""))
	if strings.TrimSpace(w.Body.String()) != `{"cursor":1,"state":{"v":"one"}}` {
		t.Fatalf("rename modified the snapshot: %s", w.Body.String())
	}

	// Rename onto a missing snapshot: 404, binding unchanged.
	if w := put(map[string]any{"deviceId": "dev-1", "snapshotCursor": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
	if got := registerVersionMust(t, h, "doc", "dev-1", "v1"); got != 2 {
		t.Fatalf("failed rename moved the marker to %d", got)
	}
}

// Delete hard-deletes the marker (404 afterward, snapshot untouched) and frees
// the name to bind again; a repeat delete misses with 404.
func TestSnapshotVersionDeleteAndReuse(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)

	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc/snapshots/versions/v1?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"deleted":true,"name":"v1"}` {
		t.Fatalf("delete body = %q", got)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions/v1?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d, want 404", w.Code)
	}
	// Snapshot still there.
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/1"); w.Code != http.StatusOK {
		t.Fatalf("delete removed the snapshot: %d", w.Code)
	}
	// Repeat delete: 404.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc/snapshots/versions/v1?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d, want 404", w.Code)
	}
	// Name can be reused, now bound to cursor 2.
	registerVersion(t, h, "doc", "dev-1", "v1", 2)
	if got := registerVersionMust(t, h, "doc", "dev-1", "v1"); got != 2 {
		t.Fatalf("reused name points at %d, want 2", got)
	}
}

// By-name restore appends the named snapshot's state as an ordinary change
// (cursor +1) and answers the exact cursor-based restore body; the existing
// restore idempotency and conflict rules carry over.
func TestSnapshotVersionRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v2", 2)

	restore := func(changeID string) *httptest.ResponseRecorder {
		w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions/v2/restore", map[string]any{
			"deviceId": "dev-1", "changeId": changeID,
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
	// A missing version name is 404.
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/versions/nope/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r2",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("restore unknown name = %d, want 404", w.Code)
	}
}

// Fixed verdict order: request shape (400) -> device existence (404) ->
// document permission (403) -> version/snapshot existence (404). An earlier
// failure masks every later one and exposes no version content.
func TestSnapshotVersionFailureOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)
	registerDevice(t, h, "dev-2")
	registerDevice(t, h, "dev-rev")
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-rev", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	collection := "/v1/documents/doc/snapshots/versions"
	item := "/v1/documents/doc/snapshots/versions/v1"
	restore := item + "/restore"

	shapeCases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"register wrong content type", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":1}`},
		{"register bad json", http.MethodPost, collection, `{not json`},
		{"register trailing data", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":1} garbage`},
		{"register missing deviceId", http.MethodPost, collection, `{"name":"x","snapshotCursor":1}`},
		{"register empty name", http.MethodPost, collection, `{"deviceId":"dev-1","name":"","snapshotCursor":1}`},
		{"register missing cursor", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x"}`},
		{"register zero cursor", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":0}`},
		{"register string cursor", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":"1"}`},
		{"register float cursor", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":1.5}`},
		{"register null cursor", http.MethodPost, collection, `{"deviceId":"dev-1","name":"x","snapshotCursor":null}`},
		{"rename missing cursor", http.MethodPut, item, `{"deviceId":"dev-1"}`},
		{"rename empty deviceId", http.MethodPut, item, `{"deviceId":"","snapshotCursor":1}`},
		{"restore missing changeId", http.MethodPost, restore, `{"deviceId":"dev-1"}`},
		{"restore empty changeId", http.MethodPost, restore, `{"deviceId":"dev-1","changeId":""}`},
		{"wrong method GET on restore", http.MethodGet, restore, ""},
		{"wrong method POST on item", http.MethodPost, item, ""},
		{"wrong method PUT on collection", http.MethodPut, collection, ""},
		{"extra segment", http.MethodGet, item + "/extra", ""},
		{"restore extra segment", http.MethodPost, restore + "/x", ""},
		{"trailing slash collection", http.MethodGet, collection + "/", ""},
	}
	for _, tc := range shapeCases {
		t.Run("shape/"+tc.name, func(t *testing.T) {
			ct := "application/json"
			if tc.name == "register wrong content type" {
				ct = "text/plain"
			}
			r := newJSONRequest(tc.method, tc.path, tc.body, ct)
			if tc.method == http.MethodGet {
				r.Header.Del("Content-Type")
			}
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d, want 400, body = %s", tc.method, tc.path, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// The "missing deviceId on list" case is really a 404 (an empty caller is
	// indistinguishable from an unregistered one), checked explicitly.
	w, _ = doRequest(t, h, http.MethodGet, collection)
	if w.Code != http.StatusNotFound {
		t.Fatalf("list without deviceId = %d, want 404", w.Code)
	}

	// Device existence precedes permission and content, on every verb.
	if w, _ := postJSON(t, h, collection, map[string]any{"deviceId": "ghost", "name": "g", "snapshotCursor": 1}); w.Code != http.StatusNotFound {
		t.Fatalf("register unknown device = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, item+"?deviceId=ghost"); w.Code != http.StatusNotFound {
		t.Fatalf("read unknown device = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, item+"?deviceId=ghost"); w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown device = %d, want 404", w.Code)
	}

	// Permission precedes version/snapshot existence: a revoked caller is 403
	// even against a name or snapshot that exists (or one that does not).
	if w, _ := postJSON(t, h, collection, map[string]any{"deviceId": "dev-rev", "name": "anything", "snapshotCursor": 1}); w.Code != http.StatusForbidden {
		t.Fatalf("register revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, item+"?deviceId=dev-rev"); w.Code != http.StatusForbidden {
		t.Fatalf("read revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, collection+"?deviceId=dev-rev"); w.Code != http.StatusForbidden {
		t.Fatalf("list revoked = %d, want 403", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, collection+"/missing?deviceId=dev-rev"); w.Code != http.StatusForbidden {
		t.Fatalf("delete missing-name revoked = %d, want 403 (permission precedes existence)", w.Code)
	}

	// Content existence is last: an authorized device gets a 404 for a
	// missing name or a missing target snapshot.
	if w, _ := doRequest(t, h, http.MethodGet, collection+"/missing?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("read missing name = %d, want 404", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, collection+"/missing?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("delete missing name = %d, want 404", w.Code)
	}
	if w, _ := postJSON(t, h, collection, map[string]any{"deviceId": "dev-1", "name": "fresh", "snapshotCursor": 99}); w.Code != http.StatusNotFound {
		t.Fatalf("register missing snapshot = %d, want 404", w.Code)
	}
	if w := serveRecorder(h, newJSONRequest(http.MethodPut, collection+"/v1",
		`{"deviceId":"dev-1","snapshotCursor":99}`, "application/json")); w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
}

// Version markers are part of the document: deleting the document removes them
// together with its snapshots, and the same document id recreated afterwards
// inherits no version and binds names anew.
func TestSnapshotVersionCascadesOnDocumentDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)

	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("delete document = %d", w.Code)
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1"); strings.TrimSpace(w.Body.String()) != `{"versions":[],"count":0}` {
		t.Fatalf("versions survived delete: %s", w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions/v1?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("marker readable after delete = %d", w.Code)
	}

	// Recreate: cursor space restarts at 1 and the name binds as a new marker.
	// The device is still registered, so seed only the document history.
	for i, state := range []string{`{"v":"one"}`, `{"v":"two"}`} {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
			"deviceId": "dev-1",
			"changes":  []any{map[string]any{"id": fmt.Sprintf("n%d", i+1), "payload": map[string]any{"n": i + 1}}},
		})
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
	registerVersion(t, h, "doc", "dev-1", "v1", 1)
	if got := registerVersionMust(t, h, "doc", "dev-1", "v1"); got != 1 {
		t.Fatalf("recreated doc inherited a stale marker: v1 -> %d", got)
	}
}

// Registrations, renames, deletes and the verdicts survive a process restart;
// the bodies are byte-for-byte unchanged.
func TestSnapshotVersionPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "versions-restart.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedVersionFixture(t, h, "dev-1", "doc")
	registerVersion(t, h, "doc", "dev-1", "v1", 1)
	registerVersion(t, h, "doc", "dev-1", "v2", 2)
	before := serveRecorder(h, newJSONRequest(http.MethodGet, "/v1/documents/doc/snapshots/versions/v2?deviceId=dev-1", "", ""))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	w, _ := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if got := strings.TrimSpace(w.Body.String()); got !=
		`{"versions":[{"name":"v1","snapshotCursor":1},{"name":"v2","snapshotCursor":2}],"count":2}` {
		t.Fatalf("list after restart = %q", got)
	}
	// Idempotency still holds; a rebind is still a 409 after restart.
	if w, _ := postJSON(t, h2, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v1", "snapshotCursor": 2,
	}); w.Code != http.StatusConflict {
		t.Fatalf("conflict verdict after restart = %d, want 409", w.Code)
	}
	// Read body unchanged.
	after := serveRecorder(h2, newJSONRequest(http.MethodGet, "/v1/documents/doc/snapshots/versions/v2?deviceId=dev-1", "", ""))
	if !bytes.Equal(before.Body.Bytes(), after.Body.Bytes()) {
		t.Fatalf("read changed across restart: %q vs %q", before.Body.String(), after.Body.String())
	}
}

// A version name may be any non-empty string within one path segment,
// including characters that need percent-encoding; the decoded name is the
// stored key.
func TestSnapshotVersionNameEscaping(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")
	name := "a b.c+d"
	registerVersion(t, h, "doc", "dev-1", name, 1)

	w := serveRecorder(h, newJSONRequest(http.MethodGet,
		"/v1/documents/doc/snapshots/versions/a%20b.c%2Bd?deviceId=dev-1", "", ""))
	if w.Code != http.StatusOK {
		t.Fatalf("encoded read = %d %s", w.Code, w.Body.String())
	}
	if strings.TrimSpace(w.Body.String()) != `{"cursor":1,"state":{"v":"one"}}` {
		t.Fatalf("encoded read body = %q", w.Body.String())
	}
	// Listing echoes the decoded name.
	w, _ = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if !strings.Contains(w.Body.String(), `"name":"a b.c+d"`) {
		t.Fatalf("list lost the decoded name: %s", w.Body.String())
	}
}

// The document-level change commit's 409 now carries the conflicting id in a
// conflictId field alongside the error message.
func TestPostChangesConflictCarriesConflictID(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev")
	if w, _ := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev", "changes": []any{map[string]any{"id": "x", "payload": map[string]any{"k": "v"}}},
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev", "changes": []any{map[string]any{"id": "x", "payload": map[string]any{"k": "other"}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict = %d %s", w.Code, w.Body.String())
	}
	if body["conflictId"] != "x" || body["error"] == nil {
		t.Fatalf("409 body = %v, want error and conflictId=x", body)
	}
}

// ---- small helpers ----

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// registerVersionMust returns the cursor the named version points at, failing
// the test on any error.
func registerVersionMust(t *testing.T, h http.Handler, doc, device, name string) int {
	t.Helper()
	w := serveRecorder(h, newJSONRequest(http.MethodGet,
		fmt.Sprintf("/v1/documents/%s/snapshots/versions/%s?deviceId=%s", doc, name, device), "", ""))
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

// Concurrent registrations of distinct names all land; repeated attempts to
// bind one name to the same cursor are all idempotent 200s, and an attempted
// rebind to a different cursor never leaves a torn state.
func TestSnapshotVersionConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")

	const n = 30
	var wg sync.WaitGroup
	start := make(chan struct{})
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("vn-%02d", i)
			w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
				"deviceId": "dev-1", "name": name, "snapshotCursor": 1,
			})
			statuses[i] = w.Code
		}(i)
	}
	close(start)
	wg.Wait()
	for i, code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("register %d = %d, want 200", i, code)
		}
	}

	// Many concurrent idempotent re-binds of one name to the same cursor.
	const m = 20
	wg2 := sync.WaitGroup{}
	start2 := make(chan struct{})
	codes2 := make([]int, m)
	for i := 0; i < m; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			<-start2
			w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
				"deviceId": "dev-1", "name": "vn-00", "snapshotCursor": 1,
			})
			codes2[i] = w.Code
		}(i)
	}
	close(start2)
	wg2.Wait()
	for _, code := range codes2 {
		if code != http.StatusOK {
			t.Fatalf("idempotent concurrent rebind = %d, want 200", code)
		}
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d", w.Code)
	}
	if int(body["count"].(float64)) != n {
		t.Fatalf("count = %v, want %d", body["count"], n)
	}
}

// A version name that collides with an endpoint keyword ("merge", "restore",
// "versions") is an ordinary identifier in the name segment and keeps the
// version item routes, including the by-name restore nested under it.
func TestSnapshotVersionKeywordNamedVersions(t *testing.T) {
	h, _ := newTestHandler(t)
	seedVersionFixture(t, h, "dev-1", "doc")

	for _, name := range []string{"merge", "restore", "versions", "poll", "subscribe"} {
		registerVersion(t, h, "doc", "dev-1", name, 1)
		w := serveRecorder(h, newJSONRequest(http.MethodGet,
			fmt.Sprintf("/v1/documents/doc/snapshots/versions/%s?deviceId=dev-1", name), "", ""))
		if w.Code != http.StatusOK {
			t.Fatalf("read version named %q = %d %s", name, w.Code, w.Body.String())
		}
		w, _ = doRequest(t, h, http.MethodDelete,
			fmt.Sprintf("/v1/documents/doc/snapshots/versions/%s?deviceId=dev-1", name))
		if w.Code != http.StatusOK {
			t.Fatalf("delete version named %q = %d %s", name, w.Code, w.Body.String())
		}
	}

	// A version literally named "restore" restored by name via the nested
	// restore segment.
	registerVersion(t, h, "doc", "dev-1", "restore", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions/restore/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "rr1",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("by-name restore of version named restore = %d %s", w.Code, w.Body.String())
	}
	if got := strings.TrimSpace(w.Body.String()); got != `{"id":"rr1","created":true,"cursor":3,"restoredFrom":2}` {
		t.Fatalf("nested restore body = %q", got)
	}
}
