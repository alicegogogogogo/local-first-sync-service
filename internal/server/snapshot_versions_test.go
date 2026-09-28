package server

import (
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// ---- small path/body helpers local to the version surface --------------

func versionCollectionPath(doc string) string {
	return "/v1/documents/" + doc + "/snapshot-versions"
}

func versionItemPath(doc, name string) string {
	return "/v1/documents/" + doc + "/snapshot-versions/" + name
}

func versionRenamePath(doc, name string) string {
	return "/v1/documents/" + doc + "/snapshot-versions/" + name + "/rename"
}

func versionRestorePath(doc, name string) string {
	return "/v1/documents/" + doc + "/snapshot-versions/" + name + "/restore"
}

func sessionVersionCollectionPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshot-versions"
}

func sessionVersionItemPath(session, doc, name string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshot-versions/" + name
}

func sessionVersionRenamePath(session, doc, name string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshot-versions/" + name + "/rename"
}

func sessionVersionRestorePath(session, doc, name string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/snapshot-versions/" + name + "/restore"
}

// registerVersion registers name at cursor and asserts 200.
func registerVersion(t *testing.T, h http.Handler, doc, name string, cursor int) {
	t.Helper()
	w, _ := postJSON(t, h, versionCollectionPath(doc),
		`{"name":`+strconv.Quote(name)+`,"cursor":`+strconv.Itoa(cursor)+`}`)
	if w.Code != http.StatusOK {
		t.Fatalf("register %s@%d status = %d body = %s", name, cursor, w.Code, w.Body.String())
	}
}

// ---- registration -------------------------------------------------------

func TestSnapshotVersionRegisterAndRead(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"a":1}`, 2: `[1,2]`, 3: `null`})

	// First registration: one compact line, exactly name and cursor.
	w, body := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":2}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","cursor":2}`+"\n" {
		t.Fatalf("register body = %q", got)
	}
	// The response carries no created flag.
	if _, present := body["created"]; present {
		t.Fatalf("register body must not carry a created field: %v", body)
	}

	// The name read is byte-identical to the cursor read at cursor 2.
	byName, _ := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1"))
	byCursor, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/2")
	if byName.Code != http.StatusOK || byCursor.Code != http.StatusOK {
		t.Fatalf("read status = %d %d", byName.Code, byCursor.Code)
	}
	if byName.Body.String() != byCursor.Body.String() {
		t.Fatalf("named read %q != cursor read %q", byName.Body.String(), byCursor.Body.String())
	}
}

func TestSnapshotVersionRegisterIdempotentSameCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	first, _ := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":1}`)
	if first.Code != http.StatusOK {
		t.Fatal(first.Body.String())
	}
	// Re-post the same binding: 200, body identical to the first.
	second, _ := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":1}`)
	if second.Code != http.StatusOK {
		t.Fatalf("idempotent status = %d body = %s", second.Code, second.Body.String())
	}
	if second.Body.String() != first.Body.String() {
		t.Fatalf("idempotent body %q != first %q", second.Body.String(), first.Body.String())
	}
}

func TestSnapshotVersionRegisterConflictRebind(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{"x":1}`})
	registerVersion(t, h, "doc1", "v1", 1)

	// The same name against another cursor is a 409 and writes nothing.
	w, body := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":2}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("rebind status = %d body = %s", w.Code, w.Body.String())
	}
	if body["error"] == nil || body["name"] != "v1" {
		t.Fatalf("conflict body = %v", body)
	}
	// Zero writes: the marker still points at cursor 1.
	get, got := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1"))
	if get.Code != http.StatusOK || got["cursor"].(float64) != 1 {
		t.Fatalf("marker moved after rejected rebind: %d %v", get.Code, got)
	}
}

func TestSnapshotVersionRegisterMissingSnapshot404(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	// A cursor without a snapshot is 404.
	w, _ := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":2}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing snapshot status = %d body = %s", w.Code, w.Body.String())
	}
	// An unknown document is a 404 too.
	w, _ = postJSON(t, h, versionCollectionPath("ghost"), `{"name":"v1","cursor":1}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown doc status = %d body = %s", w.Code, w.Body.String())
	}
}

// ---- listing ------------------------------------------------------------

func TestSnapshotVersionListAscendingEmpty(t *testing.T) {
	h, _ := newTestHandler(t)

	// Empty view: [] with count 0, for a document without markers and an
	// unknown document alike.
	for _, path := range []string{
		versionCollectionPath("doc1"),
		versionCollectionPath("ghost"),
	} {
		w, _ := doRequest(t, h, http.MethodGet, path)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status = %d", path, w.Code)
		}
		if got := w.Body.String(); got != `{"versions":[],"count":0}`+"\n" {
			t.Fatalf("%s empty body = %q", path, got)
		}
	}
}

func TestSnapshotVersionListAscendingItems(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{}`, 3: `{}`})
	// Register out of name order; listing must come back ascending.
	registerVersion(t, h, "doc1", "zeta", 3)
	registerVersion(t, h, "doc1", "alpha", 1)
	registerVersion(t, h, "doc1", "middle", 2)

	w, _ := doRequest(t, h, http.MethodGet, versionCollectionPath("doc1"))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	want := `{"versions":[` +
		`{"name":"alpha","cursor":1},` +
		`{"name":"middle","cursor":2},` +
		`{"name":"zeta","cursor":3}` +
		`],"count":3}` + "\n"
	if got := w.Body.String(); got != want {
		t.Fatalf("list = %q\nwant %q", got, want)
	}
}

// ---- rename -------------------------------------------------------------

func TestSnapshotVersionRenameMovesMarker(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{"v":2}`})
	registerVersion(t, h, "doc1", "v1", 1)

	w, _ := postJSON(t, h, versionRenamePath("doc1", "v1"), `{"cursor":2}`)
	if w.Code != http.StatusOK {
		t.Fatalf("rename status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","cursor":2}`+"\n" {
		t.Fatalf("rename body = %q", got)
	}
	// The name now reads the snapshot at cursor 2.
	get, body := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1"))
	if get.Code != http.StatusOK || body["cursor"].(float64) != 2 {
		t.Fatalf("after rename = %d %v", get.Code, body)
	}
	// Snapshots themselves are untouched.
	for _, c := range []int{1, 2} {
		snap, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/"+strconv.Itoa(c))
		if snap.Code != http.StatusOK {
			t.Fatalf("snapshot %d changed after rename = %d", c, snap.Code)
		}
	}
}

func TestSnapshotVersionRenameIdempotentAndMisses(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{}`})
	registerVersion(t, h, "doc1", "v1", 1)

	// Rebinding to the cursor already bound is idempotent.
	w, first := postJSON(t, h, versionRenamePath("doc1", "v1"), `{"cursor":1}`)
	if w.Code != http.StatusOK || first["cursor"].(float64) != 1 {
		t.Fatalf("same-cursor rename = %d %s", w.Code, w.Body.String())
	}

	// Target snapshot missing: 404, zero writes.
	w, _ = postJSON(t, h, versionRenamePath("doc1", "v1"), `{"cursor":9}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d", w.Code)
	}
	// Unknown name: 404.
	w, _ = postJSON(t, h, versionRenamePath("doc1", "ghost"), `{"cursor":1}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename unknown name = %d", w.Code)
	}
	// Marker still at 1.
	get, body := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1"))
	if get.Code != 200 || body["cursor"].(float64) != 1 {
		t.Fatalf("marker changed after failed renames = %v", body)
	}
}

// ---- delete -------------------------------------------------------------

func TestSnapshotVersionDeleteFreesName(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{}`})
	registerVersion(t, h, "doc1", "v1", 1)

	w, _ := doRequest(t, h, http.MethodDelete, versionItemPath("doc1", "v1"))
	if w.Code != http.StatusOK {
		t.Fatalf("delete status = %d body = %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"name":"v1","deleted":true}`+"\n" {
		t.Fatalf("delete body = %q", got)
	}
	// Name read misses.
	if get, _ := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1")); get.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d", get.Code)
	}
	// Repeat delete is 404.
	if w2, _ := doRequest(t, h, http.MethodDelete, versionItemPath("doc1", "v1")); w2.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d", w2.Code)
	}
	// The name is reusable: registration creates it again, now at cursor 2.
	w3, _ := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":2}`)
	if w3.Code != http.StatusOK {
		t.Fatalf("reregister after delete = %d %s", w3.Code, w3.Body.String())
	}
	// The snapshot it pointed at is untouched.
	if snap, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/1"); snap.Code != http.StatusOK {
		t.Fatalf("snapshot removed with marker = %d", snap.Code)
	}
}

// ---- restore by name ----------------------------------------------------

func TestSnapshotVersionRestoreByNameAppendsChange(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshot(t, h, "doc1", 2, `{"text":"hello","n":1}`)
	registerVersion(t, h, "doc1", "v1", 2)

	w, body := postJSON(t, h, versionRestorePath("doc1", "v1"),
		`{"deviceId":"dev-A","changeId":"r-1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("restore status = %d body = %s", w.Code, w.Body.String())
	}
	if body["id"] != "r-1" || body["created"] != true ||
		body["cursor"].(float64) != 3 || body["restoredFrom"].(float64) != 2 {
		t.Fatalf("restore body = %v", body)
	}
	// The appended change carries the named snapshot's state.
	list, lb := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?after=2")
	if list.Code != http.StatusOK {
		t.Fatal(list.Body.String())
	}
	row := lb["changes"].([]any)[0].(map[string]any)
	if row["id"] != "r-1" || row["deviceId"] != "dev-A" ||
		row["payload"].(map[string]any)["text"] != "hello" {
		t.Fatalf("appended row = %v", row)
	}
}

func TestSnapshotVersionRestoreUnknownName404(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshot(t, h, "doc1", 1, `{}`)

	w, _ := postJSON(t, h, versionRestorePath("doc1", "ghost"),
		`{"deviceId":"dev","changeId":"r"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown name restore = %d %s", w.Code, w.Body.String())
	}
	// Unknown document too.
	w, _ = postJSON(t, h, versionRestorePath("ghost", "v1"),
		`{"deviceId":"dev","changeId":"r"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown doc restore = %d", w.Code)
	}
}

func TestSnapshotVersionRestoreConflictSharesRules(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`, 2: `{}`})
	registerVersion(t, h, "doc1", "v1", 1)
	registerVersion(t, h, "doc1", "v2", 2)

	// An ordinary change occupies change id "ord".
	ord, _ := postJSON(t, h, "/v1/documents/doc1/changes",
		`{"deviceId":"dev","changes":[{"id":"ord","payload":{}}]}`)
	if ord.Code != http.StatusOK {
		t.Fatal(ord.Body.String())
	}
	// Restoring by name with that id is 409 with the conflict id.
	w, body := postJSON(t, h, versionRestorePath("doc1", "v1"),
		`{"deviceId":"dev","changeId":"ord"}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("ordinary-id restore = %d %s", w.Code, w.Body.String())
	}
	if body["conflictId"] != "ord" {
		t.Fatalf("conflict body = %v", body)
	}

	// First restore with a fresh id succeeds; repeating with the same name is
	// idempotent (same cursor, created false).
	first, fb := postJSON(t, h, versionRestorePath("doc1", "v2"),
		`{"deviceId":"dev","changeId":"r2"}`)
	if first.Code != http.StatusOK || fb["created"] != true {
		t.Fatalf("first restore = %d %v", first.Code, fb)
	}
	again, ab := postJSON(t, h, versionRestorePath("doc1", "v2"),
		`{"deviceId":"dev","changeId":"r2"}`)
	if again.Code != http.StatusOK || ab["created"] != false ||
		ab["cursor"] != fb["cursor"] || ab["restoredFrom"].(float64) != 2 {
		t.Fatalf("idempotent restore = %d %v", again.Code, ab)
	}
}

// ---- shape enforcement --------------------------------------------------

func TestSnapshotVersionMalformedPaths(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	cases := []struct{ method, path string }{
		// Collection shape.
		{http.MethodPut, versionCollectionPath("doc1")},
		{http.MethodPatch, versionCollectionPath("doc1")},
		{http.MethodDelete, versionCollectionPath("doc1")},
		// Item shape.
		{http.MethodPost, versionItemPath("doc1", "v1")},
		{http.MethodPut, versionItemPath("doc1", "v1")},
		{http.MethodPatch, versionItemPath("doc1", "v1")},
		// Trailing slash / empty segments.
		{http.MethodGet, "/v1/documents/doc1/snapshot-versions/"},
		{http.MethodGet, "/v1/documents//snapshot-versions"},
		{http.MethodGet, versionItemPath("doc1", "v1") + "/"},
		// Extra segments / unknown subresource.
		{http.MethodGet, versionItemPath("doc1", "v1") + "/extra"},
		{http.MethodGet, versionRestorePath("doc1", "v1") + "/extra"},
		{http.MethodPost, versionItemPath("doc1", "v1") + "/bogus"},
		// rename/restore are POST-only.
		{http.MethodGet, versionRenamePath("doc1", "v1")},
		{http.MethodDelete, versionRenamePath("doc1", "v1")},
		{http.MethodGet, versionRestorePath("doc1", "v1")},
		{http.MethodPut, versionRestorePath("doc1", "v1")},
	}
	for _, c := range cases {
		w, _ := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, body = %q", c.method, c.path, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("%s %s content type = %q", c.method, c.path, ct)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") ||
			strings.Contains(w.Body.String(), "Method Not Allowed") {
			t.Fatalf("%s %s leaked non-JSON body: %q", c.method, c.path, w.Body.String())
		}
	}
}

func TestSnapshotVersionRegisterBadBody(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})

	bodies := []string{
		`{"cursor":1}`,                // missing name
		`{"name":"v1"}`,               // missing cursor
		`{"name":"","cursor":1}`,      // empty name
		`{"name":"v1","cursor":0}`,    // zero cursor
		`{"name":"v1","cursor":-1}`,   // negative
		`{"name":"v1","cursor":"1"}`,  // string cursor
		`{"name":"v1","cursor":1.5}`,  // fraction
		`{"name":"v1","cursor":true}`, // boolean
		`{"name":"v1","cursor":null}`, // null
		`{"name":123,"cursor":1}`,     // numeric name
		`not json`,                    // malformed
		`{"name":"v1","cursor":1}x`,   // trailing content
	}
	for _, body := range bodies {
		w, _ := postJSON(t, h, versionCollectionPath("doc1"), body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want 400, %s", body, w.Code, w.Body.String())
		}
	}
	// Rename body shares the cursor rule.
	w, _ := postJSON(t, h, versionRenamePath("doc1", "v1"), `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("rename missing cursor = %d", w.Code)
	}
}

// ---- persistence --------------------------------------------------------

func TestSnapshotVersionPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "versions.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"a":1}`, 2: `[1,2]`})
	registerVersion(t, h, "doc1", "v1", 1)
	registerVersion(t, h, "doc1", "v2", 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// List is identical.
	w, _ := doRequest(t, h2, http.MethodGet, versionCollectionPath("doc1"))
	if w.Code != http.StatusOK {
		t.Fatalf("list after restart = %d", w.Code)
	}
	want := `{"versions":[{"name":"v1","cursor":1},{"name":"v2","cursor":2}],"count":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("list after restart = %q\nwant %q", w.Body.String(), want)
	}
	// Named read stays byte-identical to the cursor read after restart.
	named, _ := doRequest(t, h2, http.MethodGet, versionItemPath("doc1", "v2"))
	cursorRead, _ := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/snapshots/2")
	if named.Body.String() != cursorRead.Body.String() {
		t.Fatalf("named %q vs cursor %q", named.Body.String(), cursorRead.Body.String())
	}
	// Conflict and miss judgments survive.
	conflict, _ := postJSON(t, h2, versionCollectionPath("doc1"), `{"name":"v1","cursor":2}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("rebind after restart = %d", conflict.Code)
	}
	if miss, _ := doRequest(t, h2, http.MethodGet, versionItemPath("doc1", "nope")); miss.Code != http.StatusNotFound {
		t.Fatalf("miss after restart = %d", miss.Code)
	}
}

// ---- document deletion clears versions ----------------------------------

func TestSnapshotVersionClearedOnDocumentDelete(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedSnapshots(t, h, "doc1", map[int]string{1: `{}`})
	registerVersion(t, h, "doc1", "v1", 1)

	w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc1?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", w.Code, w.Body.String())
	}
	// List is empty; the name misses.
	list, _ := doRequest(t, h, http.MethodGet, versionCollectionPath("doc1"))
	if list.Body.String() != `{"versions":[],"count":0}`+"\n" {
		t.Fatalf("list after delete = %q", list.Body.String())
	}
	if get, _ := doRequest(t, h, http.MethodGet, versionItemPath("doc1", "v1")); get.Code != http.StatusNotFound {
		t.Fatalf("named read after delete = %d", get.Code)
	}

	// Re-create the same document: no inherited marker; a fresh snapshot
	// registers the name as a brand-new marker.
	seedSnapshots(t, h, "doc1", map[int]string{1: `{"fresh":true}`})
	reg, _ := postJSON(t, h, versionCollectionPath("doc1"), `{"name":"v1","cursor":1}`)
	if reg.Code != http.StatusOK {
		t.Fatalf("register on rebuilt doc = %d %s", reg.Code, reg.Body.String())
	}
}

// ---- long poll on a deleted document ------------------------------------

// A long poll parked on a document when it is deleted must wake immediately
// and answer as it does for any now-unknown document: empty changes, cursor 0,
// timedOut false.
func TestPollDeletedDocumentReturnsEmpty(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedDoc(t, h, "doc1", 1)

	var wg sync.WaitGroup
	wg.Add(1)
	var (
		code     int
		respBody map[string]any
	)
	go func() {
		defer wg.Done()
		rr, body := doRequest(t, h, http.MethodGet,
			"/v1/documents/doc1/changes/poll?after=1&waitMs=30000")
		code = rr.Code
		respBody = body
	}()
	// Let the poll park, then delete the document.
	time.Sleep(100 * time.Millisecond)
	del, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc1?deviceId=dev-1")
	if del.Code != http.StatusOK {
		t.Fatalf("delete = %d %s", del.Code, del.Body.String())
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not wake after document deletion")
	}
	if code != http.StatusOK {
		t.Fatalf("poll status = %d", code)
	}
	if len(respBody["changes"].([]any)) != 0 ||
		respBody["nextCursor"].(float64) != 0 ||
		respBody["timedOut"] != false {
		t.Fatalf("deleted-doc poll page = %v", respBody)
	}
}

// ---- conflict body on the ordinary commit -------------------------------

func TestPostChangesConflictBodyNamesId(t *testing.T) {
	h, _ := newTestHandler(t)
	first, _ := postJSON(t, h, "/v1/documents/doc1/changes",
		`{"deviceId":"dev","changes":[{"id":"c1","payload":{"v":1}}]}`)
	if first.Code != http.StatusOK {
		t.Fatal(first.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc1/changes",
		`{"deviceId":"dev","changes":[{"id":"c1","payload":{"v":2}}]}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("status = %d %s", w.Code, w.Body.String())
	}
	if body["conflictId"] != "c1" || body["error"] == nil {
		t.Fatalf("409 body = %v", body)
	}
}
