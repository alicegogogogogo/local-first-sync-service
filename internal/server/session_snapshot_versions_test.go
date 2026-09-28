package server

import (
	"net/http"
	"testing"
)

// seedSessionVersions sets up a registered device, its session, a document
// with two snapshots, and registers one version at cursor 1.
func seedSessionVersions(t *testing.T, h http.Handler, device, session, doc string) {
	t.Helper()
	createSessionViaHTTP(t, h, device, session)
	seedSnapshots(t, h, doc, map[int]string{1: `{"v":1}`, 2: `{"v":2}`})
	registerVersion(t, h, doc, "base", 1)
}

func TestSessionVersionListAndRead(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	w, _ := doRequest(t, h, http.MethodGet, sessionVersionCollectionPath("sess", "doc1"))
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"versions":[{"name":"base","cursor":1}],"count":1}`+"\n" {
		t.Fatalf("list body = %q", got)
	}

	// The session named read is byte-identical to the cursor read.
	byName, _ := doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess", "doc1", "base"))
	byCursor, _ := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/1")
	if byName.Code != http.StatusOK || byName.Body.String() != byCursor.Body.String() {
		t.Fatalf("named %q vs cursor %q", byName.Body.String(), byCursor.Body.String())
	}
}

func TestSessionVersionRegister(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	// Register through the session path.
	w, body := postJSON(t, h, sessionVersionCollectionPath("sess", "doc1"), `{"name":"v2","cursor":2}`)
	if w.Code != http.StatusOK {
		t.Fatalf("register = %d %s", w.Code, w.Body.String())
	}
	if w.Body.String() != `{"name":"v2","cursor":2}`+"\n" {
		t.Fatalf("register body = %q", w.Body.String())
	}
	if _, ok := body["created"]; ok {
		t.Fatalf("register must not return a created field")
	}
	// Same binding again: idempotent, same body.
	again, _ := postJSON(t, h, sessionVersionCollectionPath("sess", "doc1"), `{"name":"v2","cursor":2}`)
	if again.Code != http.StatusOK || again.Body.String() != w.Body.String() {
		t.Fatalf("idempotent = %d %q", again.Code, again.Body.String())
	}
	// Rebind to another cursor: 409, zero writes.
	conflict, cb := postJSON(t, h, sessionVersionCollectionPath("sess", "doc1"), `{"name":"v2","cursor":1}`)
	if conflict.Code != http.StatusConflict || cb["name"] != "v2" {
		t.Fatalf("rebind = %d %v", conflict.Code, cb)
	}
}

func TestSessionVersionCheckOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")
	// A second, revoked device with its own session.
	createSessionViaHTTP(t, h, "dev-revoked", "sess-revoked")
	revoke, _ := postJSON(t, h, "/v1/documents/doc1/permissions",
		`{"deviceId":"dev-revoked","action":"revoke"}`)
	if revoke.Code != http.StatusOK {
		t.Fatal(revoke.Body.String())
	}

	// 1. Bad shape beats every other verdict: a malformed body against an
	// unknown session is still 400.
	w, _ := postJSON(t, h, sessionVersionCollectionPath("ghost-session", "doc1"), `{"cursor":1}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape vs unknown session = %d, want 400", w.Code)
	}

	// 2. Session existence (404).
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionCollectionPath("ghost-session", "doc1"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown session = %d, want 404", w.Code)
	}

	// 3. Document permission (403), ahead of version existence.
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess-revoked", "doc1", "base"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d, want 403, %s", w.Code, w.Body.String())
	}
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess-revoked", "doc1", "missing"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked vs missing name = %d, want 403", w.Code)
	}

	// 4. Version/snapshot existence (404) for an authorized session.
	w, _ = doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess", "doc1", "missing"))
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing name = %d, want 404", w.Code)
	}
	w, _ = postJSON(t, h, sessionVersionRenamePath("sess", "doc1", "missing"), `{"cursor":2}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename missing name = %d, want 404", w.Code)
	}
	w, _ = postJSON(t, h, sessionVersionRenamePath("sess", "doc1", "base"), `{"cursor":9}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("rename to missing snapshot = %d, want 404", w.Code)
	}
}

func TestSessionVersionRenameDeleteAndRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	// Rename base 1 -> 2 through the session path.
	w, _ := postJSON(t, h, sessionVersionRenamePath("sess", "doc1", "base"), `{"cursor":2}`)
	if w.Code != http.StatusOK || w.Body.String() != `{"name":"base","cursor":2}`+"\n" {
		t.Fatalf("rename = %d %q", w.Code, w.Body.String())
	}

	// Restore by name: appends an ordinary change as the session's device.
	w, body := postJSON(t, h, sessionVersionRestorePath("sess", "doc1", "base"), `{"changeId":"sr-1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}
	if body["id"] != "sr-1" || body["created"] != true ||
		body["cursor"].(float64) != 3 || body["restoredFrom"].(float64) != 2 {
		t.Fatalf("restore body = %v", body)
	}
	// Idempotent repeat: same cursor, created false.
	again, ab := postJSON(t, h, sessionVersionRestorePath("sess", "doc1", "base"), `{"changeId":"sr-1"}`)
	if again.Code != http.StatusOK || ab["created"] != false || ab["cursor"].(float64) != 3 {
		t.Fatalf("idempotent restore = %d %v", again.Code, ab)
	}

	// Delete the marker; repeat delete and name read are 404.
	del, _ := doRequest(t, h, http.MethodDelete, sessionVersionItemPath("sess", "doc1", "base"))
	if del.Code != http.StatusOK || del.Body.String() != `{"name":"base","deleted":true}`+"\n" {
		t.Fatalf("delete = %d %q", del.Code, del.Body.String())
	}
	if again2, _ := doRequest(t, h, http.MethodDelete, sessionVersionItemPath("sess", "doc1", "base")); again2.Code != http.StatusNotFound {
		t.Fatalf("repeat delete = %d", again2.Code)
	}
	if get, _ := doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess", "doc1", "base")); get.Code != http.StatusNotFound {
		t.Fatalf("read after delete = %d", get.Code)
	}
}

func TestSessionVersionRestoreConflict409(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	// Occupy change id "ord" with an ordinary change via the session commit.
	ord, _ := postJSON(t, h, "/v1/sessions/sess/documents/doc1/changes",
		`{"changes":[{"id":"ord","payload":{}}]}`)
	if ord.Code != http.StatusOK {
		t.Fatal(ord.Body.String())
	}
	w, body := postJSON(t, h, sessionVersionRestorePath("sess", "doc1", "base"), `{"changeId":"ord"}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "ord" {
		t.Fatalf("ordinary id restore = %d %v", w.Code, body)
	}
	// Unknown name is 404.
	w, _ = postJSON(t, h, sessionVersionRestorePath("sess", "doc1", "nope"), `{"changeId":"x"}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown name = %d", w.Code)
	}
}

// A version name may itself spell another endpoint's keyword; inside the
// versions subtree it is always an ordinary identifier.
func TestSessionVersionKeywordNamesAreIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	for _, name := range []string{"restore", "poll", "compact", "merge", "crdt", "subscribe", "replay"} {
		w, _ := postJSON(t, h, sessionVersionCollectionPath("sess", "doc1"),
			`{"name":"`+name+`","cursor":1}`)
		if w.Code != http.StatusOK {
			t.Fatalf("register keyword name %q = %d %s", name, w.Code, w.Body.String())
		}
		if get, _ := doRequest(t, h, http.MethodGet, sessionVersionItemPath("sess", "doc1", name)); get.Code != http.StatusOK {
			t.Fatalf("read keyword name %q = %d", name, get.Code)
		}
		if r, _ := postJSON(t, h, sessionVersionRestorePath("sess", "doc1", name), `{"changeId":"r-`+name+`"}`); r.Code != http.StatusOK {
			t.Fatalf("restore by keyword name %q = %d %s", name, r.Code, r.Body.String())
		}
	}
}

func TestSessionVersionMalformedPaths(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	cases := []struct{ method, path string }{
		{http.MethodPut, sessionVersionCollectionPath("sess", "doc1")},
		{http.MethodDelete, sessionVersionCollectionPath("sess", "doc1")},
		{http.MethodPost, sessionVersionItemPath("sess", "doc1", "base")},
		{http.MethodPut, sessionVersionItemPath("sess", "doc1", "base")},
		{http.MethodGet, sessionVersionItemPath("sess", "doc1", "base") + "/"},
		{http.MethodGet, sessionVersionItemPath("sess", "doc1", "base") + "/extra"},
		{http.MethodGet, sessionVersionRenamePath("sess", "doc1", "base")},
		{http.MethodDelete, sessionVersionRenamePath("sess", "doc1", "base")},
		{http.MethodGet, sessionVersionRestorePath("sess", "doc1", "base")},
		{http.MethodPut, sessionVersionRestorePath("sess", "doc1", "base")},
		{http.MethodPost, sessionVersionItemPath("sess", "doc1", "base") + "/bogus"},
		// Empty identifiers (doubled slash / trailing slash).
		{http.MethodGet, "/v1/sessions//documents/doc1/snapshot-versions"},
		{http.MethodGet, "/v1/sessions/sess/documents//snapshot-versions"},
		{http.MethodGet, sessionVersionCollectionPath("sess", "doc1") + "/"},
	}
	for _, c := range cases {
		w, _ := doRequest(t, h, c.method, c.path)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s %s = %d, want 400, body = %q", c.method, c.path, w.Code, w.Body.String())
		}
	}
}

func TestSessionVersionRegisterBadBody(t *testing.T) {
	h, _ := newTestHandler(t)
	seedSessionVersions(t, h, "dev", "sess", "doc1")

	for _, body := range []string{
		`{"cursor":1}`, `{"name":""}`, `{"name":"x","cursor":0}`,
		`{"name":"x","cursor":"1"}`, `{"name":"x","cursor":-2}`, `garbage`,
	} {
		w, _ := postJSON(t, h, sessionVersionCollectionPath("sess", "doc1"), body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body %q = %d", body, w.Code)
		}
	}
	// The session named restore takes only changeId; a stray deviceId is
	// ignored like other unknown fields, but a missing/empty changeId is 400.
	w, _ := postJSON(t, h, sessionVersionRestorePath("sess", "doc1", "base"), `{}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("restore missing changeId = %d", w.Code)
	}
}
