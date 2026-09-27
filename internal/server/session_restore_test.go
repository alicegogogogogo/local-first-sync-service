package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionRestorePath is the session-scoped history restore path.
func sessionRestorePath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/restore"
}

// postSessionRestore posts one restore request to the session-scoped restore
// entry.
func postSessionRestore(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionRestorePath(session, doc), body)
}

func TestSessionRestoreHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"text":"hello","n":1}`)

	w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId":       "restore-1",
		"snapshotCursor": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"id":"restore-1","created":true,"cursor":3,"restoredFrom":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}
	if body["id"] != "restore-1" || body["created"] != true ||
		body["cursor"].(float64) != 3 || body["restoredFrom"].(float64) != 2 {
		t.Fatalf("restore body = %v", body)
	}

	// The appended change is ordinary, carries the snapshot state and is
	// attributed to the session's owning device.
	w, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?after=2")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["id"] != "restore-1" || row["deviceId"] != "dev" || row["cursor"].(float64) != 3 {
		t.Fatalf("restored change = %v", row)
	}
	state := row["payload"].(map[string]any)
	if state["text"] != "hello" || state["n"].(float64) != 1 {
		t.Fatalf("restored payload = %v", row["payload"])
	}
	if list["nextCursor"].(float64) != 3 {
		t.Fatalf("nextCursor = %v", list["nextCursor"])
	}

	// Old rows are untouched and the snapshot is still readable.
	w, snap := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/2")
	if w.Code != http.StatusOK || snap["state"].(map[string]any)["text"] != "hello" {
		t.Fatalf("snapshot after restore = %d %v", w.Code, snap)
	}
}

// The success body is byte-identical to the document-level restore's for the
// same content.
func TestSessionRestoreHTTPSuccessShapeMatchesDocumentRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "docA", 1, `{"k":1}`)
	seedSnapshot(t, h, "docB", 1, `{"k":1}`)

	wDoc, _ := restoreJSON(t, h, "docA", map[string]any{
		"deviceId":       "dev",
		"changeId":       "r1",
		"snapshotCursor": 1,
	})
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionRestore(t, h, "sess", "docB", map[string]any{
		"changeId":       "r1",
		"snapshotCursor": 1,
	})
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}

// A stray deviceId in the body is ignored: the calling device is always the
// session's owning device.
func TestSessionRestoreHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"k":1}`)

	w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"deviceId":       "intruder",
		"changeId":       "r1",
		"snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	row := list["changes"].([]any)[1].(map[string]any)
	if row["deviceId"] != "dev" {
		t.Fatalf("stored device = %v, want the session device dev", row["deviceId"])
	}
}

func TestSessionRestoreHTTPIdempotentAndConflict(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":1}`)
	// A second snapshot with a different state for the mismatch cases.
	w, _ := postJSON(t, h, "/v1/documents/doc1/snapshots", `{"cursor":1,"state":{"v":9}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// First restore: created at cursor 3.
	w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != true || body["cursor"].(float64) != 3 {
		t.Fatalf("first restore = %d %v", w.Code, body)
	}

	// Identical repeat: 200, created=false, the first cursor.
	w, body = postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 3 ||
		body["restoredFrom"].(float64) != 2 {
		t.Fatalf("idempotent restore = %d %v", w.Code, body)
	}

	// Provenance is shared with the document-level entry: a document-level
	// restore with the same device and snapshot is the same idempotent call,
	// while a different device is a conflict.
	w, body = restoreJSON(t, h, "doc1", map[string]any{
		"deviceId": "dev", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 3 {
		t.Fatalf("cross-entry idempotent restore = %d %v", w.Code, body)
	}
	w, _ = restoreJSON(t, h, "doc1", map[string]any{
		"deviceId": "other", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("cross-entry device conflict = %d body = %s", w.Code, w.Body.String())
	}

	before, beforeNext, _ := s.ListChanges("doc1", 0, 100)

	// A different snapshot cursor conflicts even though that snapshot holds a
	// different state: idempotency requires device, snapshotCursor and the
	// source state all to match.
	for _, tc := range []struct {
		name string
		body any
	}{
		{"snapshotCursor mismatch", map[string]any{"changeId": "r1", "snapshotCursor": 1}},
		{"ordinary change occupies id", map[string]any{"changeId": "c1", "snapshotCursor": 2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, body := postSessionRestore(t, h, "sess", "doc1", tc.body)
			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409, body = %s", w.Code, w.Body.String())
			}
			if body["error"] == nil {
				t.Fatalf("body = %s", w.Body.String())
			}
		})
	}

	// Every conflict and the idempotent repeats left the log unchanged.
	after, afterNext, _ := s.ListChanges("doc1", 0, 100)
	if len(after) != len(before) || afterNext != beforeNext {
		t.Fatalf("writes leaked: before=%d/%d after=%d/%d", len(before), beforeNext, len(after), afterNext)
	}
}

// A second session on another device restoring the same id conflicts: the
// source device is part of the idempotency key.
func TestSessionRestoreHTTPConflictAcrossDevices(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev-1", "sess-1")
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	seedSnapshot(t, h, "doc1", 1, `{"k":1}`)

	w, body := postSessionRestore(t, h, "sess-1", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("first restore = %d %v", w.Code, body)
	}
	w, _ = postSessionRestore(t, h, "sess-2", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("cross-device restore = %d body = %s", w.Code, w.Body.String())
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows = %d next = %d", len(rows), next)
	}
}

func TestSessionRestoreHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":2}`)
	url := sessionRestorePath("sess", "doc1")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"changeId":"c","snapshotCursor":2}`},
		{"missing content type", "", `{"changeId":"c","snapshotCursor":2}`},
		{"json suffix content type", "application/vnd.api+json", `{"changeId":"c","snapshotCursor":2}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"changeId":"c","snapshotCursor":2}garbage`},
		{"second json value", "application/json", `{"changeId":"c","snapshotCursor":2}{}`},
		{"missing changeId", "application/json", `{"snapshotCursor":2}`},
		{"empty changeId", "application/json", `{"changeId":"","snapshotCursor":2}`},
		{"numeric changeId", "application/json", `{"changeId":7,"snapshotCursor":2}`},
		{"missing snapshotCursor", "application/json", `{"changeId":"c"}`},
		{"null snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":null}`},
		{"zero snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":0}`},
		{"negative snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":-1}`},
		{"fractional snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":2.5}`},
		{"string snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":"2"}`},
		{"boolean snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":true}`},
		{"body not an object", "application/json", `[1,2,3]`},
		{"stray deviceId cannot fix shape", "application/json", `{"deviceId":"dev","changeId":"c"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, url, tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Zero writes: none of the rejected requests appended a change.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestSessionRestoreHTTPSnapshotMiss(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":2}`)

	for _, tc := range []struct {
		name string
		doc  string
	}{
		{"unknown document", "ghost"},
		{"cursor without snapshot", "doc1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor := 1
			if tc.name == "cursor without snapshot" {
				cursor = 1 // snapshot exists only at cursor 2
			}
			w, body := postSessionRestore(t, h, "sess", tc.doc, map[string]any{
				"changeId": "c", "snapshotCursor": cursor,
			})
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
			}
			if body["error"] == nil {
				t.Fatalf("body = %s", w.Body.String())
			}
		})
	}

	// A cursor ahead of every snapshot also misses.
	w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "c", "snapshotCursor": 99,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("ahead cursor = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestSessionRestoreHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)
	goodBody := map[string]any{"changeId": "r1", "snapshotCursor": 1}

	// Never-created session -> 404 JSON with no result.
	w, body := postSessionRestore(t, h, "ghost", "doc1", goodBody)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["id"] != nil {
		t.Fatalf("404 leaked a result: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed restore against an
	// unknown session is still a 400.
	w, _ = postSessionRestore(t, h, "ghost", "doc1", map[string]any{"changeId": "r1"})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> restores through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionRestore(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionRestoreHTTPPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)
	goodBody := map[string]any{"changeId": "r1", "snapshotCursor": 1}

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no result and zero writes, even though the
	// snapshot exists.
	w, body := postSessionRestore(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["id"] != nil {
		t.Fatalf("403 leaked a result: %s", w.Body.String())
	}

	// Session existence precedes the permission verdict: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionRestore(t, h, "ghost", "doc1", goodBody)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}

	// Re-grant restores the restore path.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev", "action": "grant",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionRestore(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusOK || body["created"] != true || body["cursor"].(float64) != 2 {
		t.Fatalf("re-granted restore = %d %v", w.Code, body)
	}
}

func TestSessionRestoreHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	body := `{"changeId":"r1","snapshotCursor":1}`

	// Verbs other than POST on the restore path are a JSON 400.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		r := newJSONRequest(method, sessionRestorePath("sess", "doc1"), body, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash, extra segments and a restore placed
	// under the changes collection are a JSON 400, never a redirect or HTML.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/restore",
		"/v1/sessions/sess/documents//restore",
		"/v1/sessions/sess/documents/doc1/restore/",
		"/v1/sessions/sess/documents/doc1/restore/extra",
		"/v1/sessions/sess/documents/doc1/restore/extra/more",
		"/v1/sessions/sess/documents/doc1/changes/restore",
		"/v1/sessions/sess/wrong/doc1/restore",
	} {
		r := newJSONRequest(http.MethodPost, p, body, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("POST %s status = %d, want 400, body = %q", p, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
		if loc := w.Header().Get("Location"); loc != "" {
			t.Fatalf("POST %s redirected to %q", p, loc)
		}
		if strings.Contains(strings.ToLower(w.Body.String()), "<html") {
			t.Fatalf("POST %s leaked HTML: %q", p, w.Body.String())
		}
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

// A session or document literally named "restore" is an ordinary identifier:
// the restore runs normally, with no redirect and no HTML.
func TestSessionRestoreHTTPRestoreNamedIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "restore")
	seedSnapshot(t, h, "restore", 1, `{"k":1}`)

	w, body := postSessionRestore(t, h, "restore", "restore", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["created"] != true || body["cursor"].(float64) != 2 || body["restoredFrom"].(float64) != 1 {
		t.Fatalf("restore-named result = %v", body)
	}
}

// The session restore shares the one contiguous cursor space with the ordinary
// commit, session batch commit, replay and merge entries.
func TestSessionRestoreHTTPSharedCursorSpace(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 1) // c1 @1
	w, _ := postJSON(t, h, "/v1/documents/doc1/snapshots", `{"cursor":1,"state":{"s":1}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Session restore takes cursor 2.
	w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["cursor"].(float64) != 2 {
		t.Fatalf("session restore = %d %v", w.Code, body)
	}

	// A document-level ordinary commit continues at cursor 3.
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d1", "payload": map[string]any{"n": 3}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A session batch commit continues at cursor 4.
	w, _ = postJSON(t, h, "/v1/sessions/sess/documents/doc1/changes", map[string]any{
		"changes": []any{map[string]any{"id": "b1", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A second session restore continues at cursor 5.
	w, body = postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r2", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["cursor"].(float64) != 5 {
		t.Fatalf("second restore = %d %v", w.Code, body)
	}

	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	rows := list["changes"].([]any)
	if len(rows) != 5 || list["nextCursor"].(float64) != 5 {
		t.Fatalf("rows = %d nextCursor = %v", len(rows), list["nextCursor"])
	}
}

func TestSessionRestoreHTTPWakesLongPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"n":1}`)

	done := make(chan map[string]any, 1)
	go func() {
		// Park on the session-scoped long poll.
		w, body := doRequest(t, h, http.MethodGet,
			"/v1/sessions/sess/documents/doc1/changes/poll?after=1&waitMs=30000")
		if w.Code != http.StatusOK {
			t.Errorf("poll status = %d body = %s", w.Code, w.Body.String())
		}
		done <- body
	}()

	time.Sleep(50 * time.Millisecond)
	w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "live", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	select {
	case body := <-done:
		rows := body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "live" {
			t.Fatalf("polled rows = %v", rows)
		}
		if body["timedOut"] != false {
			t.Fatalf("poll timed out despite the restore: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session restore did not wake the parked poll")
	}
}

func TestSessionRestoreHTTPPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)
	if code := postHTTP(t, srv, "/v1/documents/doc/snapshots", map[string]any{
		"cursor": 1, "state": map[string]any{"n": 1},
	}); code != http.StatusOK {
		t.Fatalf("snapshot status = %d", code)
	}

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// A restore committed through the session-scoped entry is pushed like any
	// other write path.
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/restore", map[string]any{
		"changeId": "sess-r", "snapshotCursor": 1,
	}); code != http.StatusOK {
		t.Fatalf("session restore status = %d", code)
	}
	c := conn.readChange()
	if c.Cursor != 2 || c.ID != "sess-r" || c.DeviceID != "dev-1" {
		t.Fatalf("pushed frame = %+v", c)
	}

	// An idempotent repeat allocates no cursor and pushes nothing.
	conn.setReadDeadline(300 * time.Millisecond)
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/restore", map[string]any{
		"changeId": "sess-r", "snapshotCursor": 1,
	}); code != http.StatusOK {
		t.Fatalf("idempotent restore status = %d", code)
	}
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("idempotent restore produced a push")
	}
	conn.clearReadDeadline()
}

// A permission revoke while a subscription is open still ends it with 4403,
// including for clients restoring through the session entry.
func TestSessionRestoreHTTPSubscriptionCloses4403OnRevoke(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke status = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}
}

func TestSessionRestoreHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
				"changeId": fmt.Sprintf("r%02d", i), "snapshotCursor": 1,
			})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("restore %d status %d: %s", i, w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// 1 seeded change + n restores; cursors contiguous 1..n+1, none repeated.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != n+1 || body["nextCursor"].(float64) != n+1 {
		t.Fatalf("stored %d changes, nextCursor = %v, want %d", len(rows), body["nextCursor"], n+1)
	}
	seen := make(map[float64]bool, n)
	for _, row := range rows {
		cursor := row.(map[string]any)["cursor"].(float64)
		if seen[cursor] {
			t.Fatalf("cursor %v allocated twice", cursor)
		}
		seen[cursor] = true
	}
}

// Concurrent identical restores take effect exactly once: one created=true at
// one cursor, every other answer created=false at that same cursor, and the
// cursor is allocated once.
func TestSessionRestoreHTTPConcurrentIdempotent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)

	const n = 20
	var wg sync.WaitGroup
	var mu sync.Mutex
	created := 0
	cursors := make(map[float64]int)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
				"changeId": "dup", "snapshotCursor": 1,
			})
			if w.Code != http.StatusOK {
				t.Errorf("status = %d body = %s", w.Code, w.Body.String())
				return
			}
			mu.Lock()
			if body["created"] == true {
				created++
			}
			cursors[body["cursor"].(float64)]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if created != 1 {
		t.Fatalf("created count = %d, want 1", created)
	}
	if len(cursors) != 1 {
		t.Fatalf("cursors returned = %v, want one shared cursor", cursors)
	}
	var cursor float64
	for c := range cursors {
		cursor = c
	}
	if cursor != 2 {
		t.Fatalf("cursor = %v, want 2", cursor)
	}

	// Exactly one row beyond the seed was appended.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if got := len(body["changes"].([]any)); got != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("rows = %v nextCursor = %v, want exactly 2", body["changes"], body["nextCursor"])
	}
}

func TestSessionRestoreHTTPPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-restore.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"a":1}`)
	w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	h2 := NewHandler(s2)

	// Provenance survives: the identical session restore is idempotent with the
	// first cursor.
	w, body := postSessionRestore(t, h2, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 2 {
		t.Fatalf("idempotent after restart = %d %v", w.Code, body)
	}

	// The conflict decision survives too: another device is a 409.
	w, _ = restoreJSON(t, h2, "doc1", map[string]any{
		"deviceId": "other", "changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// The restored change reads back with the snapshot state.
	w, list := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes?after=1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["payload"].(map[string]any)["a"].(float64) != 1 {
		t.Fatalf("restored change after restart = %v", rows)
	}

	// A new restore appends, continuing the durable cursor.
	w, body = postSessionRestore(t, h2, "sess", "doc1", map[string]any{
		"changeId": "r2", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["created"] != true || body["cursor"].(float64) != 3 {
		t.Fatalf("post-restart restore = %d %v", w.Code, body)
	}
}
