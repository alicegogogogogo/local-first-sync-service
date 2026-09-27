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
	if body["id"] != "restore-1" || body["created"] != true ||
		body["cursor"].(float64) != 3 || body["restoredFrom"].(float64) != 2 {
		t.Fatalf("restore body = %v", body)
	}
	want := `{"id":"restore-1","created":true,"cursor":3,"restoredFrom":2}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The appended change is ordinary and carries the snapshot state; its
	// source device is the session's owning device.
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

	// Old rows and the snapshot itself are untouched.
	w, snap := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/snapshots/2")
	if w.Code != http.StatusOK || snap["state"].(map[string]any)["text"] != "hello" {
		t.Fatalf("snapshot after restore = %d %v", w.Code, snap)
	}
}

// The session restore's success body for the same content is byte-identical to
// the document-level restore's.
func TestSessionRestoreHTTPShapeMatchesDocumentRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "docA", 1, `{"k":1}`)
	seedSnapshot(t, h, "docB", 1, `{"k":1}`)

	wDoc, _ := restoreJSON(t, h, "docA", map[string]any{
		"deviceId":       "dev",
		"changeId":       "r",
		"snapshotCursor": 1,
	})
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionRestore(t, h, "sess", "docB", map[string]any{
		"changeId":       "r",
		"snapshotCursor": 1,
	})
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
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
		{"cursor ahead", "doc1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor := 1
			if tc.name == "cursor ahead" {
				cursor = 99
			}
			w, body := postSessionRestore(t, h, "sess", tc.doc, map[string]any{
				"changeId":       "c",
				"snapshotCursor": cursor,
			})
			if w.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want 404, body = %s", w.Code, w.Body.String())
			}
			if body["error"] == nil {
				t.Fatalf("body = %s", w.Body.String())
			}
			if body["id"] != nil || body["cursor"] != nil {
				t.Fatalf("404 leaked result content: %s", w.Body.String())
			}
		})
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestSessionRestoreHTTPIdempotent(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":1}`)
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

	// Identical repeat: 200, created=false, the first cursor and the source
	// snapshot cursor.
	w, body = postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 3 ||
		body["restoredFrom"].(float64) != 2 {
		t.Fatalf("idempotent restore = %d %v", w.Code, body)
	}

	// The idempotent repeat appended nothing.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || next != 3 {
		t.Fatalf("rows = %d next = %d, want 3/3", len(rows), next)
	}
}

func TestSessionRestoreHTTPConflicts(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":1}`)
	w, _ := postJSON(t, h, "/v1/documents/doc1/snapshots", `{"cursor":1,"state":{"v":9}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// First restore occupies r1 @3.
	if w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 2,
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	before, beforeNext, _ := s.ListChanges("doc1", 0, 100)

	// A second session on another device: the same id with the same snapshot
	// cursor and source state is still a conflict because the source device
	// differs.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")

	cases := []struct {
		name    string
		session string
		body    map[string]any
	}{
		{"snapshot cursor mismatch", "sess", map[string]any{"changeId": "r1", "snapshotCursor": 1}},
		{"source device mismatch", "sess-2", map[string]any{"changeId": "r1", "snapshotCursor": 2}},
		{"id taken by an ordinary change", "sess", map[string]any{"changeId": "c1", "snapshotCursor": 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, body := postSessionRestore(t, h, tc.session, "doc1", tc.body)
			if w.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409, body = %s", w.Code, w.Body.String())
			}
			if body["error"] == nil || !strings.Contains(body["error"].(string), tc.body["changeId"].(string)) {
				t.Fatalf("conflict body = %s", w.Body.String())
			}
			if body["id"] != nil || body["cursor"] != nil {
				t.Fatalf("409 leaked result content: %s", w.Body.String())
			}
		})
	}

	// Different source state for the same id is a conflict too: first restore a
	// fresh id from snapshot 1, then ask for the same id from snapshot 2 whose
	// state differs.
	wSetup, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r2", "snapshotCursor": 1,
	})
	if wSetup.Code != http.StatusOK {
		t.Fatalf("setup restore r2 = %d %s", wSetup.Code, wSetup.Body.String())
	}
	wState, stateBody := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r2", "snapshotCursor": 2,
	})
	if wState.Code != http.StatusConflict {
		t.Fatalf("source-state mismatch = %d body = %s", wState.Code, wState.Body.String())
	}
	if stateBody["id"] != nil {
		t.Fatalf("409 leaked result content: %s", wState.Body.String())
	}

	after, afterNext, _ := s.ListChanges("doc1", 0, 100)
	// Exactly one extra row from the setup restore r2.
	if len(after) != len(before)+1 || afterNext != beforeNext+1 {
		t.Fatalf("writes leaked: before=%d/%d after=%d/%d", len(before), beforeNext, len(after), afterNext)
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
		{"content type with json suffix", "application/vnd.api+json", `{"changeId":"c","snapshotCursor":2}`},
		{"malformed json", "application/json", `{`},
		{"trailing content", "application/json", `{"changeId":"c","snapshotCursor":2}garbage`},
		{"second json value", "application/json", `{"changeId":"c","snapshotCursor":2}{}`},
		{"missing changeId", "application/json", `{"snapshotCursor":2}`},
		{"empty changeId", "application/json", `{"changeId":"","snapshotCursor":2}`},
		{"numeric changeId", "application/json", `{"changeId":7,"snapshotCursor":2}`},
		{"null changeId", "application/json", `{"changeId":null,"snapshotCursor":2}`},
		{"missing snapshotCursor", "application/json", `{"changeId":"c"}`},
		{"null snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":null}`},
		{"zero snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":0}`},
		{"negative snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":-1}`},
		{"fractional snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":2.5}`},
		{"string snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":"2"}`},
		{"boolean snapshotCursor", "application/json", `{"changeId":"c","snapshotCursor":true}`},
		{"body not an object", "application/json", `[1,2,3]`},
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

	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("zero-write violated: rows=%d next=%d", len(rows), next)
	}
}

func TestSessionRestoreHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)
	goodBody := map[string]any{"changeId": "a", "snapshotCursor": 1}

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
	w, _ = postSessionRestore(t, h, "ghost", "doc1", map[string]any{
		"changeId": "a", "snapshotCursor": true,
	})
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
	goodBody := map[string]any{"changeId": "a", "snapshotCursor": 1}

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{
		"deviceId": "dev", "action": "revoke",
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no result and zero writes.
	w, body := postSessionRestore(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["id"] != nil || body["cursor"] != nil {
		t.Fatalf("403 leaked result content: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
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

// Deregistering the session's owning device hard-deletes the session, so a
// restore through that identity is a 404 with no result and writes nothing.
func TestSessionRestoreHTTPDeviceDeregistered(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)

	w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "a", "snapshotCursor": 1,
	})
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deregistered = %d %v", w.Code, body)
	}
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionRestoreHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)
	body := `{"changeId":"a","snapshotCursor":1}`

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		r := newJSONRequest(method, sessionRestorePath("sess", "doc1"), body, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	for _, p := range []string{
		"/v1/sessions//documents/doc1/restore",
		"/v1/sessions/sess/documents//restore",
		"/v1/sessions/sess/documents/doc1/restore/",
		"/v1/sessions/sess/documents/doc1/restore/extra",
		"/v1/sessions/sess/documents/doc1/restore/extra/more",
		"/v1/sessions/sess/restore",
		"/v1/sessions/sess/documents/doc1/changes/restore",
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
	if len(rows) != 1 || next != 1 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

// A session or document literally named "restore" is an ordinary identifier:
// the restore proceeds normally, with no redirect and no HTML.
func TestSessionRestoreHTTPRestoreNamedIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "restore")
	seedSnapshot(t, h, "restore", 1, `{"v":1}`)

	w, body := postSessionRestore(t, h, "restore", "restore", map[string]any{
		"changeId": "c", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["created"] != true || body["cursor"].(float64) != 2 {
		t.Fatalf("restore-named result = %v", body)
	}
}

// An extra deviceId field in the body is ignored: the calling device is always
// the session's owning device, never a body field.
func TestSessionRestoreHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)

	w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"deviceId":       "intruder",
		"changeId":       "a",
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

// The restore shares the document's contiguous cursor space with the ordinary
// session commit, replay and merge paths.
func TestSessionRestoreHTTPSharedCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 2, `{"v":2}`)

	// Restore takes cursor 3.
	w, body := postSessionRestore(t, h, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["cursor"].(float64) != 3 {
		t.Fatalf("restore cursor = %d %v", w.Code, body)
	}

	// A following session batch commit continues at 4.
	w, _ = postJSON(t, h, sessionChangesPath("sess", "doc1"), map[string]any{
		"changes": []any{map[string]any{"id": "b1", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A document-level replay continues at 5.
	w, _ = postJSON(t, h, "/v1/documents/doc1/replay", map[string]any{
		"deviceId": "dev",
		"operations": []any{
			map[string]any{"id": "o1", "payload": map[string]any{"n": 5}},
		},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// A session merge at the current cursor continues at 6.
	w, body = postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"baseCursor": 5,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"z": 6}},
	})
	if w.Code != http.StatusOK || body["cursor"].(float64) != 6 {
		t.Fatalf("merge cursor = %d %v", w.Code, body)
	}

	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if list["nextCursor"].(float64) != 6 {
		t.Fatalf("nextCursor = %v, want 6", list["nextCursor"])
	}
}

func TestSessionRestoreHTTPWakesLongPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"n":1}`)

	done := make(chan map[string]any, 1)
	go func() {
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
	// Snapshot at cursor 1 so the restore has a source.
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

	// A restore committed through the session-scoped endpoint is pushed like
	// any other write path.
	if code := postHTTP(t, srv, sessionRestorePath("sess", "doc"), map[string]any{
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
	if code := postHTTP(t, srv, sessionRestorePath("sess", "doc"), map[string]any{
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
// including for clients restoring through the session restore entry.
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
				"changeId":       fmt.Sprintf("r%02d", i),
				"snapshotCursor": 1,
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

	// The whole set took effect or was absent per request; cursors are unique
	// and contiguous: 1 seeded change + n restores, 1..n+1.
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

// Concurrent restores of the same id either all take effect once (the first
// creates, every identical repeat is idempotent with the first cursor) or are
// rejected as a whole; no cursor is allocated twice.
func TestSessionRestoreHTTPConcurrentSameId(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedSnapshot(t, h, "doc1", 1, `{"v":1}`)

	const n = 20
	var wg sync.WaitGroup
	statuses := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, _ := postSessionRestore(t, h, "sess", "doc1", map[string]any{
				"changeId": "same", "snapshotCursor": 1,
			})
			statuses <- w.Code
		}()
	}
	wg.Wait()
	close(statuses)
	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("identical concurrent restore status = %d, want 200", code)
		}
	}

	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	// One seeded change plus exactly one appended restore at cursor 2.
	if len(rows) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("rows = %d nextCursor = %v, want 2/2", len(rows), body["nextCursor"])
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

	// Provenance and the source-state decision survive the restart: an
	// identical replay is idempotent with the first cursor.
	w, body := postSessionRestore(t, h2, "sess", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK || body["created"] != false || body["cursor"].(float64) != 2 {
		t.Fatalf("replay after restart = %d %v", w.Code, body)
	}

	// A different source device is still a 409 after restart.
	createSessionViaHTTP(t, h2, "dev-2", "sess-2")
	w, _ = postSessionRestore(t, h2, "sess-2", "doc1", map[string]any{
		"changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// The appended change with the snapshot state reads back synchronously.
	w, list := doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes?after=1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := list["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["payload"].(map[string]any)["a"].(float64) != 1 {
		t.Fatalf("restored change after restart = %v", rows)
	}
}
