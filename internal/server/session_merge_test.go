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

// sessionMergePath is the session-scoped single-change merge path.
func sessionMergePath(session, doc string) string {
	return sessionChangesPath(session, doc) + "/merge"
}

// postSessionMerge posts one merge request to the session-scoped merge entry.
func postSessionMerge(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionMergePath(session, doc), body)
}

func TestSessionMergeHTTPSuccessApplied(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// First merge on an unknown document with baseCursor 0: applied at
	// cursor 1, with the document-level success shape byte-for-byte.
	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"c1","payload":{"a":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"id":"c1","outcome":"applied","cursor":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The calling device is the session's owning device.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || body["nextCursor"].(float64) != 1 {
		t.Fatalf("list = %v next=%v", rows, body["nextCursor"])
	}
	r := rows[0].(map[string]any)
	if r["deviceId"] != "dev" || r["id"] != "c1" {
		t.Fatalf("row = %v", r)
	}
}

func TestSessionMergeHTTPMergedAndSharedCursor(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2) // c1 {"n":1} @1, c2 {"n":2} @2

	// Stale client at baseCursor 1 posts a payload disjoint from the later
	// {"n":2}: merged, taking the next cursor of the shared space.
	w, body := postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"x": 9}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "merged" || body["cursor"].(float64) != 3 {
		t.Fatalf("merged = %v", body)
	}

	// baseCursor equal to the current cursor appends directly.
	w, body = postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"baseCursor": 3,
		"change":     map[string]any{"id": "m2", "payload": map[string]any{"y": 9}},
	})
	if w.Code != http.StatusOK || body["outcome"] != "applied" || body["cursor"].(float64) != 4 {
		t.Fatalf("applied = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// A following document-level merge keeps the same contiguous cursor space.
	w, body = mergeBody(t, h, "doc1", map[string]any{
		"deviceId":   "dev",
		"baseCursor": 4,
		"change":     map[string]any{"id": "d1", "payload": map[string]any{"z": 1}},
	})
	if w.Code != http.StatusOK || body["cursor"].(float64) != 5 {
		t.Fatalf("document merge cursor = %d %v", w.Code, body)
	}
}

func TestSessionMergeHTTPIdempotent(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"x","payload":{"a":1,"b":2}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical repost (decoded-equal payload, different key/number spelling):
	// idempotent with the first cursor and the embedded original result.
	w, body := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":1,"change":{"id":"x","payload":{"b":2.0,"a":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("repost status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("idempotent = %v", body)
	}
	res := body["result"].(map[string]any)
	if res["id"] != "x" || res["created"] != false || res["cursor"].(float64) != 1 {
		t.Fatalf("embedded result = %v", res)
	}
	want := `{"id":"x","outcome":"idempotent","cursor":1,` +
		`"result":{"id":"x","created":false,"cursor":1}}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// No new row was written.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPConflict(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"x","payload":{"k":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Same id, different payload: 409 JSON naming the conflicting id.
	w, body := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":1,"change":{"id":"x","payload":{"k":2}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("payload conflict = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	if body["error"] == nil || !strings.Contains(body["error"].(string), "x") {
		t.Fatalf("conflict body = %s", w.Body.String())
	}

	// A second session on another device: same id and payload but a different
	// source device is a conflict too.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	w, _ = postSessionMerge(t, h, "sess-2", "doc1", `{"baseCursor":1,"change":{"id":"x","payload":{"k":1}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict = %d body = %s", w.Code, w.Body.String())
	}

	// Top-level key collision with a later payload: 409, zero writes.
	w, _ = postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"clash","payload":{"k":99}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("key clash = %d body = %s", w.Code, w.Body.String())
	}

	// A non-object payload after the base cursor blocks the merge: 409.
	w, _ = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "arr", "payload": []any{1, 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"nonobj","payload":{"q":1}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("non-object later = %d body = %s", w.Code, w.Body.String())
	}

	// Every conflict left the log at its single original row.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("conflict leaked writes: rows = %d next = %d", len(rows), next)
	}
}

func TestSessionMergeHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	url := sessionMergePath("sess", "doc1")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"missing content type", "", `{"baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"content type with json suffix", "application/vnd.api+json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}`},
		{"malformed json", "application/json", `{"baseCursor":0,`},
		{"trailing content", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}garbage`},
		{"second json value", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}{}`},
		{"missing baseCursor", "application/json", `{"change":{"id":"c","payload":{}}}`},
		{"null baseCursor", "application/json", `{"baseCursor":null,"change":{"id":"c","payload":{}}}`},
		{"negative baseCursor", "application/json", `{"baseCursor":-1,"change":{"id":"c","payload":{}}}`},
		{"float baseCursor", "application/json", `{"baseCursor":1.5,"change":{"id":"c","payload":{}}}`},
		{"string baseCursor", "application/json", `{"baseCursor":"0","change":{"id":"c","payload":{}}}`},
		{"boolean baseCursor", "application/json", `{"baseCursor":true,"change":{"id":"c","payload":{}}}`},
		{"missing change", "application/json", `{"baseCursor":0}`},
		{"null change", "application/json", `{"baseCursor":0,"change":null}`},
		{"empty change id", "application/json", `{"baseCursor":0,"change":{"id":"","payload":{}}}`},
		{"numeric change id", "application/json", `{"baseCursor":0,"change":{"id":7,"payload":{}}}`},
		{"missing payload", "application/json", `{"baseCursor":0,"change":{"id":"c"}}`},
		{"array payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":[1]}}`},
		{"scalar payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":5}}`},
		{"null payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":null}}`},
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

	// Unknown document with a non-zero base: 400 JSON, zero writes.
	w, _ := postSessionMerge(t, h, "sess", "ghost", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "c", "payload": map[string]any{}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown doc non-zero base = %d body = %s", w.Code, w.Body.String())
	}

	// baseCursor greater than the current cursor: 400 JSON, zero writes.
	w, _ = postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"baseCursor": 9,
		"change":     map[string]any{"id": "c", "payload": map[string]any{"z": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ahead base = %d body = %s", w.Code, w.Body.String())
	}

	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPCompressedBase(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postDocChanges(t, h, "doc", "dev", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// A base below the compaction boundary cannot complete its conflict check.
	w, body := postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "late", "payload": map[string]any{"k": 1}},
	})
	if w.Code != http.StatusBadRequest || body["error"] == nil {
		t.Fatalf("compacted base = %d %v", w.Code, body)
	}

	// A trimmed id still answers idempotent from its retained summary.
	w, body = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "c1", "payload": map[string]any{"n": 1}},
	})
	if w.Code != http.StatusOK || body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("trimmed idempotent = %d %v", w.Code, body)
	}
}

func TestSessionMergeHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	goodBody := `{"baseCursor":0,"change":{"id":"a","payload":{"k":1}}}`

	// Never-created session -> 404 JSON with no result.
	w, body := postSessionMerge(t, h, "ghost", "doc1", goodBody)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["outcome"] != nil {
		t.Fatalf("404 leaked a result: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed merge against an
	// unknown session is still a 400.
	w, _ = postSessionMerge(t, h, "ghost", "doc1", `{"baseCursor":true,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> merges through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionMerge(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	goodBody := map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "a", "payload": map[string]any{"k": 1}},
	}

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no result and zero writes.
	w, body := postSessionMerge(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["outcome"] != nil {
		t.Fatalf("403 leaked a result: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionMerge(t, h, "ghost", "doc1", goodBody)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}

	// Re-grant restores the merge path.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionMerge(t, h, "sess", "doc1", goodBody)
	if w.Code != http.StatusOK || body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("re-granted merge = %d %v", w.Code, body)
	}
}

func TestSessionMergeHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	body := `{"baseCursor":0,"change":{"id":"a","payload":{"k":1}}}`

	// Verbs other than POST on the merge path are a JSON 400.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		r := newJSONRequest(method, sessionMergePath("sess", "doc1"), body, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash and extra segments are a JSON 400,
	// never a redirect or an HTML page.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/changes/merge",
		"/v1/sessions/sess/documents//changes/merge",
		"/v1/sessions/sess/documents/doc1/changes/merge/",
		"/v1/sessions/sess/documents/doc1/changes/merge/extra",
		"/v1/sessions/sess/documents/doc1/changes/merge/extra/more",
		"/v1/sessions/sess/documents/doc1/merge",
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

// A session or document literally named "merge" is an ordinary identifier: the
// request merges normally, with no redirect and no HTML.
func TestSessionMergeHTTPMergeNamedIdentifiers(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "merge")

	w, body := postSessionMerge(t, h, "merge", "merge", `{"baseCursor":0,"change":{"id":"c","payload":{"k":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("merge-named result = %v", body)
	}
}

// An extra deviceId field in the body is ignored: the calling device is always
// the session's owning device, never a body field.
func TestSessionMergeHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"deviceId":   "intruder",
		"baseCursor": 0,
		"change":     map[string]any{"id": "a", "payload": map[string]any{"k": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	_, list := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	row := list["changes"].([]any)[0].(map[string]any)
	if row["deviceId"] != "dev" {
		t.Fatalf("stored device = %v, want the session device dev", row["deviceId"])
	}
}

// The session merge's success body is byte-identical to the document-level
// merge's for the same request content.
func TestSessionMergeHTTPSuccessShapeMatchesDocumentMerge(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := mergeBody(t, h, "docA", `{"deviceId":"dev","baseCursor":0,"change":{"id":"x","payload":{"k":1}}}`)
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionMerge(t, h, "sess", "docB", `{"baseCursor":0,"change":{"id":"x","payload":{"k":1}}}`)
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}

func TestSessionMergeHTTPWakesLongPoll(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 1)

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
	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":1,"change":{"id":"live","payload":{"n":2}}}`)
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
			t.Fatalf("poll timed out despite the merge: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session merge did not wake the parked poll")
	}
}

func TestSessionMergeHTTPPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// A merge committed through the session-scoped endpoint is pushed like any
	// other write path.
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/changes/merge", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "sess-m", "payload": map[string]any{"m": 2}},
	}); code != http.StatusOK {
		t.Fatalf("session merge status = %d", code)
	}
	c := conn.readChange()
	if c.Cursor != 2 || c.ID != "sess-m" || c.DeviceID != "dev-1" {
		t.Fatalf("pushed frame = %+v", c)
	}

	// An idempotent repeat allocates no cursor and pushes nothing.
	conn.setReadDeadline(300 * time.Millisecond)
	if code := postHTTP(t, srv, "/v1/sessions/sess/documents/doc/changes/merge", map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "sess-m", "payload": map[string]any{"m": 2.0}},
	}); code != http.StatusOK {
		t.Fatalf("idempotent merge status = %d", code)
	}
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("idempotent merge produced a push")
	}
	conn.clearReadDeadline()
}

// A permission revoke while a subscription is open still ends it with 4403,
// including for clients committing through the session merge entry.
func TestSessionMergeHTTPSubscriptionCloses4403OnRevoke(t *testing.T) {
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

func TestSessionMergeHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	var applied int64
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every goroutine merges from baseCursor 0 with a unique top-level
			// key, so serialization leaves each merge applicable.
			w, body := postSessionMerge(t, h, "sess", "doc", map[string]any{
				"baseCursor": 0,
				"change": map[string]any{
					"id":      fmt.Sprintf("id-%02d", i),
					"payload": map[string]any{fmt.Sprintf("k%02d", i): i},
				},
			})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("merge %d status %d: %s", i, w.Code, w.Body.String())
				return
			}
			mu.Lock()
			if body["outcome"] == "applied" {
				applied++
			}
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Exactly one request won the applied race; the whole set took effect with
	// no cursor reused and no record lost.
	if applied != 1 {
		t.Fatalf("applied count = %d, want 1", applied)
	}
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != n || body["nextCursor"].(float64) != n {
		t.Fatalf("stored %d changes, nextCursor = %v, want %d", len(rows), body["nextCursor"], n)
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

func TestSessionMergeHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-merge.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"c1","payload":{"a":1}}}`)
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

	// Idempotency survives the restart.
	w, body := postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c1","payload":{"a":1.0}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("idempotent after restart = %v", body)
	}

	// The conflict decision survives too.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c1","payload":{"a":2}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// A merge at the current cursor appends, continuing the durable cursor.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c2","payload":{"b":2}}}`)
	if w.Code != http.StatusOK || body["outcome"] != "applied" || body["cursor"].(float64) != 2 {
		t.Fatalf("post-restart merge = %d %v", w.Code, body)
	}

	// A stale base (1) behind the current cursor merges when its keys are
	// disjoint from every later payload, continuing the durable cursor.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c3","payload":{"c":3}}}`)
	if w.Code != http.StatusOK || body["outcome"] != "merged" || body["cursor"].(float64) != 3 {
		t.Fatalf("post-restart stale merge = %d %v", w.Code, body)
	}
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c4","payload":{"d":4}}}`)
	if w.Code != http.StatusOK || body["outcome"] != "merged" || body["cursor"].(float64) != 4 {
		t.Fatalf("post-restart stale merge = %d %v", w.Code, body)
	}
}
