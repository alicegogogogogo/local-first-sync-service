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

// sessionMergePath is the session-scoped single-change merge entry, one
// segment below the change collection.
func sessionMergePath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/changes/merge"
}

// postSessionMerge posts one merge request to the session-scoped merge entry.
func postSessionMerge(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionMergePath(session, doc), body)
}

func TestSessionMergeHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// The first change on an unknown document, baseCursor 0, is "applied" at
	// cursor 1 with the document-level success shape.
	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"c1","payload":{"a":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"id":"c1","outcome":"applied","cursor":1}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The calling device is the session's owning device: the stored row names
	// it, exactly as a document-level merge by that device would.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || body["nextCursor"].(float64) != 1 {
		t.Fatalf("list = %v next=%v", rows, body["nextCursor"])
	}
	row := rows[0].(map[string]any)
	if row["deviceId"] != "dev" || row["id"] != "c1" {
		t.Fatalf("row = %v", row)
	}
}

func TestSessionMergeHTTPOutcomes(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// First change on a new doc, baseCursor 0 -> applied cursor 1.
	w, body := postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "c1", "payload": map[string]any{"a": 1}},
	})
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("applied = %v", body)
	}

	// baseCursor ahead -> 400 zero write.
	w, _ = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 9,
		"change":     map[string]any{"id": "cX", "payload": map[string]any{"z": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ahead base = %d body=%s", w.Code, w.Body.String())
	}

	// Same id repost -> idempotent with the embedded original result and the
	// first cursor.
	w, body = postSessionMerge(t, h, "sess", "doc", `{"baseCursor":1,"change":{"id":"c1","payload":{"a":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent = %d %s", w.Code, w.Body.String())
	}
	want := `{"id":"c1","outcome":"idempotent","cursor":1,` +
		`"result":{"id":"c1","created":false,"cursor":1}}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("idempotent body = %q\nwant %q", w.Body.String(), want)
	}

	// Same id, different payload -> 409.
	w, _ = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "c1", "payload": map[string]any{"a": 2}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("payload conflict = %d body=%s", w.Code, w.Body.String())
	}

	// Seed a second object {"b":2} at cursor 2 (c1 already holds {"a":1}).
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "c2", "payload": map[string]any{"b": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Stale client at baseCursor 1 posts {"c":3}: disjoint from the later
	// {"b":2} -> merged cursor 3.
	w, body = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "c3", "payload": map[string]any{"c": 3}},
	})
	if w.Code != 200 || body["outcome"] != "merged" || body["cursor"].(float64) != 3 {
		t.Fatalf("merged = %d %v body=%s", w.Code, body, w.Body.String())
	}

	// Stale client posts a payload colliding with a later top-level key
	// ("b") -> 409 zero write.
	before, _, _ := s.ListChanges("doc", 0, 100)
	w, _ = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "c4", "payload": map[string]any{"b": 99}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("key clash = %d body=%s", w.Code, w.Body.String())
	}
	after, _, _ := s.ListChanges("doc", 0, 100)
	if len(after) != len(before) {
		t.Fatalf("conflict changed row count: before=%d after=%d", len(before), len(after))
	}

	// A non-object payload after the base cursor blocks the merge with 409 as
	// well. First append such a payload (cursor 4) through the session commit.
	w, _ = postSessionChanges(t, h, "sess", "doc", `{"changes":[{"id":"c5","payload":[1,2,3]}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postSessionMerge(t, h, "sess", "doc", map[string]any{
		"baseCursor": 3,
		"change":     map[string]any{"id": "c6", "payload": map[string]any{"z": 1}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("non-object later payload = %d body=%s", w.Code, w.Body.String())
	}
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 4 || next != 4 {
		t.Fatalf("conflict leaked writes: rows = %d next = %d", len(rows), next)
	}
}

func TestSessionMergeHTTPSourceMismatch(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"x","payload":{"k":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Same id and payload but a different source device is a conflict: only
	// the session device AND the decoded payload matching is idempotent.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	w, body := postSessionMerge(t, h, "sess-2", "doc1", `{"baseCursor":1,"change":{"id":"x","payload":{"k":1}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("device conflict = %d %s", w.Code, w.Body.String())
	}
	if body["error"] == nil {
		t.Fatalf("conflict body = %s", w.Body.String())
	}

	// The rejected merge left the log untouched: one row, cursor unmoved.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "x" || next != 1 {
		t.Fatalf("conflict leaked writes: rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPSharesCursorSpace(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2)

	// The session merge continues the document-level cursor space.
	w, body := postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "m1", "payload": map[string]any{"m": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 3 {
		t.Fatalf("result = %v", body)
	}

	// A following ordinary document-level commit takes the next cursor: one
	// contiguous space across every write path.
	w, body = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d1", "payload": map[string]any{"d": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["cursor"].(float64) != 4 {
		t.Fatalf("document-level cursor = %v", r)
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
		{"malformed json", "application/json", `{"baseCursor":0,"change":{`},
		{"trailing content", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}garbage`},
		{"second json value", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":{}}}{"baseCursor":0}`},
		{"body is not an object", "application/json", `[{"id":"c"}]`},
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
		{"missing change id", "application/json", `{"baseCursor":0,"change":{"payload":{}}}`},
		{"missing payload", "application/json", `{"baseCursor":0,"change":{"id":"c"}}`},
		{"array payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":[1]}}`},
		{"scalar payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":5}}`},
		{"null payload", "application/json", `{"baseCursor":0,"change":{"id":"c","payload":null}}`},
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

	// A non-zero baseCursor for an unknown document is a 400.
	w, _ := postSessionMerge(t, h, "sess", "ghost", map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "c", "payload": map[string]any{}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown doc non-zero base = %d body=%s", w.Code, w.Body.String())
	}

	// Zero writes: every rejected request left every document unknown.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
	rows, next, _ = s.ListChanges("ghost", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated on ghost: rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Never-created session -> 404 JSON with no result.
	w, body := postSessionMerge(t, h, "ghost", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["id"] != nil || body["outcome"] != nil {
		t.Fatalf("404 leaked a result: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed merge against an
	// unknown session is still a 400.
	w, _ = postSessionMerge(t, h, "ghost", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}trailing`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> merges through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	// Zero writes: nothing reached the change log.
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionMergeHTTPPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no result and zero writes.
	w, body := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["id"] != nil || body["outcome"] != nil {
		t.Fatalf("403 leaked a result: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionMerge(t, h, "ghost", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}

	// Re-grant restores the merge path; the first change on the unknown doc is
	// applied at cursor 1.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted merge = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("re-granted result = %v", body)
	}
}

func TestSessionMergeHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Verbs other than POST on the merge path are a JSON 400.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		w, _ := doRequest(t, h, method, sessionMergePath("sess", "doc1"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash, a missing changes segment and extra
	// segments are a JSON 400, never a redirect or an HTML page.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/changes/merge",
		"/v1/sessions/sess/documents//changes/merge",
		"/v1/sessions/sess/documents/doc1/changes/merge/",
		"/v1/sessions/sess/documents/doc1/merge",
		"/v1/sessions/sess/documents/doc1/changes/merge/extra",
		"/v1/sessions/sess/documents/doc1/changes/merge/extra/more",
	} {
		r := newJSONRequest(http.MethodPost, p, `{"baseCursor":0,"change":{"id":"a","payload":{}}}`, "application/json")
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

	// Zero writes: the document is still unknown.
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

// A session or document literally named "merge" is an ordinary identifier:
// its merge entry behaves exactly like any other id's, and its ordinary
// collection routes keep working.
func TestSessionMergeHTTPNamedMergeKeepsOrdinaryRoutes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "merge")

	// Session named "merge": its merge entry works.
	w, body := postSessionMerge(t, h, "merge", "doc1", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("session named merge = %d %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 1 {
		t.Fatalf("session named merge result = %v", body)
	}

	// Document named "merge": both the merge entry and the ordinary collection
	// commit work, sharing one cursor space.
	w, body = postSessionMerge(t, h, "merge", "merge", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("doc named merge merge = %d %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 1 {
		t.Fatalf("doc named merge cursor = %v", body)
	}
	w, _ = postJSON(t, h, "/v1/sessions/merge/documents/merge/changes", map[string]any{
		"changes": []any{map[string]any{"id": "b", "payload": map[string]any{"k": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("doc named merge collection post = %d %s", w.Code, w.Body.String())
	}
	w, list := doRequest(t, h, http.MethodGet, "/v1/sessions/merge/documents/merge/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if len(list["changes"].([]any)) != 2 || list["nextCursor"].(float64) != 2 {
		t.Fatalf("doc named merge list = %v", list["changes"])
	}

	// The keyword guard does not misfire on extra segments under ids named
	// merge: an extra segment is still a malformed 400.
	r := newJSONRequest(http.MethodPost, "/v1/sessions/merge/documents/merge/changes/merge/extra",
		`{"baseCursor":0,"change":{"id":"z","payload":{}}}`, "application/json")
	if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("extra segment under merge-named ids = %d, want 400", w.Code)
	}
}

// An extra deviceId field in the body is ignored: the calling device is
// always the session's owning device, never a body field.
func TestSessionMergeHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionMerge(t, h, "sess", "doc1", map[string]any{
		"deviceId":   "intruder",
		"baseCursor": 0,
		"change":     map[string]any{"id": "a", "payload": map[string]any{"n": 1}},
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
// merge's for the same request shape.
func TestSessionMergeHTTPSuccessShapeMatchesDocumentMerge(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := postJSON(t, h, "/v1/documents/docA/merge", `{"deviceId":"dev","baseCursor":0,"change":{"id":"x","payload":{"k":1}}}`)
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
		w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/poll?after=1&waitMs=30000")
		if w.Code != http.StatusOK {
			t.Errorf("poll status = %d body = %s", w.Code, w.Body.String())
		}
		done <- body
	}()

	// A merge appending a new change wakes the parked poll.
	time.Sleep(50 * time.Millisecond)
	w, _ := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":1,"change":{"id":"live","payload":{"w":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	select {
	case body := <-done:
		rows := body["changes"].([]any)
		if len(rows) != 1 || rows[0].(map[string]any)["id"] != "live" {
			t.Fatalf("polled rows = %v", rows)
		}
		row := rows[0].(map[string]any)
		if row["deviceId"] != "dev" || row["cursor"].(float64) != 2 {
			t.Fatalf("polled row = %v", row)
		}
		if body["timedOut"] != false {
			t.Fatalf("poll timed out despite the merge: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session merge did not wake the parked poll")
	}

	// An idempotent merge allocates no cursor and does not wake a parked poll.
	timed := make(chan map[string]any, 1)
	go func() {
		_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/poll?after=2&waitMs=80")
		timed <- body
	}()
	time.Sleep(50 * time.Millisecond)
	w, _ = postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":2,"change":{"id":"live","payload":{"w":1.0}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	select {
	case body := <-timed:
		if body["timedOut"] != true || body["nextCursor"].(float64) != 2 {
			t.Fatalf("idempotent merge woke the poll: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not return")
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

	// A new change merged through the session entry is pushed like any other
	// write path.
	if code := postHTTP(t, srv, sessionMergePath("sess", "doc"), map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "merge-1", "payload": map[string]any{"m": 2}},
	}); code != http.StatusOK {
		t.Fatalf("session merge status = %d", code)
	}
	c := conn.readChange()
	if c.Cursor != 2 || c.ID != "merge-1" || c.DeviceID != "dev-1" {
		t.Fatalf("pushed frame = %+v", c)
	}

	// An idempotent merge pushes nothing: the subscription stays parked.
	if code := postHTTP(t, srv, sessionMergePath("sess", "doc"), map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "merge-1", "payload": map[string]any{"m": 2.0}},
	}); code != http.StatusOK {
		t.Fatalf("idempotent merge status = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("idempotent merge pushed a frame")
	}
	conn.clearReadDeadline()
}

func TestSessionMergeHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Every goroutine merges at baseCursor 0 with a unique top-level key, so no
	// pair of object payloads ever collides: one request is applied and every
	// other one is a valid merge. The batch must take effect as a whole across
	// writers — contiguous cursors, never duplicated, no lost row.
	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w, body := postSessionMerge(t, h, "sess", "doc", map[string]any{
				"baseCursor": 0,
				"change":     map[string]any{"id": fmt.Sprintf("id-%02d", i), "payload": map[string]any{fmt.Sprintf("k%02d", i): i}},
			})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("merge %d status %d: %s", i, w.Code, w.Body.String())
				return
			}
			switch body["outcome"] {
			case "applied", "merged":
			default:
				errs <- fmt.Errorf("merge %d unexpected outcome %v", i, body["outcome"])
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
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
	w, _ = postSessionChanges(t, h, "sess", "doc1", `{"changes":[{"id":"c2","payload":{"b":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	// Stale merge at base 1 -> merged cursor 3.
	w, body := postSessionMerge(t, h, "sess", "doc1", `{"baseCursor":1,"change":{"id":"c3","payload":{"c":3}}}`)
	if w.Code != http.StatusOK || body["outcome"] != "merged" {
		t.Fatalf("merged before restart = %d %s", w.Code, w.Body.String())
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

	// Idempotency survives the restart: same session device and decoded
	// payload -> idempotent with the first cursor and the embedded result.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":3,"change":{"id":"c1","payload":{"a":1.0}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("idempotent after restart = %v", body)
	}
	res := body["result"].(map[string]any)
	if res["created"] != false || res["cursor"].(float64) != 1 {
		t.Fatalf("embedded result after restart = %v", res)
	}

	// So does the conflict decision.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":3,"change":{"id":"c1","payload":{"a":9}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// Raw payloads read back verbatim under the session device.
	w, body = doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 3 || body["nextCursor"].(float64) != 3 {
		t.Fatalf("list after restart = %v next=%v", rows, body["nextCursor"])
	}
	for _, row := range rows {
		if row.(map[string]any)["deviceId"] != "dev" {
			t.Fatalf("device after restart = %v", row)
		}
	}

	// The cursor space continues after the restart.
	w, body = postSessionMerge(t, h2, "sess", "doc1", `{"baseCursor":3,"change":{"id":"c4","payload":{"d":4}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 4 {
		t.Fatalf("post-restart cursor = %v", body)
	}
}

// A compacted-away id still answers idempotency and conflict decisions from
// its retained summary when the merge arrives through the session path.
func TestSessionMergeHTTPCompactedIdentity(t *testing.T) {
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

	// The trimmed id is idempotent with its first cursor. The base must not be
	// below the new boundary (2).
	w, body := postSessionMerge(t, h, "sess", "doc", `{"baseCursor":2,"change":{"id":"c1","payload":{"n":1}}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed idempotent = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || body["cursor"].(float64) != 1 {
		t.Fatalf("trimmed result = %v", body)
	}

	// A differing payload against the trimmed id is still a 409.
	w, _ = postSessionMerge(t, h, "sess", "doc", `{"baseCursor":2,"change":{"id":"c1","payload":{"n":9}}}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("trimmed conflict = %d %s", w.Code, w.Body.String())
	}

	// A base below the compaction boundary cannot run the conflict check and
	// is a 400.
	w, _ = postSessionMerge(t, h, "sess", "doc", `{"baseCursor":0,"change":{"id":"cX","payload":{"x":1}}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("base below boundary = %d, want 400", w.Code)
	}

	// New ids continue past the pre-compaction cursor space.
	w, body = postSessionMerge(t, h, "sess", "doc", `{"baseCursor":2,"change":{"id":"c3","payload":{"n":3}}}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 3 {
		t.Fatalf("post-compaction cursor = %v", body)
	}
}

// The document-level merge is untouched by the new entry: it still requires
// its body deviceId and answers with its own shapes; a stray deviceId on the
// session entry never overrides the session device.
func TestSessionMergeHTTPDocumentMergeUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/merge", `{"baseCursor":0,"change":{"id":"a","payload":{}}}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("document merge without deviceId = %d, want 400", w.Code)
	}

	w, body := postJSON(t, h, "/v1/documents/doc1/merge", map[string]any{
		"deviceId":   "dev",
		"baseCursor": 0,
		"change":     map[string]any{"id": "a", "payload": map[string]any{"n": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if body["outcome"] != "applied" || body["cursor"].(float64) != 1 {
		t.Fatalf("document merge result = %v", body)
	}

	// The session path reads back exactly what the document path wrote.
	w, body = doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc1"))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["deviceId"] != "dev" {
		t.Fatalf("session read = %v", rows)
	}
}

// A revoke committed while a subscription is live ends it with 4403,
// including when the last pushed frame arrived through the session merge;
// merges attempted after the revoke are a 403 and push nothing.
func TestSessionMergeHTTPSubscriptionRevokedCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	if code := postHTTP(t, srv, sessionMergePath("sess", "doc"), map[string]any{
		"baseCursor": 1,
		"change":     map[string]any{"id": "merge-live", "payload": map[string]any{"m": 2}},
	}); code != http.StatusOK {
		t.Fatalf("merge status = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 2 || c.ID != "merge-live" {
		t.Fatalf("pushed frame = %+v", c)
	}

	if code := postHTTP(t, srv, "/v1/documents/doc/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); code != http.StatusOK {
		t.Fatalf("revoke status = %d", code)
	}
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 4403 {
		t.Fatalf("close code = %d, want 4403", code)
	}
	conn.clearReadDeadline()

	if code := postHTTP(t, srv, sessionMergePath("sess", "doc"), map[string]any{
		"baseCursor": 2,
		"change":     map[string]any{"id": "after-revoke", "payload": map[string]any{"m": 3}},
	}); code != http.StatusForbidden {
		t.Fatalf("merge after revoke = %d, want 403", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived after the 4403 close")
	}
	conn.clearReadDeadline()
}

// The termination signal ends the subscription with 1001 even when its catch-up
// frame was produced by the session merge; committed data stays readable.
func TestSessionMergeHTTPSubscriptionShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	if code := postHTTP(t, srv, sessionMergePath("sess", "doc"), map[string]any{
		"baseCursor": 0,
		"change":     map[string]any{"id": "merge-1", "payload": map[string]any{"m": 1}},
	}); code != http.StatusOK {
		t.Fatalf("merge status = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 1 || c.ID != "merge-1" {
		t.Fatalf("pushed frame = %+v", c)
	}

	st.InterruptWaits()
	conn.setReadDeadline(2 * time.Second)
	if code := conn.readCloseCode(); code != 1001 {
		t.Fatalf("close code = %d, want 1001", code)
	}
	conn.clearReadDeadline()

	rows, next, err := st.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("committed state after signal = %+v/%d", rows, next)
	}
}
