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

// sessionReplayPath is the session-scoped offline replay entry, one segment
// below the change collection.
func sessionReplayPath(session, doc string) string {
	return "/v1/sessions/" + session + "/documents/" + doc + "/changes/replay"
}

// postSessionReplay posts an offline operation batch to the session-scoped
// replay entry.
func postSessionReplay(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return postJSON(t, h, sessionReplayPath(session, doc), body)
}

func TestSessionReplayHTTPSuccess(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// First replay on an unknown document allocates from cursor 1, in request
	// order, with the document-level replay success shape byte-for-byte.
	w, _ := postSessionReplay(t, h, "sess", "doc1", `{"operations":[
		{"id":"o1","payload":{"n":1}},
		{"id":"o2","payload":[1,true,null]}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	want := `{"results":[` +
		`{"id":"o1","created":true,"cursor":1},` +
		`{"id":"o2","created":true,"cursor":2}` +
		`]}` + "\n"
	if w.Body.String() != want {
		t.Fatalf("body = %q\nwant %q", w.Body.String(), want)
	}

	// The calling device is the session's owning device: the stored rows name
	// it, exactly as a document-level replay by that device would.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("list = %v next=%v", rows, body["nextCursor"])
	}
	for i, row := range rows {
		r := row.(map[string]any)
		if r["deviceId"] != "dev" || r["id"] != fmt.Sprintf("o%d", i+1) {
			t.Fatalf("row %d = %v", i, r)
		}
	}
}

func TestSessionReplayHTTPSharesCursorSpace(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc1", 2)

	// The session replay continues the document-level cursor space.
	w, body := postSessionReplay(t, h, "sess", "doc1", map[string]any{
		"operations": []any{map[string]any{"id": "r1", "payload": map[string]any{"r": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("result = %v", r)
	}

	// A following ordinary commit takes the next cursor: one contiguous space
	// across the ordinary commit, merge/restore and every replay path.
	w, body = postJSON(t, h, "/v1/documents/doc1/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "d1", "payload": map[string]any{"d": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["cursor"].(float64) != 4 {
		t.Fatalf("document-level cursor = %v", r)
	}
}

func TestSessionReplayHTTPIdempotent(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"x","payload":{"a":1,"b":2}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Identical replay (decoded-equal payload, different key order and number
	// format): created=false with the first cursor, nothing new written.
	w, body := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"x","payload":{"b":2.0,"a":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("replay status = %d body = %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["id"] != "x" || r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("replay result = %v", r)
	}

	// A mixed batch resolves per element: the existing id is idempotent, the
	// new id takes the next cursor.
	w, body = postSessionReplay(t, h, "sess", "doc1", `{"operations":[
		{"id":"x","payload":{"a":1,"b":2}},
		{"id":"y","payload":null}
	]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	results := body["results"].([]any)
	r0 := results[0].(map[string]any)
	r1 := results[1].(map[string]any)
	if r0["created"] != false || r0["cursor"].(float64) != 1 {
		t.Fatalf("existing result = %v", r0)
	}
	if r1["id"] != "y" || r1["created"] != true || r1["cursor"].(float64) != 2 {
		t.Fatalf("new result = %v", r1)
	}

	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("rows = %v next = %d", rows, next)
	}
}

func TestSessionReplayHTTPConflict(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"x","payload":{"k":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Same id, different payload: 409 naming the conflicting id; the batch's
	// other (valid, new) element is not written either.
	w, body := postSessionReplay(t, h, "sess", "doc1", `{"operations":[
		{"id":"fresh","payload":{"n":1}},
		{"id":"x","payload":{"k":2}}
	]}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("payload conflict status = %d body = %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("conflict content type = %q", ct)
	}
	if body["error"] == nil || body["conflictId"] != "x" {
		t.Fatalf("conflict body = %s", w.Body.String())
	}

	// Same id and payload but a different source device is a conflict too:
	// only the session device AND the decoded payload matching is idempotent.
	createSessionViaHTTP(t, h, "dev-2", "sess-2")
	w, body = postSessionReplay(t, h, "sess-2", "doc1", `{"operations":[{"id":"x","payload":{"k":1}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "x" {
		t.Fatalf("device conflict = %d %s", w.Code, w.Body.String())
	}

	// Both conflicts left the log untouched: no half batch, cursor unmoved.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "x" || next != 1 {
		t.Fatalf("conflict leaked writes: rows = %v next = %d", rows, next)
	}
}

func TestSessionReplayHTTPRejectsBadInput(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	url := sessionReplayPath("sess", "doc1")

	cases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"operations":[{"id":"a","payload":1}]}`},
		{"missing content type", "", `{"operations":[{"id":"a","payload":1}]}`},
		{"content type with json suffix", "application/vnd.api+json", `{"operations":[{"id":"a","payload":1}]}`},
		{"malformed json", "application/json", `{"operations":[`},
		{"trailing content", "application/json", `{"operations":[{"id":"a","payload":1}]}garbage`},
		{"second json value", "application/json", `{"operations":[{"id":"a","payload":1}]}{"operations":[{"id":"b","payload":2}]}`},
		{"body is not an object", "application/json", `[{"id":"a","payload":1}]`},
		{"missing operations", "application/json", `{}`},
		{"null operations", "application/json", `{"operations":null}`},
		{"empty operations", "application/json", `{"operations":[]}`},
		{"operations not an array", "application/json", `{"operations":{"id":"a"}}`},
		{"element not an object", "application/json", `{"operations":[1]}`},
		{"empty id", "application/json", `{"operations":[{"id":"","payload":1}]}`},
		{"missing id", "application/json", `{"operations":[{"payload":1}]}`},
		{"numeric id", "application/json", `{"operations":[{"id":123,"payload":1}]}`},
		{"missing payload", "application/json", `{"operations":[{"id":"a"}]}`},
		{"duplicate ids in batch", "application/json", `{"operations":[{"id":"a","payload":1},{"id":"a","payload":2}]}`},
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

	// A JSON null payload is legal ("any legal JSON payload"): it commits.
	w, body := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"null-op","payload":null}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("null payload status = %d body = %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("null payload result = %v", body["results"])
	}

	// Zero writes from every rejected request besides the one accepted null.
	rows, next, err := s.ListChanges("doc1", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || next != 1 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionReplayHTTPSessionMissing(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Never-created session -> 404 JSON with no results.
	w, body := postSessionReplay(t, h, "ghost", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("unknown session = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("404 leaked results: %s", w.Body.String())
	}

	// Request shape precedes the session lookup: a malformed batch against an
	// unknown session is still a 400.
	w, _ = postSessionReplay(t, h, "ghost", "doc1", `{"operations":[{"id":"a","payload":1}]}trailing`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad shape + missing session = %d, want 400", w.Code)
	}

	// Delete the session -> replays through it now 404.
	w, _ = doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound || body["error"] == nil {
		t.Fatalf("deleted session = %d %v", w.Code, body)
	}

	// Zero writes: nothing reached the change log.
	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}
}

func TestSessionReplayHTTPPermissionRevoked(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Revoked: 403 JSON with no results and zero writes.
	w, body := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusForbidden || body["error"] == nil {
		t.Fatalf("revoked = %d %v", w.Code, body)
	}
	if body["results"] != nil {
		t.Fatalf("403 leaked results: %s", w.Body.String())
	}

	// Session existence precedes the permission check: an unknown session
	// against a revoked document is a 404, not a 403.
	w, _ = postSessionReplay(t, h, "ghost", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing session + revoked doc = %d, want 404", w.Code)
	}

	rows, next, _ := s.ListChanges("doc1", 0, 100)
	if len(rows) != 0 || next != 0 {
		t.Fatalf("zero-write violated: rows = %v next = %d", rows, next)
	}

	// Re-grant restores the replay path.
	w, _ = postJSON(t, h, "/v1/documents/doc1/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body = postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("re-granted replay = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 1 {
		t.Fatalf("re-granted result = %v", r)
	}
}

func TestSessionReplayHTTPRejectsBadMethodAndPath(t *testing.T) {
	h, s := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// Verbs other than POST on the replay path are a JSON 400.
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		w, _ := doRequest(t, h, method, sessionReplayPath("sess", "doc1"))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400, body = %q", method, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Empty identifiers, a trailing slash, a missing changes segment and extra
	// segments are a JSON 400, never a redirect or an HTML page.
	for _, p := range []string{
		"/v1/sessions//documents/doc1/changes/replay",
		"/v1/sessions/sess/documents//changes/replay",
		"/v1/sessions/sess/documents/doc1/changes/replay/",
		"/v1/sessions/sess/documents/doc1/replay",
		"/v1/sessions/sess/documents/doc1/changes/replay/extra",
		"/v1/sessions/sess/documents/doc1/changes/replay/extra/more",
	} {
		r := newJSONRequest(http.MethodPost, p, `{"operations":[{"id":"a","payload":1}]}`, "application/json")
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

// A session or document literally named "replay" is an ordinary identifier:
// its replay entry behaves exactly like any other id's, and its ordinary
// collection routes keep working.
func TestSessionReplayHTTPNamedReplayKeepsOrdinaryRoutes(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "replay")

	// Session named "replay": its replay entry works.
	w, body := postSessionReplay(t, h, "replay", "doc1", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("session named replay = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("session named replay result = %v", body["results"])
	}

	// Document named "replay": both the replay entry and the ordinary
	// collection commit work, sharing one cursor space.
	w, body = postSessionReplay(t, h, "replay", "replay", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("doc named replay replay = %d %s", w.Code, w.Body.String())
	}
	if body["results"].([]any)[0].(map[string]any)["cursor"].(float64) != 1 {
		t.Fatalf("doc named replay cursor = %v", body["results"])
	}
	w, _ = postJSON(t, h, "/v1/sessions/replay/documents/replay/changes", map[string]any{
		"changes": []any{map[string]any{"id": "b", "payload": 2}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("doc named replay collection post = %d %s", w.Code, w.Body.String())
	}
	w, list := doRequest(t, h, http.MethodGet, "/v1/sessions/replay/documents/replay/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if len(list["changes"].([]any)) != 2 || list["nextCursor"].(float64) != 2 {
		t.Fatalf("doc named replay list = %v", list["changes"])
	}

	// The keyword guard does not misfire on extra segments under an id named
	// replay: an extra segment is still a malformed 400.
	r := newJSONRequest(http.MethodPost, "/v1/sessions/replay/documents/replay/changes/replay/extra",
		`{"operations":[{"id":"z","payload":1}]}`, "application/json")
	if w := serveRecorder(h, r); w.Code != http.StatusBadRequest {
		t.Fatalf("extra segment under replay-named ids = %d, want 400", w.Code)
	}
}

// An extra deviceId field in the body is ignored: the calling device is
// always the session's owning device, never a body field.
func TestSessionReplayHTTPExtraDeviceFieldIgnored(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postSessionReplay(t, h, "sess", "doc1", map[string]any{
		"deviceId":   "intruder",
		"operations": []any{map[string]any{"id": "a", "payload": 1}},
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

// The session replay's success body is byte-identical to the document-level
// replay's for the same batch.
func TestSessionReplayHTTPSuccessShapeMatchesDocumentReplay(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	wDoc, _ := postJSON(t, h, "/v1/documents/docA/replay", `{"deviceId":"dev","operations":[{"id":"x","payload":{"k":1}},{"id":"y","payload":[1]}]}`)
	if wDoc.Code != http.StatusOK {
		t.Fatal(wDoc.Body.String())
	}
	wSess, _ := postSessionReplay(t, h, "sess", "docB", `{"operations":[{"id":"x","payload":{"k":1}},{"id":"y","payload":[1]}]}`)
	if wSess.Code != http.StatusOK {
		t.Fatal(wSess.Body.String())
	}
	if wDoc.Body.String() != wSess.Body.String() {
		t.Fatalf("success bodies differ:\ndocument %q\nsession  %q", wDoc.Body.String(), wSess.Body.String())
	}
}

func TestSessionReplayHTTPWakesLongPoll(t *testing.T) {
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

	// A replay carrying a new operation wakes the parked poll.
	time.Sleep(50 * time.Millisecond)
	w, _ := postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"live","payload":{"n":2}}]}`)
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
			t.Fatalf("poll timed out despite the replay: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("session replay did not wake the parked poll")
	}

	// An idempotent replay allocates no cursor and does not wake a parked poll.
	timed := make(chan map[string]any, 1)
	go func() {
		_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc1/changes/poll?after=2&waitMs=80")
		timed <- body
	}()
	time.Sleep(50 * time.Millisecond)
	w, _ = postSessionReplay(t, h, "sess", "doc1", `{"operations":[{"id":"live","payload":{"n":2.0}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	select {
	case body := <-timed:
		if body["timedOut"] != true || body["nextCursor"].(float64) != 2 {
			t.Fatalf("idempotent replay woke the poll: %v", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not return")
	}
}

func TestSessionReplayHTTPPushesToSubscribers(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	// A new operation replayed through the session entry is pushed like any
	// other write path.
	if code := postHTTP(t, srv, sessionReplayPath("sess", "doc"), map[string]any{
		"operations": []any{map[string]any{"id": "replay-1", "payload": map[string]any{"n": 2}}},
	}); code != http.StatusOK {
		t.Fatalf("session replay status = %d", code)
	}
	c := conn.readChange()
	if c.Cursor != 2 || c.ID != "replay-1" || c.DeviceID != "dev-1" {
		t.Fatalf("pushed frame = %+v", c)
	}

	// An idempotent replay pushes nothing: the subscription stays parked.
	if code := postHTTP(t, srv, sessionReplayPath("sess", "doc"), map[string]any{
		"operations": []any{map[string]any{"id": "replay-1", "payload": map[string]any{"n": 2.0}}},
	}); code != http.StatusOK {
		t.Fatalf("idempotent replay status = %d", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("idempotent replay pushed a frame")
	}
	conn.clearReadDeadline()
}

func TestSessionReplayHTTPConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	const batches, width = 20, 3
	var wg sync.WaitGroup
	errs := make(chan error, batches)
	for b := 0; b < batches; b++ {
		wg.Add(1)
		go func(b int) {
			defer wg.Done()
			ops := make([]any, width)
			for j := range ops {
				ops[j] = map[string]any{
					"id":      fmt.Sprintf("b%02d-o%d", b, j),
					"payload": map[string]any{"b": b, "j": j},
				}
			}
			w, _ := postSessionReplay(t, h, "sess", "doc", map[string]any{"operations": ops})
			if w.Code != http.StatusOK {
				errs <- fmt.Errorf("batch %d status %d: %s", b, w.Code, w.Body.String())
			}
		}(b)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// The whole batch set took effect: every row present, cursors contiguous
	// and never duplicated.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?limit=1000")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != batches*width || body["nextCursor"].(float64) != batches*width {
		t.Fatalf("stored %d changes, nextCursor = %v, want %d", len(rows), body["nextCursor"], batches*width)
	}
	seenCursor := make(map[float64]bool, batches*width)
	byBatch := make(map[string][]int64, batches)
	for _, row := range rows {
		r := row.(map[string]any)
		cursor := r["cursor"].(float64)
		if seenCursor[cursor] {
			t.Fatalf("cursor %v allocated twice", cursor)
		}
		seenCursor[cursor] = true
		byBatch[r["id"].(string)[:3]] = append(byBatch[r["id"].(string)[:3]], int64(cursor))
	}
	// Each individual batch committed as a whole: its width cursors are
	// contiguous in request order, with no other batch's row interleaved.
	for b := 0; b < batches; b++ {
		cursors := byBatch[fmt.Sprintf("b%02d", b)]
		if len(cursors) != width {
			t.Fatalf("batch %d stored %d rows, want %d", b, len(cursors), width)
		}
		for j, cursor := range cursors {
			if cursor != cursors[0]+int64(j) {
				t.Fatalf("batch %d cursors not contiguous: %v", b, cursors)
			}
		}
	}
}

func TestSessionReplayHTTPPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session-replay.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	createSessionViaHTTP(t, h, "dev", "sess")
	w, _ := postSessionReplay(t, h, "sess", "doc1", `{"operations":[
		{"id":"o1","payload":{"a":1}},
		{"id":"o2","payload":[1,true,null]}
	]}`)
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

	// Idempotency survives the restart: same session device and decoded
	// payload -> created=false with the first cursor.
	w, body := postSessionReplay(t, h2, "sess", "doc1", `{"operations":[{"id":"o1","payload":{"a":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("idempotent after restart = %v", r)
	}

	// So does the conflict decision, with the whole batch rejected.
	w, body = postSessionReplay(t, h2, "sess", "doc1", `{"operations":[
		{"id":"fresh","payload":{"z":0}},
		{"id":"o1","payload":{"a":2}}
	]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "o1" {
		t.Fatalf("conflict after restart = %d %s", w.Code, w.Body.String())
	}

	// Raw payloads read back verbatim and the cursor space continues.
	w, body = doRequest(t, h2, http.MethodGet, "/v1/documents/doc1/changes")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 2 || body["nextCursor"].(float64) != 2 {
		t.Fatalf("list after restart = %v next=%v", rows, body["nextCursor"])
	}
	second := rows[1].(map[string]any)
	payload := second["payload"].([]any)
	if len(payload) != 3 || payload[0].(float64) != 1 || payload[1] != true || payload[2] != nil {
		t.Fatalf("payload after restart = %v", second["payload"])
	}

	w, body = postSessionReplay(t, h2, "sess", "doc1", `{"operations":[{"id":"o3","payload":42}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("post-restart cursor = %v", r)
	}
}

// A compacted-away id still answers idempotency and conflict decisions from
// its retained summary when the replay arrives through the session path.
func TestSessionReplayHTTPCompactedIdentity(t *testing.T) {
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

	// The trimmed id is idempotent with its first cursor.
	w, body := postSessionReplay(t, h, "sess", "doc", `{"operations":[{"id":"c1","payload":{"n":1}}]}`)
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed idempotent = %d %s", w.Code, w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != false || r["cursor"].(float64) != 1 {
		t.Fatalf("trimmed result = %v", r)
	}

	// A differing payload against the trimmed id is still a 409.
	w, body = postSessionReplay(t, h, "sess", "doc", `{"operations":[{"id":"c1","payload":{"n":9}}]}`)
	if w.Code != http.StatusConflict || body["conflictId"] != "c1" {
		t.Fatalf("trimmed conflict = %d %s", w.Code, w.Body.String())
	}

	// New ids continue past the pre-compaction cursor space.
	w, body = postSessionReplay(t, h, "sess", "doc", `{"operations":[{"id":"c3","payload":{"n":3}}]}`)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r = body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 3 {
		t.Fatalf("post-compaction cursor = %v", r)
	}
}

// The document-level replay is untouched by the new entry: it still requires
// its body deviceId and answers with its own shapes.
func TestSessionReplayHTTPDocumentReplayUnchanged(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, _ := postJSON(t, h, "/v1/documents/doc1/replay", `{"operations":[{"id":"a","payload":1}]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("document replay without deviceId = %d, want 400", w.Code)
	}

	w, body := postJSON(t, h, "/v1/documents/doc1/replay", map[string]any{
		"deviceId":   "dev",
		"operations": []any{map[string]any{"id": "a", "payload": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	r := body["results"].([]any)[0].(map[string]any)
	if r["created"] != true || r["cursor"].(float64) != 1 {
		t.Fatalf("document replay result = %v", r)
	}
}

// A revoke committed while a subscription is live ends it with 4403,
// including when the last pushed frame arrived through the session replay;
// replays attempted after the revoke are a 403 and push nothing.
func TestSessionReplayHTTPSubscriptionRevokedCloses4403(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 1)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	if code := postHTTP(t, srv, sessionReplayPath("sess", "doc"), map[string]any{
		"operations": []any{map[string]any{"id": "replay-live", "payload": map[string]any{"n": 2}}},
	}); code != http.StatusOK {
		t.Fatalf("replay status = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 2 || c.ID != "replay-live" {
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

	if code := postHTTP(t, srv, sessionReplayPath("sess", "doc"), map[string]any{
		"operations": []any{map[string]any{"id": "after-revoke", "payload": map[string]any{"n": 3}}},
	}); code != http.StatusForbidden {
		t.Fatalf("replay after revoke = %d, want 403", code)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a frame arrived after the 4403 close")
	}
	conn.clearReadDeadline()
}

// The termination signal ends the subscription with 1001 even when its catch-up
// frames were produced by the session replay; committed data stays readable.
func TestSessionReplayHTTPSubscriptionShutdownCloses1001(t *testing.T) {
	srv, st := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 0)

	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	if code := postHTTP(t, srv, sessionReplayPath("sess", "doc"), map[string]any{
		"operations": []any{map[string]any{"id": "replay-1", "payload": map[string]any{"n": 1}}},
	}); code != http.StatusOK {
		t.Fatalf("replay status = %d", code)
	}
	if c := conn.readChange(); c.Cursor != 1 || c.ID != "replay-1" {
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
