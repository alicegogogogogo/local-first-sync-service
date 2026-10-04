package server

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCheckpointPath is the session-scoped checkpoint path.
func sessionCheckpointPath(session, doc string) string {
	return sessionChangesPath(session, doc) + "/checkpoint"
}

// putCheckpoint issues a PUT checkpoint request with a raw body and content
// type and returns the recorder.
func putCheckpoint(t *testing.T, h http.Handler, session, doc, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPut, sessionCheckpointPath(session, doc), strings.NewReader(body))
	r.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// getCheckpoint issues the checkpoint GET and returns the recorder and decoded
// body.
func getCheckpoint(t *testing.T, h http.Handler, session, doc string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodGet, sessionCheckpointPath(session, doc))
}

// revokePermission revokes device's permission on doc through the document
// permission endpoint, failing the test on error.
func revokePermission(t *testing.T, h http.Handler, doc, device string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/permissions", map[string]any{"deviceId": device, "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCheckpointLifecycle(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3) // cursors 1..3, no snapshot/compaction

	// Never confirmed: cursor 0, recorded=false, with the standing boundary and
	// high-water mark.
	w, body := getCheckpoint(t, h, "sess", "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("get = %d %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 0 || body["recorded"] != false ||
		body["boundary"].(float64) != 0 || body["maxCursor"].(float64) != 3 {
		t.Fatalf("unconfirmed body = %v", body)
	}
	if got := w.Body.String(); got != "{\"cursor\":0,\"recorded\":false,\"boundary\":0,\"maxCursor\":3}\n" {
		t.Fatalf("unconfirmed body literal = %q", got)
	}

	// First confirmation advances.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":2,\"advanced\":true}\n" {
		t.Fatalf("put 2 = %d %q", w.Code, w.Body.String())
	}

	// Re-confirming the same cursor is idempotent and writes nothing.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":2,\"advanced\":false}\n" {
		t.Fatalf("put 2 repeat = %d %q", w.Code, w.Body.String())
	}

	// Advancing reports advanced=true.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":3}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":3,\"advanced\":true}\n" {
		t.Fatalf("put 3 = %d %q", w.Code, w.Body.String())
	}

	w, body = getCheckpoint(t, h, "sess", "doc")
	if body["cursor"].(float64) != 3 || body["recorded"] != true ||
		body["boundary"].(float64) != 0 || body["maxCursor"].(float64) != 3 {
		t.Fatalf("confirmed body = %v", body)
	}
}

// An unknown document only accepts cursor 0; its read is the all-zero state and
// confirming 0 creates no document (the next change still allocates cursor 1).
func TestSessionCheckpointUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	w, body := getCheckpoint(t, h, "sess", "ghost")
	if w.Code != http.StatusOK {
		t.Fatalf("get unknown = %d %s", w.Code, w.Body.String())
	}
	if body["cursor"].(float64) != 0 || body["recorded"] != false ||
		body["boundary"].(float64) != 0 || body["maxCursor"].(float64) != 0 {
		t.Fatalf("unknown body = %v", body)
	}

	// Only zero is confirmable on an unknown document.
	if w := putCheckpoint(t, h, "sess", "ghost", `{"cursor":1}`, "application/json"); w.Code != http.StatusConflict {
		t.Fatalf("put 1 unknown = %d %s", w.Code, w.Body.String())
	} else if got := w.Body.String(); got != "{\"error\":\"checkpoint cursor is above the document maximum\",\"maxCursor\":0}\n" {
		t.Fatalf("above-max body = %q", got)
	}

	if w := putCheckpoint(t, h, "sess", "ghost", `{"cursor":0}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":0,\"advanced\":true}\n" {
		t.Fatalf("put 0 unknown = %d %q", w.Code, w.Body.String())
	}
	// Re-confirming zero is a repeat: advanced=false.
	if w := putCheckpoint(t, h, "sess", "ghost", `{"cursor":0}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":0,\"advanced\":false}\n" {
		t.Fatalf("put 0 repeat = %d %q", w.Code, w.Body.String())
	}

	// The zero confirmation created no document: its cursor space still starts
	// at 1 on the first real change.
	w2, body := postJSON(t, h, "/v1/documents/ghost/changes", map[string]any{
		"deviceId": "dev",
		"changes":  []any{map[string]any{"id": "g1", "payload": map[string]any{"n": 1}}},
	})
	if w2.Code != http.StatusOK {
		t.Fatalf("first ghost change = %d %s", w2.Code, w2.Body.String())
	}
	if c := body["results"].([]any)[0].(map[string]any)["cursor"].(float64); c != 1 {
		t.Fatalf("first ghost cursor = %v, want 1", c)
	}
}

// A cursor may not regress and may not exceed the document high-water mark;
// each 409 names the cursor value the client reconciles against and writes
// nothing.
func TestSessionCheckpointConflicts(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)

	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":3}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put 3 = %d %s", w.Code, w.Body.String())
	}

	// Regression.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusConflict {
		t.Fatalf("regress = %d, want 409", w.Code)
	} else if got := w.Body.String(); got != "{\"currentCursor\":3,\"error\":\"checkpoint cursor must not move backward\"}\n" {
		t.Fatalf("regress body = %q", got)
	}

	// Above the high-water mark.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":4}`, "application/json"); w.Code != http.StatusConflict {
		t.Fatalf("above max = %d, want 409", w.Code)
	} else if got := w.Body.String(); got != "{\"error\":\"checkpoint cursor is above the document maximum\",\"maxCursor\":3}\n" {
		t.Fatalf("above-max body = %q", got)
	}

	// Zero writes from the conflicts: the recorded cursor is still 3.
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["cursor"].(float64) != 3 {
		t.Fatalf("cursor after conflicts = %v, want 3", body["cursor"])
	}
}

// The first confirmation must be at or above the compaction boundary; equality
// is permitted. An already-confirmed pair is governed only by monotonicity and
// the high-water mark afterward.
func TestSessionCheckpointCompactionBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "late"})
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2) // boundary 2 once compacted
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// A session confirming for the first time below the boundary is sent to the
	// snapshot.
	if w := putCheckpoint(t, h, "late", "doc", `{"cursor":1}`, "application/json"); w.Code != http.StatusConflict {
		t.Fatalf("below boundary = %d, want 409", w.Code)
	} else if got := w.Body.String(); got != "{\"boundary\":2,\"error\":\"checkpoint cursor is below the compaction boundary; restore the snapshot first\"}\n" {
		t.Fatalf("boundary body = %q", got)
	}

	// Equality with the boundary is a valid first confirmation.
	if w := putCheckpoint(t, h, "late", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK ||
		w.Body.String() != "{\"cursor\":2,\"advanced\":true}\n" {
		t.Fatalf("put boundary = %d %q", w.Code, w.Body.String())
	}

	// GET reports the stored boundary and the surviving high-water mark.
	if _, body := getCheckpoint(t, h, "late", "doc"); body["boundary"].(float64) != 2 ||
		body["maxCursor"].(float64) != 3 || body["cursor"].(float64) != 2 {
		t.Fatalf("get after boundary = %v", body)
	}

	// After the first confirmation, a still-valid value above the boundary
	// advances normally; the boundary rule is first-confirmation-only.
	if w := putCheckpoint(t, h, "late", "doc", `{"cursor":3}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put 3 = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCheckpointBodyValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	for _, tc := range []struct {
		name        string
		session     string
		body        string
		contentType string
	}{
		{"invalid JSON", "sess", `{not json`, "application/json"},
		{"trailing content", "sess", `{"cursor":1} {}`, "application/json"},
		{"empty body", "sess", ``, "application/json"},
		{"wrong content type", "sess", `{"cursor":1}`, "text/plain"},
		{"no content type", "sess", `{"cursor":1}`, ""},
		{"not an object array", "sess", `[1]`, "application/json"},
		{"not an object string", "sess", `"x"`, "application/json"},
		{"not an object number", "sess", `1`, "application/json"},
		{"not an object null", "sess", `null`, "application/json"},
		{"missing cursor", "sess", `{}`, "application/json"},
		{"cursor null", "sess", `{"cursor":null}`, "application/json"},
		{"cursor float", "sess", `{"cursor":1.5}`, "application/json"},
		{"cursor float whole", "sess", `{"cursor":1.0}`, "application/json"},
		{"cursor string", "sess", `{"cursor":"1"}`, "application/json"},
		{"cursor bool", "sess", `{"cursor":true}`, "application/json"},
		{"cursor negative", "sess", `{"cursor":-1}`, "application/json"},
		{"cursor exponent", "sess", `{"cursor":1e2}`, "application/json"},
		// Request shape precedes the session lookup: a malformed body against an
		// unknown session is still a 400.
		{"malformed body unknown session", "ghost", `{`, "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := putCheckpoint(t, h, tc.session, "doc", tc.body, tc.contentType)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}

	// An unknown field is tolerated (ignored), like every session endpoint.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":1,"deviceId":"someone-else"}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("extra field = %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCheckpointGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	revokePermission(t, h, "doc2", "dev") // doc2 otherwise unknown; revoked

	// A session that never existed is a 404.
	if w := putCheckpoint(t, h, "ghost", "doc", `{"cursor":0}`, "application/json"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session put = %d, want 404", w.Code)
	}
	if w, _ := getCheckpoint(t, h, "ghost", "doc"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session get = %d, want 404", w.Code)
	}

	// A deleted session is a 404 even with a valid body.
	w0, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "gone"})
	if w0.Code != http.StatusOK {
		t.Fatalf("create gone = %d %s", w0.Code, w0.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/gone"); w.Code != http.StatusOK {
		t.Fatalf("delete gone = %d", w.Code)
	}
	if w := putCheckpoint(t, h, "gone", "doc", `{"cursor":0}`, "application/json"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session = %d, want 404", w.Code)
	}

	// Revoked permission is a 403 after shape and session checks, for both
	// verbs, even though doc2 is otherwise unknown.
	if w := putCheckpoint(t, h, "sess", "doc2", `{"cursor":0}`, "application/json"); w.Code != http.StatusForbidden {
		t.Fatalf("revoked put = %d, want 403", w.Code)
	}
	if w, _ := getCheckpoint(t, h, "sess", "doc2"); w.Code != http.StatusForbidden {
		t.Fatalf("revoked get = %d, want 403", w.Code)
	}
}

func TestSessionCheckpointPathAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"empty session id", http.MethodPut, "/v1/sessions//documents/doc/changes/checkpoint"},
		{"empty document id", http.MethodPut, "/v1/sessions/sess/documents//changes/checkpoint"},
		{"trailing slash", http.MethodPut, "/v1/sessions/sess/documents/doc/changes/checkpoint/"},
		{"extra segment", http.MethodPut, "/v1/sessions/sess/documents/doc/changes/checkpoint/x"},
		{"missing changes segment", http.MethodPut, "/v1/sessions/sess/documents/doc/checkpoint"},
		{"wrong method POST", http.MethodPost, "/v1/sessions/sess/documents/doc/changes/checkpoint"},
		{"wrong method DELETE", http.MethodDelete, "/v1/sessions/sess/documents/doc/changes/checkpoint"},
		{"wrong method PATCH", http.MethodPatch, "/v1/sessions/sess/documents/doc/changes/checkpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"cursor":0}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s, want 400", w.Code, w.Body.String())
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}

	// A session literally named "checkpoint" is an ordinary identifier.
	createSessionViaHTTP(t, h, "dev2", "checkpoint")
	if w, _ := getCheckpoint(t, h, "checkpoint", "doc"); w.Code != http.StatusOK {
		t.Fatalf("session named checkpoint = %d %s", w.Code, w.Body.String())
	}
}

// Confirming a checkpoint neither allocates a cursor nor wakes a parked long
// poll, and reading changes does not advance it.
func TestSessionCheckpointNoSideEffects(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put = %d %s", w.Code, w.Body.String())
	}

	// Reading changes does not move the checkpoint.
	if _, body := doRequest(t, h, http.MethodGet, sessionChangesPath("sess", "doc")+"?after=0"); body == nil {
		t.Fatal("changes read failed")
	}
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["cursor"].(float64) != 1 {
		t.Fatalf("checkpoint moved after read = %v", body["cursor"])
	}

	// A parked poll at the tail is not woken by a checkpoint confirmation.
	type pollResult struct{ body map[string]any }
	done := make(chan pollResult, 1)
	go func() {
		_, b := doRequest(t, h, http.MethodGet,
			sessionChangesPath("sess", "doc")+"/poll?after=2&waitMs=300")
		done <- pollResult{b}
	}()
	time.Sleep(100 * time.Millisecond)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put 2 = %d %s", w.Code, w.Body.String())
	}
	select {
	case got := <-done:
		if got.body["timedOut"] != true {
			t.Fatalf("poll timedOut = %v", got.body)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll returned early: the checkpoint confirmation woke it")
	}

	// The confirmation allocated no change cursor: the high-water mark is 2.
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["maxCursor"].(float64) != 2 {
		t.Fatalf("maxCursor = %v, want 2", body["maxCursor"])
	}
}

// Concurrent confirmations stay monotonic: same-cursor races collapse to one
// advance, and a racing mix of values never returns a 500 or overwrites a
// larger cursor with a smaller one.
func TestSessionCheckpointConcurrentMonotonic(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)

	const n = 24
	// All goroutines confirm the same valid cursor concurrently: exactly one
	// advances and the rest are idempotent repeats, none regress.
	var wg sync.WaitGroup
	var advanced, repeated int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			w := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`, "application/json")
			mu.Lock()
			defer mu.Unlock()
			if w.Code != http.StatusOK {
				t.Errorf("concurrent put = %d %s", w.Code, w.Body.String())
				return
			}
			if strings.Contains(w.Body.String(), `"advanced":true`) {
				advanced++
			} else {
				repeated++
			}
		}()
	}
	close(start)
	wg.Wait()
	if advanced != 1 || repeated != n-1 {
		t.Fatalf("advanced=%d repeated=%d, want 1 and %d", advanced, repeated, n-1)
	}

	// A racing mix of the valid cursors 0 and 1: every verdict is 200 (the
	// only possible conflict would be a regression, and 0 is never recorded
	// after 1 won because each regression would surface as 409 — accept 200 or
	// 409, but never a 500, and the final value must be the maximum, 1).
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := i % 2
			w := putCheckpoint(t, h, "sess", "doc", `{"cursor":`+strconv.Itoa(c)+`}`, "application/json")
			if w.Code != http.StatusOK && w.Code != http.StatusConflict {
				t.Errorf("mixed put = %d %s", w.Code, w.Body.String())
			}
		}(i)
	}
	wg.Wait()
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["cursor"].(float64) != 1 {
		t.Fatalf("cursor after races = %v, want 1", body["cursor"])
	}
}

// Deleting the session clears its checkpoint; the id created again starts
// unconfirmed.
func TestSessionCheckpointSessionDeleteCascade(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess"); w.Code != http.StatusOK {
		t.Fatalf("delete session = %d", w.Code)
	}
	// Re-create the same session id: it is brand new and unconfirmed.
	w, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatalf("recreate = %d %s", w.Code, w.Body.String())
	}
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["recorded"] != false || body["cursor"].(float64) != 0 {
		t.Fatalf("checkpoint survived session delete = %v", body)
	}
}

// Deregistering the device clears its sessions' checkpoints.
func TestSessionCheckpointDeviceDeleteCascade(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 1)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev"); w.Code != http.StatusOK {
		t.Fatalf("deregister = %d", w.Code)
	}
	// Re-register device and session: the position is gone.
	createSessionViaHTTP(t, h, "dev", "sess")
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["recorded"] != false {
		t.Fatalf("checkpoint survived device delete = %v", body)
	}
}

// Deleting the document clears every session checkpoint on it; the re-created
// document starts unconfirmed with a cursor space back at 1.
func TestSessionCheckpointDocumentDeleteCascade(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev"); w.Code != http.StatusOK {
		t.Fatalf("delete document = %d %s", w.Code, w.Body.String())
	}
	if _, body := getCheckpoint(t, h, "sess", "doc"); body["recorded"] != false ||
		body["cursor"].(float64) != 0 || body["maxCursor"].(float64) != 0 {
		t.Fatalf("checkpoint survived document delete = %v", body)
	}
}

// A permission revoke clears the device's session checkpoints on the document
// inside the revoke; re-authorization starts from the unconfirmed state.
func TestSessionCheckpointRevokeClearsAndReauthorize(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess2"})
	seedDoc(t, h, "doc", 2)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put sess = %d %s", w.Code, w.Body.String())
	}
	if w := putCheckpoint(t, h, "sess2", "doc", `{"cursor":1}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put sess2 = %d %s", w.Code, w.Body.String())
	}

	revokePermission(t, h, "doc", "dev")
	// Both sessions are denied while revoked.
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusForbidden {
		t.Fatalf("revoked put = %d, want 403", w.Code)
	}

	// Re-authorize: both sessions read as never confirmed even though the
	// document and its cursors are unchanged.
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatalf("grant = %d %s", w.Code, w.Body.String())
	}
	for _, sess := range []string{"sess", "sess2"} {
		if _, body := getCheckpoint(t, h, sess, "doc"); body["recorded"] != false ||
			body["cursor"].(float64) != 0 || body["maxCursor"].(float64) != 2 {
			t.Fatalf("%s after regrant = %v", sess, body)
		}
	}
}

// A checkpoint survives a process restart.
func TestSessionCheckpointRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`, "application/json"); w.Code != http.StatusOK {
		t.Fatalf("put = %d %s", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()
	// The durable device and session rows are re-established through the same
	// create calls (created=false).
	postJSON(t, h2, "/v1/devices/dev/sessions", map[string]any{"sessionId": "sess"})
	w, body := getCheckpoint(t, h2, "sess", "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("get after restart = %d %s", w.Code, w.Body.String())
	}
	if body["recorded"] != true || body["cursor"].(float64) != 2 || body["maxCursor"].(float64) != 2 {
		t.Fatalf("checkpoint after restart = %v", body)
	}
}
