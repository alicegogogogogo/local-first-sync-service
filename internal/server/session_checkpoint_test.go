package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// sessionCheckpointPath is the session-scoped sync checkpoint path.
func sessionCheckpointPath(session, doc string) string {
	return sessionChangesPath(session, doc) + "/checkpoint"
}

// putCheckpoint sends one raw confirmation body to the session checkpoint
// endpoint and returns the recorder and the decoded body.
func putCheckpoint(t *testing.T, h http.Handler, session, doc string, body any) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	var r *http.Request
	switch b := body.(type) {
	case string:
		r = httptest.NewRequest(http.MethodPut, sessionCheckpointPath(session, doc), strings.NewReader(b))
	default:
		raw, _ := json.Marshal(b)
		r = httptest.NewRequest(http.MethodPut, sessionCheckpointPath(session, doc), bytes.NewReader(raw))
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var decoded map[string]any
	if w.Body.Len() > 0 {
		_ = json.Unmarshal(w.Body.Bytes(), &decoded)
	}
	return w, decoded
}

// getCheckpoint reads the session checkpoint and returns the recorder and the
// decoded body.
func getCheckpoint(t *testing.T, h http.Handler, session, doc string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	return doRequest(t, h, http.MethodGet, sessionCheckpointPath(session, doc))
}

// addSessionViaHTTP creates one session on an already registered device.
func addSessionViaHTTP(t *testing.T, h http.Handler, device, session string) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/devices/"+device+"/sessions", map[string]any{"sessionId": session})
	if w.Code != http.StatusOK {
		t.Fatalf("create session: %d %s", w.Code, w.Body.String())
	}
}

func TestSessionCheckpointConfirmAndRead(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)

	// Never confirmed: the read reports the all-zero cursor with
	// recorded=false against the live boundary and high-water mark.
	w, body := getCheckpoint(t, h, "sess", "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("get = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"cursor\":0,\"recorded\":false,\"boundary\":0,\"maxCursor\":3}\n" {
		t.Fatalf("unconfirmed body = %q", got)
	}

	// First confirmation advances.
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`)
	if w.Code != http.StatusOK || body["advanced"] != true || int64(body["cursor"].(float64)) != 2 {
		t.Fatalf("first confirm = %d %v", w.Code, body)
	}

	// The read reports the recorded position.
	w, body = getCheckpoint(t, h, "sess", "doc")
	if w.Code != http.StatusOK {
		t.Fatalf("get after confirm = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"cursor\":2,\"recorded\":true,\"boundary\":0,\"maxCursor\":3}\n" {
		t.Fatalf("confirmed body = %q", got)
	}

	// Re-confirming the same cursor is idempotent: advanced=false.
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`)
	if w.Code != http.StatusOK || body["advanced"] != false || int64(body["cursor"].(float64)) != 2 {
		t.Fatalf("repeat confirm = %d %v", w.Code, body)
	}

	// Advancing to the high-water mark succeeds.
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":3}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("advance confirm = %d %v", w.Code, body)
	}

	// Confirming zero on a fresh pair is a real first write.
	addSessionViaHTTP(t, h, "dev", "sess2")
	w, body = putCheckpoint(t, h, "sess2", "doc", `{"cursor":0}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("zero confirm = %d %v", w.Code, body)
	}
	_, body = getCheckpoint(t, h, "sess2", "doc")
	if body["recorded"] != true || int64(body["cursor"].(float64)) != 0 {
		t.Fatalf("zero confirmed state = %v", body)
	}
}

func TestSessionCheckpointBodyValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	// Shape failures precede the session lookup: a malformed body against an
	// unknown session is still a 400.
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
		{"missing cursor", "sess", `{}`, "application/json"},
		{"null cursor", "sess", `{"cursor":null}`, "application/json"},
		{"negative cursor", "sess", `{"cursor":-1}`, "application/json"},
		{"fraction cursor", "sess", `{"cursor":1.5}`, "application/json"},
		{"float integer cursor", "sess", `{"cursor":1.0}`, "application/json"},
		{"string cursor", "sess", `{"cursor":"1"}`, "application/json"},
		{"boolean cursor", "sess", `{"cursor":true}`, "application/json"},
		{"array body", "sess", `[1]`, "application/json"},
		{"malformed body unknown session", "ghost", `{`, "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPut, sessionCheckpointPath(tc.session, "doc"), strings.NewReader(tc.body))
			r.Header.Set("Content-Type", tc.contentType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}

	// Every rejection wrote nothing: the pair is still unconfirmed.
	_, body := getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false {
		t.Fatalf("state after rejections = %v", body)
	}
}

func TestSessionCheckpointGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// An unknown or deleted session is a 404 on both verbs, even with an
	// otherwise valid body.
	if w, _ := putCheckpoint(t, h, "ghost", "doc", `{"cursor":1}`); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := getCheckpoint(t, h, "ghost", "doc"); w.Code != http.StatusNotFound {
		t.Fatalf("unknown session get = %d %s", w.Code, w.Body.String())
	}
	gw, _ := postJSON(t, h, "/v1/devices/dev/sessions", map[string]any{"sessionId": "gone"})
	if gw.Code != http.StatusOK {
		t.Fatalf("create gone session: %d %s", gw.Code, gw.Body.String())
	}
	if dw, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/gone"); dw.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", dw.Code, dw.Body.String())
	}
	if w, _ := putCheckpoint(t, h, "gone", "doc", `{"cursor":1}`); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := getCheckpoint(t, h, "gone", "doc"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted session get = %d %s", w.Code, w.Body.String())
	}

	// A revoked device permission is a 403 on both verbs, after the shape and
	// session checks pass.
	if w, _ := putCheckpoint(t, h, "sess", "doc2", `{"cursor":1}`); w.Code != http.StatusForbidden {
		t.Fatalf("revoked put = %d %s", w.Code, w.Body.String())
	}
	if w, _ := getCheckpoint(t, h, "sess", "doc2"); w.Code != http.StatusForbidden {
		t.Fatalf("revoked get = %d %s", w.Code, w.Body.String())
	}

	// The revoked document stays unreadable, but the authorized document is
	// untouched and still confirms.
	w, body := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("authorized confirm after rejections = %d %v", w.Code, body)
	}
}

func TestSessionCheckpointCursorConflicts(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)

	// Beyond the high-water mark: 409 carrying maxCursor.
	w, body := putCheckpoint(t, h, "sess", "doc", `{"cursor":4}`)
	if w.Code != http.StatusConflict || int64(body["maxCursor"].(float64)) != 3 {
		t.Fatalf("beyond max = %d %v", w.Code, body)
	}

	// The rejected confirmation wrote nothing.
	_, body = getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false {
		t.Fatalf("state after beyond-max rejection = %v", body)
	}

	// Confirm, then a regression is a 409 carrying currentCursor.
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`)
	if w.Code != http.StatusConflict || int64(body["currentCursor"].(float64)) != 2 {
		t.Fatalf("regression = %d %v", w.Code, body)
	}
	// The recorded position is unchanged.
	_, body = getCheckpoint(t, h, "sess", "doc")
	if int64(body["cursor"].(float64)) != 2 || body["recorded"] != true {
		t.Fatalf("state after regression = %v", body)
	}
}

func TestSessionCheckpointCompactionBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// A first confirmation below the boundary is a 409 carrying the boundary.
	w, body := putCheckpoint(t, h, "sess", "doc", `{"cursor":1}`)
	if w.Code != http.StatusConflict || int64(body["boundary"].(float64)) != 2 {
		t.Fatalf("below boundary = %d %v", w.Code, body)
	}
	_, body = getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false || int64(body["boundary"].(float64)) != 2 {
		t.Fatalf("state after boundary rejection = %v", body)
	}

	// Confirming exactly the boundary succeeds.
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("boundary confirm = %d %v", w.Code, body)
	}
}

func TestSessionCheckpointUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")

	// The unknown document reads as the all-zero state.
	w, body := getCheckpoint(t, h, "sess", "ghost")
	if w.Code != http.StatusOK {
		t.Fatalf("get unknown = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"cursor\":0,\"recorded\":false,\"boundary\":0,\"maxCursor\":0}\n" {
		t.Fatalf("unknown body = %q", got)
	}

	// Only cursor 0 confirms on an unknown document.
	w, body = putCheckpoint(t, h, "sess", "ghost", `{"cursor":1}`)
	if w.Code != http.StatusConflict || int64(body["maxCursor"].(float64)) != 0 {
		t.Fatalf("unknown beyond = %d %v", w.Code, body)
	}
	w, body = putCheckpoint(t, h, "sess", "ghost", `{"cursor":0}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("unknown zero confirm = %d %v", w.Code, body)
	}

	// The confirmation never created the document: its change log is still
	// empty and its deletion still misses.
	_, body = doRequest(t, h, http.MethodGet, "/v1/sessions/sess/documents/ghost/changes")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("ghost changes = %v", got)
	}
	if next := int64(body["nextCursor"].(float64)); next != 0 {
		t.Fatalf("ghost nextCursor = %d, want 0", next)
	}
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/ghost?deviceId=dev"); w.Code != http.StatusNotFound {
		t.Fatalf("ghost delete = %d, want 404", w.Code)
	}
}

// A checkpoint confirmation is not a change: it allocates no document cursor
// and wakes no parked long poll.
func TestSessionCheckpointHasNoChangeSideEffects(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)

	type pollResult struct {
		body    map[string]any
		elapsed time.Duration
	}
	done := make(chan pollResult, 1)
	go func() {
		start := time.Now()
		_, body := doRequest(t, h, http.MethodGet,
			"/v1/sessions/sess/documents/doc/changes/poll?after=2&waitMs=300")
		done <- pollResult{body, time.Since(start)}
	}()
	time.Sleep(50 * time.Millisecond)

	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}

	select {
	case got := <-done:
		if got.body["timedOut"] != true {
			t.Fatalf("poll timedOut = %v", got.body)
		}
		if got.elapsed < 200*time.Millisecond {
			t.Fatalf("poll returned after %v: the confirmation woke it", got.elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return")
	}

	// The next committed change continues the cursor space at 3: the
	// confirmation occupied no cursor.
	w, body := postJSON(t, h, sessionChangesPath("sess", "doc"), map[string]any{
		"changes": []any{map[string]any{"id": "c3", "payload": map[string]any{"n": 3}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post after confirm: %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 3 {
		t.Fatalf("new change result = %v, want created cursor 3", result)
	}

	// Reading the change log never advances the checkpoint.
	if _, body := getCheckpoint(t, h, "sess", "doc"); int64(body["cursor"].(float64)) != 2 {
		t.Fatalf("checkpoint after reads = %v, want cursor 2", body)
	}
}

// Concurrent confirmations of the same pair stay monotonic: the recorded
// position ends at the greatest confirmed cursor and a smaller value never
// overwrites it.
func TestSessionCheckpointConcurrentMonotonic(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 50)

	var wg sync.WaitGroup
	results := make(chan int, 50)
	for i := 1; i <= 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			w, _ := putCheckpoint(t, h, "sess", "doc", map[string]any{"cursor": n})
			results <- w.Code
		}(i)
	}
	wg.Wait()
	close(results)
	for code := range results {
		if code != http.StatusOK && code != http.StatusConflict {
			t.Fatalf("concurrent confirm status = %d", code)
		}
	}

	_, body := getCheckpoint(t, h, "sess", "doc")
	if got := int64(body["cursor"].(float64)); got != 50 {
		t.Fatalf("recorded cursor after race = %d, want 50", got)
	}
}

func TestSessionCheckpointSessionDeleteClears(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}

	// Deleting the session clears its checkpoints: the re-created session of
	// the same id starts unconfirmed.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev/sessions/sess"); w.Code != http.StatusOK {
		t.Fatalf("delete session = %d %s", w.Code, w.Body.String())
	}
	addSessionViaHTTP(t, h, "dev", "sess")
	_, body := getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false || int64(body["cursor"].(float64)) != 0 {
		t.Fatalf("state after session re-create = %v", body)
	}
}

func TestSessionCheckpointRevokeClears(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}

	// Revoking the device's permission clears every checkpoint its sessions
	// recorded on the document.
	w, _ := postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w, _ := getCheckpoint(t, h, "sess", "doc"); w.Code != http.StatusForbidden {
		t.Fatalf("get while revoked = %d, want 403", w.Code)
	}

	// Re-authorizing treats the pair as never confirmed.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev", "action": "grant"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	_, body := getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false || int64(body["cursor"].(float64)) != 0 {
		t.Fatalf("state after re-grant = %v", body)
	}
	// A fresh first confirmation succeeds again.
	w, body = putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("confirm after re-grant = %d %v", w.Code, body)
	}
}

func TestSessionCheckpointDocumentDeleteClears(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}

	// Deleting the document clears its checkpoints: the re-created document
	// starts unconfirmed and the cursor space restarts.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/documents/doc?deviceId=dev"); w.Code != http.StatusOK {
		t.Fatalf("delete document = %d %s", w.Code, w.Body.String())
	}
	_, body := getCheckpoint(t, h, "sess", "doc")
	if got := int64(body["cursor"].(float64)); body["recorded"] != false || got != 0 {
		t.Fatalf("state after document delete = %v", body)
	}
	seedDoc(t, h, "doc", 1)
	_, body = getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false || int64(body["maxCursor"].(float64)) != 1 {
		t.Fatalf("state after document re-create = %v", body)
	}
}

func TestSessionCheckpointDeviceDeregisterClears(t *testing.T) {
	h, _ := newTestHandler(t)
	createSessionViaHTTP(t, h, "dev", "sess")
	seedDoc(t, h, "doc", 2)
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":2}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}

	// Deregistering the device removes its sessions and their checkpoints:
	// the re-registered device and session start unconfirmed.
	if w, _ := doRequest(t, h, http.MethodDelete, "/v1/devices/dev"); w.Code != http.StatusOK {
		t.Fatalf("deregister = %d %s", w.Code, w.Body.String())
	}
	if w, _ := getCheckpoint(t, h, "sess", "doc"); w.Code != http.StatusNotFound {
		t.Fatalf("get after deregister = %d, want 404", w.Code)
	}
	w, _ := postJSON(t, h, "/v1/devices", map[string]any{"deviceId": "dev"})
	if w.Code != http.StatusOK {
		t.Fatalf("re-register: %d %s", w.Code, w.Body.String())
	}
	addSessionViaHTTP(t, h, "dev", "sess")
	_, body := getCheckpoint(t, h, "sess", "doc")
	if body["recorded"] != false || int64(body["cursor"].(float64)) != 0 {
		t.Fatalf("state after device re-register = %v", body)
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
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"cursor":1}`))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
			if loc := w.Header().Get("Location"); loc != "" {
				t.Fatalf("unexpected redirect to %q", loc)
			}
		})
	}

	// A session or document literally named "checkpoint" is an ordinary
	// identifier, not the endpoint keyword.
	createSessionViaHTTP(t, h, "dev2", "checkpoint")
	w, body := putCheckpoint(t, h, "checkpoint", "doc", `{"cursor":1}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("session named checkpoint = %d %v", w.Code, body)
	}
	w, body = putCheckpoint(t, h, "sess", "checkpoint", `{"cursor":0}`)
	if w.Code != http.StatusOK || body["advanced"] != true {
		t.Fatalf("document named checkpoint = %d %v", w.Code, body)
	}
}

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
	seedDoc(t, h, "doc", 3)
	snapshotViaHTTP(t, h, "doc", 2)
	if w := postSessionCompact(t, h, "sess", "doc", `{}`); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if w, _ := putCheckpoint(t, h, "sess", "doc", `{"cursor":3}`); w.Code != http.StatusOK {
		t.Fatalf("confirm = %d %s", w.Code, w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()

	// The checkpoint, the boundary and the high-water mark survive the
	// restart; the regression and idempotency judgments are intact.
	_, body := getCheckpoint(t, h2, "sess", "doc")
	if body["recorded"] != true || int64(body["cursor"].(float64)) != 3 ||
		int64(body["boundary"].(float64)) != 2 || int64(body["maxCursor"].(float64)) != 3 {
		t.Fatalf("state after restart = %v", body)
	}
	w, body := putCheckpoint(t, h2, "sess", "doc", `{"cursor":3}`)
	if w.Code != http.StatusOK || body["advanced"] != false {
		t.Fatalf("repeat confirm after restart = %d %v", w.Code, body)
	}
	w, body = putCheckpoint(t, h2, "sess", "doc", `{"cursor":2}`)
	if w.Code != http.StatusConflict || int64(body["currentCursor"].(float64)) != 3 {
		t.Fatalf("regression after restart = %d %v", w.Code, body)
	}
}
