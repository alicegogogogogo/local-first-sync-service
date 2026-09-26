package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// seedChanges posts n changes (id-1..id-n, payload {"n":i}) for doc and
// requires them all to be created.
func seedChanges(t *testing.T, h http.Handler, doc, device string, n int) {
	t.Helper()
	batch := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		batch = append(batch, map[string]any{"id": fmt.Sprintf("id-%d", i), "payload": map[string]any{"n": i}})
	}
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{"deviceId": device, "changes": batch})
	if w.Code != http.StatusOK {
		t.Fatalf("seed changes: %d %s", w.Code, w.Body.String())
	}
}

func compactChanges(t *testing.T, h http.Handler, doc, device string) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes/compact", map[string]any{"deviceId": device})
	return w
}

func TestChangesCompactEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedChanges(t, h, "doc", "dev-1", 5)

	// Snapshot at cursor 3 pins the compaction boundary.
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 3, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}

	w = compactChanges(t, h, "doc", "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"boundary\":3,\"removed\":3}\n" {
		t.Fatalf("compact body = %q", got)
	}

	// Only the online tail is listed; the page stays cursor-ascending.
	w = getRaw(t, h, "/v1/documents/doc/changes?after=0")
	if got := w.Body.String(); !strings.Contains(got, `"cursor":4`) || !strings.Contains(got, `"cursor":5`) ||
		strings.Contains(got, `"cursor":3`) || !strings.Contains(got, `"nextCursor":5`) {
		t.Fatalf("list after compact = %q", got)
	}

	// A repeat compaction reports the same boundary and zero removed.
	w = compactChanges(t, h, "doc", "dev-1")
	if got := w.Body.String(); w.Code != http.StatusOK || got != "{\"boundary\":3,\"removed\":0}\n" {
		t.Fatalf("re-compact = %d %q", w.Code, got)
	}

	// The trimmed id stays idempotent with its first cursor; a conflicting
	// re-submission is still a 409.
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "id-2", "payload": map[string]any{"n": 2}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent re-post = %d %s", w.Code, w.Body.String())
	}
	results := body["results"].([]any)
	first := results[0].(map[string]any)
	if first["created"] != false || first["cursor"] != float64(2) {
		t.Fatalf("re-post result = %v, want created=false cursor 2", first)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "id-2", "payload": map[string]any{"n": 999}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("conflicting re-post = %d %s", w.Code, w.Body.String())
	}

	// New changes continue past the greatest cursor ever allocated.
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "id-6", "payload": map[string]any{"n": 6}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post after compact = %d %s", w.Code, w.Body.String())
	}
	first = body["results"].([]any)[0].(map[string]any)
	if first["created"] != true || first["cursor"] != float64(6) {
		t.Fatalf("new change = %v, want created=true cursor 6", first)
	}
}

func TestChangesCompactFullTrimReadFloor(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedChanges(t, h, "doc", "dev-1", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}
	w = compactChanges(t, h, "doc", "dev-1")
	if got := w.Body.String(); got != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("compact body = %q", got)
	}

	// The document is still known: an empty page floors nextCursor at the
	// boundary rather than echoing 0 or the bare request cursor.
	w = getRaw(t, h, "/v1/documents/doc/changes?after=0")
	if got := w.Body.String(); got != "{\"changes\":[],\"nextCursor\":2}\n" {
		t.Fatalf("empty page = %q", got)
	}
	// Long polling reports the same floor on an immediate timeout.
	w = getRaw(t, h, "/v1/documents/doc/changes/poll?after=0&waitMs=0")
	if got := w.Body.String(); got != "{\"changes\":[],\"nextCursor\":2,\"timedOut\":true}\n" {
		t.Fatalf("poll = %q", got)
	}
	// An unknown document is unchanged: empty list, zero cursor.
	w = getRaw(t, h, "/v1/documents/ghost/changes?after=0")
	if got := w.Body.String(); got != "{\"changes\":[],\"nextCursor\":0}\n" {
		t.Fatalf("unknown doc page = %q", got)
	}
}

func TestChangesCompactUnknownDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	w := compactChanges(t, h, "ghost", "dev-1")
	if got := w.Body.String(); w.Code != http.StatusOK || got != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("compact unknown doc = %d %q", w.Code, got)
	}
	// The no-op compaction did not make the document known.
	w = getRaw(t, h, "/v1/documents/ghost/changes")
	if got := w.Body.String(); got != "{\"changes\":[],\"nextCursor\":0}\n" {
		t.Fatalf("ghost page = %q", got)
	}
}

func TestChangesCompactRequestShape(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedChanges(t, h, "doc", "dev-1", 1)

	bad := []struct {
		name        string
		contentType string
		body        string
	}{
		{"wrong content type", "text/plain", `{"deviceId":"dev-1"}`},
		{"invalid JSON", "application/json", `{"deviceId":`},
		{"trailing content", "application/json", `{"deviceId":"dev-1"} {}`},
		{"missing deviceId", "application/json", `{}`},
		{"empty deviceId", "application/json", `{"deviceId":""}`},
		{"non-string deviceId", "application/json", `{"deviceId":7}`},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			r := newJSONRequest(http.MethodPost, "/v1/documents/doc/changes/compact", tc.body, tc.contentType)
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s = %d %s", tc.name, w.Code, w.Body.String())
			}
			assertJSONError(t, w)
		})
	}

	// Method mismatches and malformed paths are JSON 400s, never redirects.
	for _, path := range []string{
		"/v1/documents/doc/changes/compact",
	} {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			r := newJSONRequest(method, path, "", "")
			w := serveRecorder(h, r)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("%s %s = %d", method, path, w.Code)
			}
			assertJSONError(t, w)
		}
	}
	for _, path := range []string{
		"/v1/documents//changes/compact",
		"/v1/documents/doc/changes/compact/",
		"/v1/documents/doc/changes/compact/extra",
		"/v1/documents/doc/compact",
	} {
		r := newJSONRequest(http.MethodPost, path, `{"deviceId":"dev-1"}`, "application/json")
		w := serveRecorder(h, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("POST %s = %d %s", path, w.Code, w.Body.String())
		}
		assertJSONError(t, w)
	}

	// Every rejected request above wrote nothing: the log is intact.
	w := getRaw(t, h, "/v1/documents/doc/changes")
	if got := w.Body.String(); !strings.Contains(got, `"nextCursor":1`) {
		t.Fatalf("log changed by rejected requests: %q", got)
	}
}

func TestChangesCompactGateOrdering(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	seedChanges(t, h, "doc", "dev-1", 1)

	// Shape failures precede device existence: a malformed body from an
	// unregistered device is a 400, not a 404.
	r := newJSONRequest(http.MethodPost, "/v1/documents/doc/changes/compact", `{}`, "application/json")
	w := serveRecorder(h, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("shape before existence = %d", w.Code)
	}

	// Unregistered device: 404 JSON, zero writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": "ghost"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unknown device = %d %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// Revoked device: 403 JSON, zero writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/permissions", map[string]any{"deviceId": "dev-2", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": "dev-2"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked device = %d %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// Neither rejection trimmed anything.
	w = getRaw(t, h, "/v1/documents/doc/changes")
	if got := w.Body.String(); !strings.Contains(got, `"nextCursor":1`) {
		t.Fatalf("log changed by rejected compactions: %q", got)
	}
}

func TestChangesCompactMergeBaseBelowBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	seedChanges(t, h, "doc", "dev-1", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact: %d %s", w.Code, w.Body.String())
	}

	// A merge based below the compaction boundary cannot be conflict-checked.
	w, _ = postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 1,
		"change":     map[string]any{"id": "m-1", "payload": map[string]any{"x": 1}},
	})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("merge below boundary = %d %s", w.Code, w.Body.String())
	}
	assertJSONError(t, w)

	// A merge based at the boundary still applies, taking the next cursor.
	w, body := postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId":   "dev-1",
		"baseCursor": 2,
		"change":     map[string]any{"id": "m-1", "payload": map[string]any{"x": 1}},
	})
	if w.Code != http.StatusOK || body["cursor"] != float64(4) || body["outcome"] != "merged" {
		t.Fatalf("merge at boundary = %d %v", w.Code, body)
	}
}

func TestChangesCompactSubscriptionBackfillsFromBoundary(t *testing.T) {
	srv, _ := newWSTestServer(t)
	setupSession(t, srv, "dev-1", "sess", "doc", 3)

	// Snapshot at cursor 2 and compact: seed-1 and seed-2 leave the log.
	postHTTP(t, srv, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if code := postHTTP(t, srv, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": "dev-1"}); code != http.StatusOK {
		t.Fatalf("compact = %d", code)
	}

	// A subscription starting inside the trimmed range backfills from the
	// first online change after the boundary, then seamlessly goes live.
	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "1"))
	if conn == nil {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	defer conn.close()

	first := conn.readChange()
	if first.Cursor != 3 || first.ID != "seed-3" {
		t.Fatalf("backfill = %+v, want cursor 3 seed-3", first)
	}
	conn.setReadDeadline(300 * time.Millisecond)
	if _, _, _, ok := conn.readFrameMaybe(); ok {
		t.Fatal("a trimmed change was pushed")
	}
	conn.clearReadDeadline()

	postDocChange(t, srv, "dev-1", "doc", "live-1", map[string]any{"n": 4})
	live := conn.readChange()
	if live.Cursor != 4 || live.ID != "live-1" {
		t.Fatalf("live frame = %+v, want cursor 4 live-1", live)
	}
}

func TestChangesCompactKeywordNamedDocument(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	// A document literally named "compact" keeps its ordinary routes; the
	// keyword is recognized only in the endpoint's own segment position.
	seedChanges(t, h, "compact", "dev-1", 1)
	w := compactChanges(t, h, "compact", "dev-1")
	if w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("compact on doc named compact = %d %q", w.Code, w.Body.String())
	}
	w = getRaw(t, h, "/v1/documents/compact/changes")
	if got := w.Body.String(); !strings.Contains(got, `"nextCursor":1`) {
		t.Fatalf("doc named compact list = %q", got)
	}
}

func TestChangesCompactPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(s)
	registerDevice(t, h, "dev-1")
	seedChanges(t, h, "doc", "dev-1", 3)
	if w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 3, "state": nil}); w.Code != http.StatusOK {
		t.Fatal("snapshot failed")
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Body.String() != "{\"boundary\":3,\"removed\":3}\n" {
		t.Fatalf("compact = %q", w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	h2 := NewHandler(s2)

	// The boundary, the trimmed identities and the high-water mark all
	// survived the restart.
	w := getRaw(t, h2, "/v1/documents/doc/changes?after=0")
	if got := w.Body.String(); got != "{\"changes\":[],\"nextCursor\":3}\n" {
		t.Fatalf("page after restart = %q", got)
	}
	w, body := postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "id-1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent re-post after restart = %d %s", w.Code, w.Body.String())
	}
	first := body["results"].([]any)[0].(map[string]any)
	if first["created"] != false || first["cursor"] != float64(1) {
		t.Fatalf("re-post after restart = %v, want created=false cursor 1", first)
	}
	w, body = postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []map[string]any{{"id": "id-4", "payload": map[string]any{"n": 4}}},
	})
	first = body["results"].([]any)[0].(map[string]any)
	if first["created"] != true || first["cursor"] != float64(4) {
		t.Fatalf("new change after restart = %v, want created=true cursor 4", first)
	}
	if w := compactChanges(t, h2, "doc", "dev-1"); w.Body.String() != "{\"boundary\":3,\"removed\":0}\n" {
		t.Fatalf("re-compact after restart = %q", w.Body.String())
	}
}
