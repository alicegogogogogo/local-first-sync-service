package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// compactChanges posts the compaction request and returns the raw recorder so
// tests can assert the exact single-line body.
func compactChanges(t *testing.T, h http.Handler, doc, device string) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes/compact", map[string]any{"deviceId": device})
	return w
}

// postDocChanges posts one batch of changes with ids c1..cN and payloads
// {"n":i} from the given device.
func postDocChanges(t *testing.T, h http.Handler, doc, device string, n int) {
	t.Helper()
	batch := make([]any, n)
	for i := range batch {
		batch[i] = map[string]any{"id": fmt.Sprintf("c%d", i+1), "payload": map[string]any{"n": i + 1}}
	}
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/changes", map[string]any{
		"deviceId": device,
		"changes":  batch,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post changes: %d %s", w.Code, w.Body.String())
	}
}

func TestChangesCompactEndToEnd(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)

	// Snapshot at cursor 2, so the compaction boundary is 2.
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": map[string]any{"s": 1}})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body.String())
	}

	w = compactChanges(t, h, "doc", "dev-1")
	if w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("compact body = %q", got)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("compact content type = %q", ct)
	}

	// Only the online tail is readable, in cursor order.
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=0")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	rows := body["changes"].([]any)
	if len(rows) != 1 {
		t.Fatalf("changes after compact = %v", rows)
	}
	row := rows[0].(map[string]any)
	if row["id"] != "c3" || row["deviceId"] != "dev-1" || int64(row["cursor"].(float64)) != 3 {
		t.Fatalf("remaining row = %v", row)
	}
	if next := int64(body["nextCursor"].(float64)); next != 3 {
		t.Fatalf("nextCursor = %d, want 3", next)
	}

	// An empty page inside the trimmed range reports the boundary, not after.
	_, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=3")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("changes after 3 = %v", got)
	}
	if next := int64(body["nextCursor"].(float64)); next != 3 {
		t.Fatalf("nextCursor after 3 = %d, want 3", next)
	}

	// Repeating the compaction is not an error: same boundary, zero removed.
	w = compactChanges(t, h, "doc", "dev-1")
	if w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":2,\"removed\":0}\n" {
		t.Fatalf("re-compact = %d %q", w.Code, w.Body.String())
	}

	// The cursor space continues past the pre-compaction maximum.
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("post after compact: %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 4 {
		t.Fatalf("new change result = %v, want created cursor 4", result)
	}
}

func TestChangesCompactUnknownAndSnapshotless(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")

	// An unknown document compacts successfully with boundary 0, removing
	// nothing.
	w := compactChanges(t, h, "ghost", "dev-1")
	if w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("unknown compact = %d %q", w.Code, w.Body.String())
	}
	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/ghost/changes")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("unknown changes = %v", got)
	}
	if next := int64(body["nextCursor"].(float64)); next != 0 {
		t.Fatalf("unknown nextCursor = %d, want 0", next)
	}

	// A document without snapshots has boundary 0: nothing leaves the log.
	postDocChanges(t, h, "doc", "dev-1", 2)
	w = compactChanges(t, h, "doc", "dev-1")
	if w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":0,\"removed\":0}\n" {
		t.Fatalf("snapshotless compact = %d %q", w.Code, w.Body.String())
	}
	_, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if got := body["changes"].([]any); len(got) != 2 {
		t.Fatalf("snapshotless changes = %v", got)
	}
}

func TestChangesCompactValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 1)
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// Request-shape failures: 400 JSON and zero writes.
	t.Run("content type", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/changes/compact", strings.NewReader(`{"deviceId":"dev-1"}`))
		r.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("invalid JSON", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes/compact", `{"deviceId":`)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("trailing content", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes/compact", `{"deviceId":"dev-1"} {}`)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("missing deviceId", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("empty deviceId", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": ""})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("deviceId wrong type", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": 7})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})

	// Path and method failures: 400 JSON, never a redirect or HTML.
	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"empty document id", http.MethodPost, "/v1/documents//changes/compact"},
		{"trailing slash", http.MethodPost, "/v1/documents/doc/changes/compact/"},
		{"extra segment", http.MethodPost, "/v1/documents/doc/changes/compact/x"},
		{"missing changes segment", http.MethodPost, "/v1/documents/doc/compact"},
		{"wrong method GET", http.MethodGet, "/v1/documents/doc/changes/compact"},
		{"wrong method DELETE", http.MethodDelete, "/v1/documents/doc/changes/compact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"deviceId":"dev-1"}`))
			r.Header.Set("Content-Type", "application/json")
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

	// Gate failures: unregistered device 404, revoked permission 403, both
	// after the shape checks and with zero writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes/compact", map[string]any{"deviceId": "ghost"})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered = %d %s", w.Code, w.Body.String())
	}
	w = compactChanges(t, h, "doc2", "dev-1")
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked = %d %s", w.Code, w.Body.String())
	}

	// None of the failures wrote anything: the log is intact and no boundary
	// was recorded.
	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes")
	if got := body["changes"].([]any); len(got) != 1 {
		t.Fatalf("changes after failed compactions = %v", got)
	}
}

func TestChangesCompactIdempotencyAndConflict(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc", "dev-1", 2)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// Re-posting a trimmed id with the same device and payload is idempotent:
	// created=false with the first cursor.
	w, body := postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay = %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != false || int64(result["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed replay result = %v, want created=false cursor 1", result)
	}

	// A differing payload or a different source device is a 409 with zero
	// writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 99}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("payload mismatch = %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-2",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("device mismatch = %d %s", w.Code, w.Body.String())
	}

	// The conflicts wrote nothing: no new cursor was allocated.
	_, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes?after=0")
	if got := body["changes"].([]any); len(got) != 0 {
		t.Fatalf("changes after conflicts = %v", got)
	}
}

func TestChangesCompactRestoreProvenance(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)
	for _, cursor := range []int{1, 2} {
		w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": cursor, "state": map[string]any{"snap": cursor}})
		if w.Code != http.StatusOK {
			t.Fatalf("snapshot %d: %s", cursor, w.Body.String())
		}
	}

	// Restore snapshot 2 as change r1 (cursor 3), then snapshot at 3 so the
	// compaction boundary covers the restore.
	w, _ := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("restore: %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 3, "state": map[string]any{"snap": 3}})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":3,\"removed\":3}\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}

	// The trimmed restore replays idempotently from its retained provenance.
	w, body := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed restore replay = %d %s", w.Code, w.Body.String())
	}
	if body["created"] != false || int64(body["cursor"].(float64)) != 3 || int64(body["restoredFrom"].(float64)) != 2 {
		t.Fatalf("trimmed restore result = %v", body)
	}

	// A different snapshot source for the same id is a 409 with zero writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("restore source mismatch = %d %s", w.Code, w.Body.String())
	}

	// A trimmed ordinary change id cannot be reused as a restore id.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "c1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("ordinary id as restore = %d %s", w.Code, w.Body.String())
	}

	// Snapshot export and reads are untouched by the compaction.
	w = getRaw(t, h, "/v1/documents/doc/snapshots")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	var exported map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &exported); err != nil {
		t.Fatal(err)
	}
	if int64(exported["count"].(float64)) != 3 {
		t.Fatalf("exported snapshots = %s", w.Body.String())
	}
}

func TestChangesCompactMergeBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}

	// A base cursor below the boundary cannot complete the conflict check:
	// 400 and zero writes.
	for _, base := range []int{0, 1} {
		w, _ := postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
			"deviceId": "dev-1", "baseCursor": base,
			"change": map[string]any{"id": fmt.Sprintf("m%d", base), "payload": map[string]any{"k": 1}},
		})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("merge base %d = %d %s", base, w.Code, w.Body.String())
		}
	}

	// A base cursor at the boundary merges against the online tail.
	w, body := postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId": "dev-1", "baseCursor": 2,
		"change": map[string]any{"id": "m2", "payload": map[string]any{"k": 2}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge at boundary = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "merged" || int64(body["cursor"].(float64)) != 4 {
		t.Fatalf("merge result = %v", body)
	}

	// A trimmed id replays idempotently through merge with its first cursor.
	w, body = postJSON(t, h, "/v1/documents/doc/merge", map[string]any{
		"deviceId": "dev-1", "baseCursor": 4,
		"change": map[string]any{"id": "c1", "payload": map[string]any{"n": 1}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("merge trimmed id = %d %s", w.Code, w.Body.String())
	}
	if body["outcome"] != "idempotent" || int64(body["cursor"].(float64)) != 1 {
		t.Fatalf("merge trimmed result = %v", body)
	}
}

func TestChangesCompactPollAndSubscription(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	w, _ := postJSON(t, h, "/v1/devices/dev-1/sessions", map[string]any{"sessionId": "sess"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	postDocChanges(t, h, "doc", "dev-1", 3)
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// A long poll parked at the tail is not woken by the compaction: it
	// returns only when its own wait deadline expires.
	type pollResult struct {
		body    map[string]any
		elapsed time.Duration
	}
	pollDone := make(chan pollResult, 1)
	go func() {
		start := time.Now()
		_, body := doRequest(t, h, http.MethodGet, "/v1/documents/doc/changes/poll?after=3&waitMs=300")
		pollDone <- pollResult{body, time.Since(start)}
	}()
	time.Sleep(50 * time.Millisecond)
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK {
		t.Fatalf("compact = %d %s", w.Code, w.Body.String())
	}
	select {
	case got := <-pollDone:
		if got.body["timedOut"] != true {
			t.Fatalf("poll timedOut = %v", got.body)
		}
		if next := int64(got.body["nextCursor"].(float64)); next != 3 {
			t.Fatalf("poll nextCursor = %d, want 3", next)
		}
		if got.elapsed < 200*time.Millisecond {
			t.Fatalf("poll returned after %v: the compaction woke it", got.elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poll did not return")
	}

	// A fully trimmed document is still known: a poll inside the trimmed
	// range parks, times out and reports the boundary as the next cursor.
	postDocChanges(t, h, "full", "dev-1", 2)
	w, _ = postJSON(t, h, "/v1/documents/full/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "full", "dev-1"); w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("full compact = %d %q", w.Code, w.Body.String())
	}
	_, body := doRequest(t, h, http.MethodGet, "/v1/documents/full/changes/poll?after=1&waitMs=50")
	if body["timedOut"] != true {
		t.Fatalf("full poll timedOut = %v", body)
	}
	if next := int64(body["nextCursor"].(float64)); next != 2 {
		t.Fatalf("full poll nextCursor = %d, want the boundary 2", next)
	}
	if changes := body["changes"].([]any); len(changes) != 0 {
		t.Fatalf("full poll changes = %v", changes)
	}

	// A subscription starting inside the trimmed range backfills from the
	// first online change after the boundary, then continues live.
	srv := httptest.NewServer(h)
	defer srv.Close()
	conn, resp := dialWS(t, subscribeURL(srv, "sess", "doc", "0"))
	if conn == nil {
		t.Fatalf("subscribe status = %d", resp.StatusCode)
	}
	defer conn.close()
	if c := conn.readChange(); c.ID != "c3" || c.Cursor != 3 {
		t.Fatalf("backfill frame = %+v, want c3 at cursor 3", c)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if c := conn.readChange(); c.ID != "c4" || c.Cursor != 4 {
		t.Fatalf("live frame = %+v, want c4 at cursor 4", c)
	}
}

func TestChangesCompactRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "sync.db")

	st, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(st)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{"cursor": 2, "state": nil})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":2,\"removed\":2}\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := app.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st2.Close() }()
	h2 := NewHandler(st2)

	// The boundary, the reads and the idempotency decisions survive the
	// restart.
	if w := compactChanges(t, h2, "doc", "dev-1"); w.Code != http.StatusOK || w.Body.String() != "{\"boundary\":2,\"removed\":0}\n" {
		t.Fatalf("re-compact after restart = %d %q", w.Code, w.Body.String())
	}
	_, body := doRequest(t, h2, http.MethodGet, "/v1/documents/doc/changes?after=0")
	rows := body["changes"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["id"] != "c3" {
		t.Fatalf("changes after restart = %v", rows)
	}
	w, body = postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c1", "payload": map[string]any{"n": 1}}},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("trimmed replay after restart = %d %s", w.Code, w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != false || int64(result["cursor"].(float64)) != 1 {
		t.Fatalf("trimmed replay result after restart = %v", result)
	}

	// New changes continue the pre-compaction cursor space: no reuse, no gap.
	w, body = postJSON(t, h2, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	result = body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 4 {
		t.Fatalf("new cursor after restart = %v, want 4", result)
	}
}
