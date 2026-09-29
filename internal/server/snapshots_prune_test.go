package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// putSnapshot posts one snapshot and fails the test unless it is accepted.
func putSnapshot(t *testing.T, h http.Handler, doc string, cursor int, state any) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots", map[string]any{"cursor": cursor, "state": state})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot at %d: %d %s", cursor, w.Code, w.Body.String())
	}
}

// pruneSnapshots posts the retention prune and returns the raw recorder so
// tests can assert the exact single-line body.
func pruneSnapshots(t *testing.T, h http.Handler, doc, device string, keep any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots/prune", map[string]any{"deviceId": device, "keep": keep})
	return w
}

// snapshotCursors returns the snapshot cursors an export lists for a doc.
func snapshotCursors(t *testing.T, h http.Handler, doc string) []int64 {
	t.Helper()
	w := getRaw(t, h, "/v1/documents/"+doc+"/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("export snapshots: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Snapshots []struct {
			Cursor int64 `json:"cursor"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	out := make([]int64, len(body.Snapshots))
	for i, sn := range body.Snapshots {
		out[i] = sn.Cursor
	}
	return out
}

// Keeping only the n newest snapshots deletes every older cursor and reports
// the greatest surviving cursor together with the number deleted; deleted
// cursors read 404, surviving ones keep reading and exporting.
func TestSnapshotPruneKeepsNewest(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 5)
	for _, c := range []int{1, 2, 3, 4, 5} {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}

	w := pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK {
		t.Fatalf("prune = %d %s", w.Code, w.Body.String())
	}
	if got := w.Body.String(); got != `{"maxCursor":5,"removed":3}`+"\n" {
		t.Fatalf("prune body = %q", got)
	}

	if got := snapshotCursors(t, h, "doc"); len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("surviving snapshots = %v, want [4 5]", got)
	}

	// Deleted cursors miss in the single read; surviving cursors still read.
	for _, c := range []int{1, 2, 3} {
		if w := getRaw(t, h, fmt.Sprintf("/v1/documents/doc/snapshots/%d", c)); w.Code != http.StatusNotFound {
			t.Fatalf("snapshot %d after prune = %d, want 404", c, w.Code)
		}
	}
	for _, c := range []int{4, 5} {
		if w := getRaw(t, h, fmt.Sprintf("/v1/documents/doc/snapshots/%d", c)); w.Code != http.StatusOK {
			t.Fatalf("snapshot %d after prune = %d %s, want 200", c, w.Code, w.Body.String())
		}
	}

	// An interval export spanning the deleted range only returns survivors.
	w = getRaw(t, h, "/v1/documents/doc/snapshots?from=0&to=5")
	if w.Code != http.StatusOK || strings.Count(w.Body.String(), `"cursor"`) != 2 {
		t.Fatalf("range export = %d %s", w.Code, w.Body.String())
	}
}

// A document with no more snapshots than keep deletes nothing; a snapshot-less
// or unknown document succeeds with both numbers zero and creates nothing.
func TestSnapshotPruneNoopAndUnknown(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)
	putSnapshot(t, h, "doc", 1, nil)
	putSnapshot(t, h, "doc", 2, nil)

	// More retention than snapshots: nothing removed, maximum cursor reported.
	w := pruneSnapshots(t, h, "doc", "dev-1", 10)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":2,"removed":0}`+"\n" {
		t.Fatalf("oversized keep = %d %q", w.Code, w.Body.String())
	}
	w = pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":2,"removed":0}`+"\n" {
		t.Fatalf("equal keep = %d %q", w.Code, w.Body.String())
	}

	// An unknown document: success, both zero, no snapshot row created.
	w = pruneSnapshots(t, h, "ghost", "dev-1", 3)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":0,"removed":0}`+"\n" {
		t.Fatalf("unknown doc prune = %d %q", w.Code, w.Body.String())
	}
	if got := snapshotCursors(t, h, "ghost"); len(got) != 0 {
		t.Fatalf("unknown doc snapshots = %v, want none", got)
	}

	// A known document without snapshots answers identically.
	postDocChanges(t, h, "nosnap", "dev-1", 1)
	w = pruneSnapshots(t, h, "nosnap", "dev-1", 1)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":0,"removed":0}`+"\n" {
		t.Fatalf("snapshotless prune = %d %q", w.Code, w.Body.String())
	}
}

// Repeating a prune is idempotent: the second call removes nothing and reports
// the same maximum cursor.
func TestSnapshotPruneIdempotent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	for _, c := range []int{1, 2, 3, 4} {
		putSnapshot(t, h, "doc", c, nil)
	}
	if w := pruneSnapshots(t, h, "doc", "dev-1", 2); w.Body.String() != `{"maxCursor":4,"removed":2}`+"\n" {
		t.Fatalf("first prune = %q", w.Body.String())
	}
	for i := 0; i < 3; i++ {
		w := pruneSnapshots(t, h, "doc", "dev-1", 2)
		if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":4,"removed":0}`+"\n" {
			t.Fatalf("repeat prune %d = %d %q", i, w.Code, w.Body.String())
		}
	}
	if got := snapshotCursors(t, h, "doc"); len(got) != 2 {
		t.Fatalf("snapshots after repeats = %v", got)
	}
}

// All request-shape failures are 400 JSON with zero writes.
func TestSnapshotPruneShapeValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 1)
	putSnapshot(t, h, "doc", 1, nil)

	t.Run("content type", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/documents/doc/snapshots/prune", strings.NewReader(`{"deviceId":"dev-1","keep":1}`))
		r.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d %s", w.Code, w.Body.String())
		}
	})
	t.Run("invalid JSON", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/prune", `{"deviceId":"dev-1",`)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", w.Code)
		}
	})
	t.Run("trailing content", func(t *testing.T) {
		w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/prune", `{"deviceId":"dev-1","keep":1} x`)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d", w.Code)
		}
	})

	// deviceId is part of the request shape and fails before the gate.
	for _, tc := range []struct {
		name string
		body any
	}{
		{"missing deviceId", map[string]any{"keep": 1}},
		{"empty deviceId", map[string]any{"deviceId": "", "keep": 1}},
		{"wrong type deviceId", map[string]any{"deviceId": 7, "keep": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/prune", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d %s", w.Code, w.Body.String())
			}
		})
	}

	// keep legality is checked after the gate: every invalid value is a 400
	// for this registered, authorized device. Bodies are sent verbatim so
	// literals like 2.0 survive to the parser.
	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing", `{"deviceId":"dev-1"}`},
		{"null", `{"deviceId":"dev-1","keep":null}`},
		{"zero", `{"deviceId":"dev-1","keep":0}`},
		{"negative", `{"deviceId":"dev-1","keep":-1}`},
		{"fraction", `{"deviceId":"dev-1","keep":1.5}`},
		{"integer-looking float", `{"deviceId":"dev-1","keep":2.0}`},
		{"exponent", `{"deviceId":"dev-1","keep":1e2}`},
		{"string", `{"deviceId":"dev-1","keep":"2"}`},
		{"boolean", `{"deviceId":"dev-1","keep":true}`},
		{"array", `{"deviceId":"dev-1","keep":[1]}`},
		{"object", `{"deviceId":"dev-1","keep":{"n":1}}`},
		{"too large", `{"deviceId":"dev-1","keep":1001}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/prune", tc.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("keep %s = %d %s, want 400", tc.name, w.Code, w.Body.String())
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("content type = %q", ct)
			}
		})
	}

	// The boundary values are accepted and delete nothing here (one snapshot).
	for _, keep := range []int{1, 1000} {
		w := pruneSnapshots(t, h, "doc", "dev-1", keep)
		if w.Code != http.StatusOK {
			t.Fatalf("keep %d = %d %s", keep, w.Code, w.Body.String())
		}
	}

	// Nothing a failed prune touched removed the snapshot.
	if got := snapshotCursors(t, h, "doc"); len(got) != 1 || got[0] != 1 {
		t.Fatalf("snapshot after failures = %v", got)
	}
}

// The verdict order is shape (400), device existence (404), permission (403),
// then parameter legality (400): earlier failures win even with an invalid
// keep.
func TestSnapshotPruneVerdictOrder(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 1)
	w, _ := postJSON(t, h, "/v1/documents/doc2/permissions", map[string]any{"deviceId": "dev-1", "action": "revoke"})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	// An unregistered device is 404 even with an invalid keep.
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/prune", map[string]any{"deviceId": "ghost", "keep": 0})
	if w.Code != http.StatusNotFound {
		t.Fatalf("unregistered with bad keep = %d %s, want 404", w.Code, w.Body.String())
	}
	// A revoked device is 403 even with an invalid keep.
	w, _ = postJSON(t, h, "/v1/documents/doc2/snapshots/prune", map[string]any{"deviceId": "dev-1", "keep": "bad"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("revoked with bad keep = %d %s, want 403", w.Code, w.Body.String())
	}
	// A registered, authorized device finally surfaces the invalid keep as 400.
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/prune", map[string]any{"deviceId": "dev-1", "keep": 0})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid keep = %d %s, want 400", w.Code, w.Body.String())
	}
}

// Path and method failures are 400 JSON, never a redirect or HTML.
func TestSnapshotPrunePathAndMethod(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	body := `{"deviceId":"dev-1","keep":1}`
	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{"empty document id", http.MethodPost, "/v1/documents//snapshots/prune"},
		{"trailing slash", http.MethodPost, "/v1/documents/doc/snapshots/prune/"},
		{"extra segment", http.MethodPost, "/v1/documents/doc/snapshots/prune/x"},
		{"missing snapshots segment", http.MethodPost, "/v1/documents/doc/prune"},
		{"wrong method GET", http.MethodGet, "/v1/documents/doc/snapshots/prune"},
		{"wrong method DELETE", http.MethodDelete, "/v1/documents/doc/snapshots/prune"},
		{"wrong method PUT", http.MethodPut, "/v1/documents/doc/snapshots/prune"},
		{"collection wrong method PATCH", http.MethodPatch, "/v1/documents/doc/snapshots/prune"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(body))
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

	// GET on the prune word is not treated as a single-snapshot cursor read.
	w := getRaw(t, h, "/v1/documents/doc/snapshots/prune")
	if w.Code != http.StatusBadRequest {
		t.Fatalf("GET prune = %d, want 400", w.Code)
	}
}

// A named version keeps mapping its name to the cursor; once the snapshot that
// cursor names is pruned, the by-name read and by-name restore answer 404
// while the marker itself still lists.
func TestSnapshotPruneDanglingVersion(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 3)
	putSnapshot(t, h, "doc", 1, map[string]any{"v": 1})
	putSnapshot(t, h, "doc", 2, map[string]any{"v": 2})
	putSnapshot(t, h, "doc", 3, map[string]any{"v": 3})

	// v-old points at cursor 1, v-new at cursor 3.
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v-old", "snapshotCursor": 1,
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v-new", "snapshotCursor": 3,
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	if w := pruneSnapshots(t, h, "doc", "dev-1", 1); w.Body.String() != `{"maxCursor":3,"removed":2}`+"\n" {
		t.Fatalf("prune = %q", w.Body.String())
	}

	// Both markers still list with their cursor bindings.
	w = getRaw(t, h, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"name":"v-old","snapshotCursor":1`) {
		t.Fatalf("versions list = %d %s", w.Code, w.Body.String())
	}

	// The dangling name reads 404 and restores 404 with zero writes; the
	// surviving name still reads.
	if w := getRaw(t, h, "/v1/documents/doc/snapshots/versions/v-old?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("by-name read dangling = %d %s, want 404", w.Code, w.Body.String())
	}
	if w := getRaw(t, h, "/v1/documents/doc/snapshots/versions/v-new?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("by-name read surviving = %d %s", w.Code, w.Body.String())
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/versions/v-old/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-old",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("by-name restore dangling = %d %s, want 404", w.Code, w.Body.String())
	}
}

// An accepted restore keeps answering idempotently (and keeps its conflict
// verdict) after a prune deletes the snapshot it came from; a fresh restore of
// that pruned cursor is a 404. The compaction boundary and the cursor space
// move on from the surviving data alone.
func TestSnapshotPrunePreservesRestoreRecordAndBoundary(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 2)
	putSnapshot(t, h, "doc", 1, map[string]any{"from": "snap-1"})
	putSnapshot(t, h, "doc", 2, map[string]any{"from": "snap-2"})

	// Restore snapshot 2 as change r-2 at cursor 3.
	w, body := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-2", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != true || int64(body["cursor"].(float64)) != 3 {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}

	// Keep only snapshot 2; snapshot 1 is pruned.
	if w := pruneSnapshots(t, h, "doc", "dev-1", 1); w.Body.String() != `{"maxCursor":2,"removed":1}`+"\n" {
		t.Fatalf("first prune = %q", w.Body.String())
	}

	// The accepted restore at cursor 2 is still idempotent even though the
	// snapshot row still exists here; then prune snapshot 2 as well by adding
	// a newer snapshot and pruning again.
	putSnapshot(t, h, "doc", 3, map[string]any{"from": "snap-3"})
	if w := pruneSnapshots(t, h, "doc", "dev-1", 1); w.Body.String() != `{"maxCursor":3,"removed":1}`+"\n" {
		t.Fatalf("second prune = %q", w.Body.String())
	}
	if got := snapshotCursors(t, h, "doc"); len(got) != 1 || got[0] != 3 {
		t.Fatalf("survivors = %v, want [3]", got)
	}

	// Repeating the accepted restore is idempotent from its recorded
	// provenance despite the pruned snapshot: created=false, first cursor.
	w, body = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-2", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("idempotent restore after prune = %d %s", w.Code, w.Body.String())
	}
	if body["created"] != false || int64(body["cursor"].(float64)) != 3 || int64(body["restoredFrom"].(float64)) != 2 {
		t.Fatalf("idempotent restore body = %v", body)
	}
	// The same id with a different source cursor is a conflict, not a 404.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-2", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("restore source mismatch = %d %s, want 409", w.Code, w.Body.String())
	}

	// A fresh restore of a pruned cursor is a 404 and writes nothing.
	before := snapshotCursors(t, h, "doc")
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-fresh", "snapshotCursor": 2,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("fresh restore of pruned snapshot = %d %s, want 404", w.Code, w.Body.String())
	}
	if got := snapshotCursors(t, h, "doc"); len(got) != len(before) {
		t.Fatalf("failed restore changed snapshots: %v", got)
	}
	// An id held by an ordinary online change also misses rather than
	// conflicting: the snapshot miss precedes the id conflict as before prune.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "c2", "snapshotCursor": 2,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("ordinary change id restore = %d %s, want 404", w.Code, w.Body.String())
	}

	// The compaction boundary follows the surviving snapshots only (3).
	w = compactChanges(t, h, "doc", "dev-1")
	if w.Code != http.StatusOK || w.Body.String() != `{"boundary":3,"removed":3}`+"\n" {
		t.Fatalf("compact after prune = %d %q", w.Code, w.Body.String())
	}

	// The cursor space is untouched: the next change takes cursor 4.
	w, body = postJSON(t, h, "/v1/documents/doc/changes", map[string]any{
		"deviceId": "dev-1",
		"changes":  []any{map[string]any{"id": "c4", "payload": map[string]any{"n": 4}}},
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	result := body["results"].([]any)[0].(map[string]any)
	if result["created"] != true || int64(result["cursor"].(float64)) != 4 {
		t.Fatalf("next cursor = %v, want 4", result)
	}
}

// Pruning and concurrent snapshot creation commit whole: when the dust
// settles every stored snapshot is a created one, exactly the newest keep
// survive, and a deleted cursor never reappears.
func TestSnapshotPruneConcurrentWithCreation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 40)
	for c := 1; c <= 20; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}

	var wg sync.WaitGroup
	for c := 21; c <= 40; c++ {
		wg.Add(1)
		go func(cursor int) {
			defer wg.Done()
			// Report (not FailNow) from this worker goroutine; the settled set
			// assertions below catch any missed creation.
			w, _ := postJSON(t, h, "/v1/documents/doc/snapshots", map[string]any{
				"cursor": cursor, "state": map[string]any{"s": cursor},
			})
			if w.Code != http.StatusOK {
				t.Errorf("create snapshot %d = %d %s", cursor, w.Code, w.Body.String())
			}
		}(c)
	}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := pruneSnapshots(t, h, "doc", "dev-1", 5); w.Code != http.StatusOK {
				t.Errorf("concurrent prune = %d %s", w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()

	// Final prune settles the retention: exactly the five newest snapshots
	// survive, and every surviving cursor reads with the stored state. The
	// removed count depends on how the interleaved transactions committed, so
	// only the maximum cursor and the settled set are asserted.
	w := pruneSnapshots(t, h, "doc", "dev-1", 5)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":40,"removed":0}`+"\n" {
		// One more settling prune if a concurrent creation committed after the
		// last concurrent prune; the second call must then be the zero-removal
		// fixed point.
		if w2 := pruneSnapshots(t, h, "doc", "dev-1", 5); w2.Body.String() != `{"maxCursor":40,"removed":0}`+"\n" {
			t.Fatalf("settling prune = %q then %q", w.Body.String(), w2.Body.String())
		}
	}
	got := snapshotCursors(t, h, "doc")
	if len(got) != 5 {
		t.Fatalf("survivors = %v, want five", got)
	}
	for i, c := range []int64{36, 37, 38, 39, 40} {
		if got[i] != c {
			t.Fatalf("survivor %d = %d, want %d", i, got[i], c)
		}
	}
}

// The prune result and the deleted-cursor reads survive a restart verbatim.
func TestSnapshotPruneRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (http.Handler, *app.App) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return NewHandler(s), s
	}

	h, s := open(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	for c := 1; c <= 4; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}
	if w := pruneSnapshots(t, h, "doc", "dev-1", 2); w.Body.String() != `{"maxCursor":4,"removed":2}`+"\n" {
		t.Fatalf("prune = %q", w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	h2, s2 := open(t)
	defer func() { _ = s2.Close() }()

	// The repeat prune is idempotent with the same boundary numbers.
	w := pruneSnapshots(t, h2, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":4,"removed":0}`+"\n" {
		t.Fatalf("prune after restart = %d %q", w.Code, w.Body.String())
	}
	// Deleted cursors stay deleted; survivors stay readable.
	if w := getRaw(t, h2, "/v1/documents/doc/snapshots/2"); w.Code != http.StatusNotFound {
		t.Fatalf("deleted snapshot after restart = %d, want 404", w.Code)
	}
	if w := getRaw(t, h2, "/v1/documents/doc/snapshots/4"); w.Code != http.StatusOK {
		t.Fatalf("surviving snapshot after restart = %d", w.Code)
	}
}
