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

// pruneSnapshots posts the snapshot-prune request and returns the raw
// recorder so tests can assert the exact single-line body.
func pruneSnapshots(t *testing.T, h http.Handler, doc, device string, keep any) *httptest.ResponseRecorder {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots/prune", map[string]any{
		"deviceId": device,
		"keep":     keep,
	})
	return w
}

// putSnapshot creates one snapshot of doc at cursor with the given state,
// failing the test on error.
func putSnapshot(t *testing.T, h http.Handler, doc string, cursor int, state any) {
	t.Helper()
	w, _ := postJSON(t, h, "/v1/documents/"+doc+"/snapshots", map[string]any{
		"cursor": cursor,
		"state":  state,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("snapshot at %d: %d %s", cursor, w.Code, w.Body.String())
	}
}

// snapshotCursors returns the snapshot cursors currently exported for doc in
// ascending order.
func snapshotCursors(t *testing.T, h http.Handler, doc string) []any {
	t.Helper()
	w, body := doRequest(t, h, http.MethodGet, "/v1/documents/"+doc+"/snapshots")
	if w.Code != http.StatusOK {
		t.Fatalf("export snapshots: %d %s", w.Code, w.Body.String())
	}
	rows := body["snapshots"].([]any)
	cursors := make([]any, 0, len(rows))
	for _, row := range rows {
		cursors = append(cursors, row.(map[string]any)["cursor"])
	}
	return cursors
}

func TestSnapshotPruneRetention(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 5)
	for c := 1; c <= 5; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}

	// Keep the two newest snapshots: cursors 4 and 5 survive, the other three
	// are deleted, and the answer names cursor 5 as the maximum.
	w := pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":5,"deleted":3}`+"\n" {
		t.Fatalf("prune = %d %q", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}

	if got := snapshotCursors(t, h, "doc"); len(got) != 2 ||
		int64(got[0].(float64)) != 4 || int64(got[1].(float64)) != 5 {
		t.Fatalf("surviving snapshots = %v, want cursors 4 and 5", got)
	}

	// The pruned cursors are gone from the single read and any interval
	// export, and a restore of one is a 404 with zero writes.
	for _, c := range []int{1, 2, 3} {
		if w := getRaw(t, h, fmt.Sprintf("/v1/documents/doc/snapshots/%d", c)); w.Code != http.StatusNotFound {
			t.Fatalf("read pruned snapshot %d = %d, want 404", c, w.Code)
		}
		w, _ := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
			"deviceId": "dev-1", "changeId": fmt.Sprintf("r-new-%d", c), "snapshotCursor": c,
		})
		if w.Code != http.StatusNotFound {
			t.Fatalf("restore pruned snapshot %d = %d, want 404", c, w.Code)
		}
	}
	for _, c := range []int{4, 5} {
		if w := getRaw(t, h, fmt.Sprintf("/v1/documents/doc/snapshots/%d", c)); w.Code != http.StatusOK {
			t.Fatalf("read surviving snapshot %d = %d, want 200", c, w.Code)
		}
	}

	// A repeat prune is idempotent: same maximum, nothing deleted.
	w = pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":5,"deleted":0}`+"\n" {
		t.Fatalf("repeat prune = %d %q", w.Code, w.Body.String())
	}
}

func TestSnapshotPruneGapsAndBoundaries(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 6)

	// Snapshot cursors with gaps: the keep-th highest cursor is the cutoff and
	// only strictly older cursors are deleted, regardless of the gaps.
	for _, c := range []int{1, 3, 5} {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}
	w := pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":5,"deleted":1}`+"\n" {
		t.Fatalf("prune with gaps = %d %q", w.Code, w.Body.String())
	}
	if got := snapshotCursors(t, h, "doc"); len(got) != 2 ||
		int64(got[0].(float64)) != 3 || int64(got[1].(float64)) != 5 {
		t.Fatalf("surviving snapshots = %v, want cursors 3 and 5", got)
	}

	// Keeping at least as many snapshots as exist deletes nothing; the largest
	// accepted count is 1000.
	w = pruneSnapshots(t, h, "doc", "dev-1", 2)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":5,"deleted":0}`+"\n" {
		t.Fatalf("no-op prune = %d %q", w.Code, w.Body.String())
	}
	w = pruneSnapshots(t, h, "doc", "dev-1", 1000)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":5,"deleted":0}`+"\n" {
		t.Fatalf("keep 1000 = %d %q", w.Code, w.Body.String())
	}

	// Keeping one snapshot leaves only the highest cursor.
	w = pruneSnapshots(t, h, "doc2", "dev-1", 1)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":0,"deleted":0}`+"\n" {
		t.Fatalf("unknown document prune = %d %q", w.Code, w.Body.String())
	}
	postDocChanges(t, h, "doc2", "dev-1", 6)
	for _, c := range []int{2, 4, 6} {
		putSnapshot(t, h, "doc2", c, nil)
	}
	w = pruneSnapshots(t, h, "doc2", "dev-1", 1)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":6,"deleted":2}`+"\n" {
		t.Fatalf("keep one = %d %q", w.Code, w.Body.String())
	}
	if got := snapshotCursors(t, h, "doc2"); len(got) != 1 || int64(got[0].(float64)) != 6 {
		t.Fatalf("surviving snapshots = %v, want only cursor 6", got)
	}
	// A repeat keep-one prune is idempotent: the only survivor stays and
	// nothing else is deleted.
	w = pruneSnapshots(t, h, "doc2", "dev-1", 1)
	if w.Code != http.StatusOK || w.Body.String() != `{"maxCursor":6,"deleted":0}`+"\n" {
		t.Fatalf("second keep-one = %d %q", w.Code, w.Body.String())
	}
}

func TestSnapshotPruneValidation(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc", "dev-1", 3)
	for c := 1; c <= 3; c++ {
		putSnapshot(t, h, "doc", c, nil)
	}
	// dev-1 is revoked on doc-revoked.
	if w, _ := postJSON(t, h, "/v1/documents/doc-revoked/permissions", map[string]any{
		"deviceId": "dev-1", "action": "revoke",
	}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}

	t.Run("request shape", func(t *testing.T) {
		// Wrong content type.
		r := newPruneRequest(t, http.MethodPost, "/v1/documents/doc/snapshots/prune", `{"deviceId":"dev-1","keep":1}`)
		r.Header.Set("Content-Type", "text/plain")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("content type = %d %s", w.Code, w.Body.String())
		}
		// Malformed JSON and trailing content.
		for _, body := range []string{`{"deviceId":"dev-1","keep":`, `{"deviceId":"dev-1","keep":1} {}`} {
			w := pruneRaw(t, h, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("body %q = %d", body, w.Code)
			}
		}
		// A missing/empty/wrong-type deviceId is a shape error even when keep
		// is also missing.
		for _, body := range []string{
			`{"keep":1}`,
			`{"deviceId":"","keep":1}`,
			`{"deviceId":7,"keep":1}`,
			`{}`,
		} {
			if w := pruneRaw(t, h, body); w.Code != http.StatusBadRequest {
				t.Fatalf("body %q = %d %s", body, w.Code, w.Body.String())
			}
		}
	})

	// keep legality is checked after the gate: an unregistered device wins
	// over an illegal keep with a 404, a revoked one with a 403.
	for _, keep := range []string{`0`, `-1`, `1001`, `1.5`, `"2"`, `true`, `null`, `1e3`} {
		w := pruneRaw(t, h, `{"deviceId":"ghost","keep":`+keep+`}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("unregistered with keep %s = %d, want 404", keep, w.Code)
		}
		// dev-1 is authorized on doc but revoked on doc-revoked: route the
		// illegal-keep probe at the revoked document to assert 403 precedence.
		w = pruneRawPath(t, h, "/v1/documents/doc-revoked/snapshots/prune", `{"deviceId":"dev-1","keep":`+keep+`}`)
		if w.Code != http.StatusForbidden {
			t.Fatalf("revoked with keep %s = %d %s, want 403", keep, w.Code, w.Body.String())
		}
		// And on an authorized document the illegal keep itself is a 400.
		w = pruneRaw(t, h, `{"deviceId":"dev-2","keep":`+keep+`}`)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("illegal keep %s = %d %s, want 400", keep, w.Code, w.Body.String())
		}
	}
	// A missing keep field is likewise a parameter error after the gate.
	if w := pruneRaw(t, h, `{"deviceId":"dev-1"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("missing keep = %d, want 400", w.Code)
	}

	// None of the failed calls deleted anything: all three snapshots remain.
	if got := snapshotCursors(t, h, "doc"); len(got) != 3 {
		t.Fatalf("snapshots after failures = %v, want all three", got)
	}
}

func TestSnapshotPrunePathsAndMethods(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	body := `{"deviceId":"dev-1","keep":1}`
	for _, tc := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/documents//snapshots/prune"},
		{http.MethodPost, "/v1/documents/doc/snapshots/prune/"},
		{http.MethodPost, "/v1/documents/doc/snapshots/prune/x"},
		{http.MethodPost, "/v1/documents/doc/prune"},
		{http.MethodGet, "/v1/documents/doc/snapshots/prune"},
		{http.MethodPut, "/v1/documents/doc/snapshots/prune"},
		{http.MethodDelete, "/v1/documents/doc/snapshots/prune"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			r := newPruneRequest(t, tc.method, tc.path, body)
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
}

func TestSnapshotPruneNamedVersions(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	for c := 1; c <= 4; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}
	// Bind a name to cursor 2 and perform a named restore while that snapshot
	// still exists, so its provenance is recorded; then prune cursors 1 and 2
	// away, keeping 3 and 4.
	w, _ := postJSON(t, h, "/v1/documents/doc/snapshots/versions", map[string]any{
		"deviceId": "dev-1", "name": "v-old", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w, body := postJSON(t, h, "/v1/documents/doc/snapshots/versions/v-old/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-name-done",
	})
	if w.Code != http.StatusOK {
		t.Fatalf("seed named restore = %d %s", w.Code, w.Body.String())
	}
	firstCursor := int64(body["cursor"].(float64))
	if w := pruneSnapshots(t, h, "doc", "dev-1", 2); w.Code != http.StatusOK ||
		w.Body.String() != `{"maxCursor":4,"deleted":2}`+"\n" {
		t.Fatalf("prune = %d %q", w.Code, w.Body.String())
	}

	// The marker survives and still names cursor 2 in the listing.
	w, body = doRequest(t, h, http.MethodGet, "/v1/documents/doc/snapshots/versions?deviceId=dev-1")
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	versions := body["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("versions = %v", versions)
	}
	v := versions[0].(map[string]any)
	if v["name"] != "v-old" || int64(v["snapshotCursor"].(float64)) != 2 {
		t.Fatalf("version marker = %v", v)
	}

	// Reading by the dangling name, or restoring it with a fresh id, now
	// misses, with zero writes.
	if w := getRaw(t, h, "/v1/documents/doc/snapshots/versions/v-old?deviceId=dev-1"); w.Code != http.StatusNotFound {
		t.Fatalf("read by dangling name = %d, want 404", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/snapshots/versions/v-old/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-name",
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("fresh named restore = %d %s, want 404", w.Code, w.Body.String())
	}

	// The restore that already happened by that name keeps its idempotent
	// verdict from the recorded provenance after the snapshot is pruned.
	w, body = postJSON(t, h, "/v1/documents/doc/snapshots/versions/v-old/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r-name-done",
	})
	if w.Code != http.StatusOK || body["created"] != false ||
		int64(body["cursor"].(float64)) != firstCursor {
		t.Fatalf("idempotent named restore after prune = %d %v %s", w.Code, body, w.Body.String())
	}

	// The name can be rebound to a surviving snapshot and read again.
	putBody, _ := json.Marshal(map[string]any{"deviceId": "dev-1", "snapshotCursor": 4})
	r := newPruneRequest(t, http.MethodPut, "/v1/documents/doc/snapshots/versions/v-old", string(putBody))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusOK || rec.Body.String() != `{"name":"v-old","snapshotCursor":4}`+"\n" {
		t.Fatalf("rebind = %d %q", rec.Code, rec.Body.String())
	}
	if w := getRaw(t, h, "/v1/documents/doc/snapshots/versions/v-old?deviceId=dev-1"); w.Code != http.StatusOK {
		t.Fatalf("read by rebound name = %d, want 200", w.Code)
	}
}

func TestSnapshotPruneRestoreProvenance(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc", "dev-1", 3)
	for c := 1; c <= 3; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}

	// Restore snapshot 2 as r1 (cursor 4), snapshot the result, then compact
	// so the restore change is trimmed into a retained summary.
	w, body := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != true || int64(body["cursor"].(float64)) != 4 {
		t.Fatalf("restore = %d %s", w.Code, w.Body.String())
	}
	putSnapshot(t, h, "doc", 4, nil)
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK ||
		w.Body.String() != `{"boundary":4,"removed":4}`+"\n" {
		t.Fatalf("compact = %d %q", w.Code, w.Body.String())
	}

	// Prune the source snapshot (and another): the stored compaction boundary
	// is untouched.
	if w := pruneSnapshots(t, h, "doc", "dev-1", 2); w.Code != http.StatusOK ||
		w.Body.String() != `{"maxCursor":4,"deleted":2}`+"\n" {
		t.Fatalf("prune = %d %q", w.Code, w.Body.String())
	}
	if w := compactChanges(t, h, "doc", "dev-1"); w.Code != http.StatusOK ||
		w.Body.String() != `{"boundary":4,"removed":0}`+"\n" {
		t.Fatalf("compact after prune = %d %q", w.Code, w.Body.String())
	}

	// Repeating the recorded restore stays idempotent from its retained
	// provenance even though snapshot 2 is gone: created=false, first cursor.
	w, body = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != false ||
		int64(body["cursor"].(float64)) != 4 || int64(body["restoredFrom"].(float64)) != 2 {
		t.Fatalf("idempotent restore after prune = %d %v %s", w.Code, body, w.Body.String())
	}
	// A different source device or a different snapshot cursor is a 409.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-2", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("device mismatch = %d, want 409", w.Code)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 1,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("snapshot cursor mismatch = %d, want 409", w.Code)
	}
	// A genuinely new id restoring a pruned cursor still misses with 404 and
	// zero writes.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r2", "snapshotCursor": 2,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("new restore of pruned snapshot = %d, want 404", w.Code)
	}
	// An ordinary trimmed change id keeps the snapshot-miss precedence: the
	// pruned cursor is a 404, not the id conflict.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "c1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("ordinary trimmed id at pruned cursor = %d, want 404", w.Code)
	}
}

func TestSnapshotPruneOnlineRestoreProvenance(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	registerDevice(t, h, "dev-2")
	postDocChanges(t, h, "doc", "dev-1", 3)
	for c := 1; c <= 3; c++ {
		putSnapshot(t, h, "doc", c, map[string]any{"s": c})
	}
	// An online (uncompacted) restore from snapshot 2, then prune snapshot 2
	// away (keep only the newest snapshot).
	w, body := postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != true {
		t.Fatalf("restore = %s", w.Body.String())
	}
	if w := pruneSnapshots(t, h, "doc", "dev-1", 1); w.Code != http.StatusOK ||
		w.Body.String() != `{"maxCursor":3,"deleted":2}`+"\n" {
		t.Fatalf("prune = %d %q", w.Code, w.Body.String())
	}

	// The recorded restore is still idempotent on repeat, and mismatches still
	// conflict.
	w, body = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusOK || body["created"] != false || int64(body["cursor"].(float64)) != 4 {
		t.Fatalf("online idempotent restore = %d %v", w.Code, body)
	}
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-2", "changeId": "r1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("online device mismatch = %d, want 409", w.Code)
	}
	// An ordinary online change id at the pruned cursor keeps the miss
	// precedence: 404 rather than an id conflict.
	w, _ = postJSON(t, h, "/v1/documents/doc/restore", map[string]any{
		"deviceId": "dev-1", "changeId": "c1", "snapshotCursor": 2,
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("ordinary id at pruned cursor = %d, want 404", w.Code)
	}
}

func TestSnapshotPruneConcurrent(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 6)
	for c := 1; c <= 6; c++ {
		putSnapshot(t, h, "doc", c, nil)
	}

	// Concurrent identical prunes serialize: exactly one call removes the four
	// oldest snapshots and every other call removes nothing; the final set is
	// exactly the two highest cursors with no half result.
	const goroutines = 24
	var wg sync.WaitGroup
	var mu sync.Mutex
	var deletedTotal int64
	nonZero := 0
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			w := pruneSnapshots(t, h, "doc", "dev-1", 2)
			if w.Code != http.StatusOK {
				t.Errorf("prune = %d %s", w.Code, w.Body.String())
				return
			}
			var resp struct {
				MaxCursor int64 `json:"maxCursor"`
				Deleted   int64 `json:"deleted"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			deletedTotal += resp.Deleted
			if resp.Deleted > 0 {
				nonZero++
				if resp.MaxCursor != 6 || resp.Deleted != 4 {
					t.Errorf("deleting prune = %+v, want maxCursor 6 deleted 4", resp)
				}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if deletedTotal != 4 || nonZero != 1 {
		t.Fatalf("deleted total = %d across %d deleting calls, want 4 across 1", deletedTotal, nonZero)
	}
	if got := snapshotCursors(t, h, "doc"); len(got) != 2 ||
		int64(got[0].(float64)) != 5 || int64(got[1].(float64)) != 6 {
		t.Fatalf("surviving snapshots = %v, want cursors 5 and 6", got)
	}
}

func TestSnapshotPruneConcurrentWithCreate(t *testing.T) {
	h, _ := newTestHandler(t)
	registerDevice(t, h, "dev-1")
	// Enough changes for the extra snapshots the create side posts.
	postDocChanges(t, h, "doc", "dev-1", 12)
	for c := 1; c <= 6; c++ {
		putSnapshot(t, h, "doc", c, nil)
	}

	// A prune and snapshot creations for higher cursors race: every statement
	// is one serialized transaction, so each operation is wholly present or
	// absent. Afterwards the surviving snapshots are exactly the keep=3
	// highest among the created set and no deleted cursor reappears.
	const creators = 6
	var wg sync.WaitGroup
	wg.Add(1 + creators)
	go func() {
		defer wg.Done()
		if w := pruneSnapshots(t, h, "doc", "dev-1", 3); w.Code != http.StatusOK {
			t.Errorf("prune = %d %s", w.Code, w.Body.String())
		}
	}()
	for c := 7; c <= 12; c++ {
		c := c
		go func() {
			defer wg.Done()
			putSnapshot(t, h, "doc", c, nil)
		}()
	}
	wg.Wait()

	got := snapshotCursors(t, h, "doc")
	if len(got) < 3 {
		t.Fatalf("surviving snapshots = %v, want at least the retained three", got)
	}
	// The exported cursors are strictly ascending: no deleted cursor came
	// back and no duplicate appeared.
	prev := int64(0)
	for _, c := range got {
		cur := int64(c.(float64))
		if cur <= prev {
			t.Fatalf("snapshot cursors not strictly ascending: %v", got)
		}
		prev = cur
	}
	// Pruning again converges the set to the three highest cursors.
	if w := pruneSnapshots(t, h, "doc", "dev-1", 3); w.Code != http.StatusOK {
		t.Fatalf("converging prune: %s", w.Body.String())
	}
	got = snapshotCursors(t, h, "doc")
	if len(got) != 3 ||
		int64(got[0].(float64)) != 10 || int64(got[1].(float64)) != 11 || int64(got[2].(float64)) != 12 {
		t.Fatalf("final snapshots = %v, want cursors 10, 11, 12", got)
	}
}

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

	h, st := open(t)
	registerDevice(t, h, "dev-1")
	postDocChanges(t, h, "doc", "dev-1", 4)
	for c := 1; c <= 4; c++ {
		putSnapshot(t, h, "doc", c, nil)
	}
	if w := pruneSnapshots(t, h, "doc", "dev-1", 2); w.Code != http.StatusOK ||
		w.Body.String() != `{"maxCursor":4,"deleted":2}`+"\n" {
		t.Fatalf("prune = %d %q", w.Code, w.Body.String())
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	h2, st2 := open(t)
	defer func() { _ = st2.Close() }()
	// The surviving set and the idempotent repeat are unchanged after restart.
	if got := snapshotCursors(t, h2, "doc"); len(got) != 2 ||
		int64(got[0].(float64)) != 3 || int64(got[1].(float64)) != 4 {
		t.Fatalf("snapshots after restart = %v, want cursors 3 and 4", got)
	}
	if w := pruneSnapshots(t, h2, "doc", "dev-1", 2); w.Code != http.StatusOK ||
		w.Body.String() != `{"maxCursor":4,"deleted":0}`+"\n" {
		t.Fatalf("repeat after restart = %d %q", w.Code, w.Body.String())
	}
	if w := getRaw(t, h2, "/v1/documents/doc/snapshots/2"); w.Code != http.StatusNotFound {
		t.Fatalf("pruned snapshot readable after restart = %d, want 404", w.Code)
	}
}

// ---- small local helpers (httptest without widening the import list) ----

func newPruneRequest(t *testing.T, method, path, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func pruneRaw(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	return pruneRawPath(t, h, "/v1/documents/doc/snapshots/prune", body)
}

func pruneRawPath(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, newPruneRequest(t, http.MethodPost, path, body))
	return w
}
