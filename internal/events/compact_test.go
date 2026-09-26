package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// compactFixture seeds doc with n ordinary changes from dev-1 and registers
// the device so the compaction gate passes.
func compactFixture(t *testing.T, s *app.App, doc string, n int) {
	t.Helper()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, string(rune('a'+i))+"-id")
	}
	batch := make([]events.Change, n)
	for i := 0; i < n; i++ {
		batch[i] = events.Change{ID: ids[i], DeviceID: "dev-1", Payload: json.RawMessage(`{"n":` + jsonNumber(i+1) + `}`)}
	}
	if _, err := s.PostChanges(doc, batch); err != nil {
		t.Fatal(err)
	}
}

func jsonNumber(n int) string {
	raw, _ := json.Marshal(n)
	return string(raw)
}

func TestCompactTrimsUpToSnapshotBoundary(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 5)

	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 3 || res.Removed != 3 {
		t.Fatalf("compact = %+v, want boundary 3 removed 3", res)
	}

	// Only online rows past the boundary are listed.
	listed, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Cursor != 4 || listed[1].Cursor != 5 || next != 5 {
		t.Fatalf("list after compact = %+v next=%d", listed, next)
	}

	// A read starting inside the trimmed range sees the same online tail.
	listed, _, err = s.ListChanges("doc", 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Cursor != 4 {
		t.Fatalf("list from trimmed range = %+v", listed)
	}

	// Repeating the compaction reports the same boundary and removes nothing.
	res, err = s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 3 || res.Removed != 0 {
		t.Fatalf("re-compact = %+v, want boundary 3 removed 0", res)
	}
}

func TestCompactWithoutSnapshotIsNoOp(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 2)

	res, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 0 || res.Removed != 0 {
		t.Fatalf("compact = %+v, want boundary 0 removed 0", res)
	}
	listed, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || next != 2 {
		t.Fatalf("no-op compact changed the log: %+v next=%d", listed, next)
	}
}

func TestCompactUnknownDocumentSucceedsWithZeroBoundary(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	res, err := s.CompactChanges("ghost", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 0 || res.Removed != 0 {
		t.Fatalf("compact = %+v, want zeroes", res)
	}
	// The document stays unknown: reads still report an empty list and 0.
	listed, next, err := s.ListChanges("ghost", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 || next != 0 {
		t.Fatalf("ghost doc became known: %+v next=%d", listed, next)
	}
	if known, _ := s.DocumentExists("ghost"); known {
		t.Fatal("ghost doc known after no-op compaction")
	}
}

func TestCompactEmptyPageNextCursorFloorsAtBoundary(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 3)
	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// The whole log is trimmed; the document stays known and an empty page
	// reports the larger of the requested cursor and the boundary.
	listed, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 || next != 3 {
		t.Fatalf("empty page = %+v next=%d, want empty/3", listed, next)
	}
	if known, _ := s.DocumentExists("doc"); !known {
		t.Fatal("fully compacted document is no longer known")
	}

	// A later cursor above the boundary still echoes itself.
	_, next, err = s.ListChanges("doc", 9, 100)
	if err != nil {
		t.Fatal(err)
	}
	if next != 9 {
		t.Fatalf("next = %d, want 9", next)
	}

	// Long polling inherits the same floor on an immediate timeout.
	_, next, timedOut, err := s.WaitForChanges(context.Background(), "doc", 0, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 || !timedOut {
		t.Fatalf("poll = next %d timedOut %v, want 3/true", next, timedOut)
	}
}

func TestCompactKeepsIdempotencyAndConflictDecisions(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 2) // a-id {"n":1} cursor 1, b-id {"n":2} cursor 2
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// Same device and payload: idempotent with the first cursor, zero writes.
	results, err := s.PostChanges("doc", []events.Change{{ID: "a-id", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("re-post = %+v, want created=false cursor 1", results)
	}

	// JSON-semantic equality still applies (key order, number formatting).
	results, err = s.PostChanges("doc", []events.Change{{ID: "b-id", DeviceID: "dev-1", Payload: json.RawMessage(`{ "n": 2.0 }`)}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[0].Cursor != 2 {
		t.Fatalf("semantic re-post = %+v, want created=false cursor 2", results)
	}

	// Different payload or different device: conflict, nothing written.
	for _, c := range []events.Change{
		{ID: "a-id", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":99}`)},
		{ID: "a-id", DeviceID: "dev-2", Payload: json.RawMessage(`{"n":1}`)},
	} {
		var conflict *events.ErrConflict
		if err := postOne(s, "doc", c); !errors.As(err, &conflict) {
			t.Fatalf("post %+v = %v, want conflict", c, err)
		}
	}

	// Replay and merge answer the trimmed id the same way.
	results, err = s.ReplayChanges("doc", []events.Change{{ID: "a-id", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("replay = %+v, want created=false cursor 1", results)
	}
	merged, err := s.MergeChange("doc", 2, events.Change{ID: "b-id", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Outcome != "idempotent" || merged.Cursor != 2 {
		t.Fatalf("merge = %+v, want idempotent cursor 2", merged)
	}
}

func postOne(s *app.App, doc string, c events.Change) error {
	_, err := s.PostChanges(doc, []events.Change{c})
	return err
}

func TestCompactCursorSpaceContinuesAfterTrim(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	compactFixture(t, s, "doc", 3)
	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// The whole online log is gone, yet the next change continues past the
	// greatest cursor ever allocated.
	results, err := s.PostChanges("doc", []events.Change{{ID: "new-1", DeviceID: "dev-1", Payload: json.RawMessage(`1`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Created || results[0].Cursor != 4 {
		t.Fatalf("post after full trim = %+v, want cursor 4", results)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// After a restart the high-water mark, the boundary and the trimmed
	// identities are all still in force.
	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	results, err = s2.PostChanges("doc", []events.Change{{ID: "new-2", DeviceID: "dev-1", Payload: json.RawMessage(`2`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Created || results[0].Cursor != 5 {
		t.Fatalf("post after restart = %+v, want cursor 5", results)
	}
	results, err = s2.PostChanges("doc", []events.Change{{ID: "a-id", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":1}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("trimmed id after restart = %+v, want created=false cursor 1", results)
	}
	listed, next, err := s2.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 || listed[0].Cursor != 4 || listed[1].Cursor != 5 || next != 5 {
		t.Fatalf("list after restart = %+v next=%d", listed, next)
	}
	res, err := s2.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 3 || res.Removed != 0 {
		t.Fatalf("re-compact after restart = %+v, want boundary 3 removed 0", res)
	}
}

func TestCompactMergeBaseBelowBoundaryRejected(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 4)
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// A base cursor below the boundary can no longer be conflict-checked.
	if _, err := s.MergeChange("doc", 1, events.Change{ID: "m-1", DeviceID: "dev-1", Payload: json.RawMessage(`{"x":1}`)}); !errors.Is(err, events.ErrStaleCursor) {
		t.Fatalf("merge below boundary = %v, want ErrStaleCursor", err)
	}
	// At the boundary the online tail is still fully checkable.
	if _, err := s.MergeChange("doc", 2, events.Change{ID: "m-1", DeviceID: "dev-1", Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatalf("merge at boundary = %v", err)
	}
	// The rejected merge wrote nothing: the accepted one took cursor 5.
	_, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if next != 5 {
		t.Fatalf("next = %d, want 5", next)
	}
}

func TestCompactRestoreIdempotencyAfterTrim(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 2)
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	restored, err := s.RestoreSnapshot("doc", "dev-1", "restore-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Cursor != 3 {
		t.Fatalf("restore cursor = %d, want 3", restored.Cursor)
	}
	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`{"s":2}`)); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Boundary != 3 || res.Removed != 3 {
		t.Fatalf("compact = %+v, want boundary 3 removed 3", res)
	}

	// The trimmed restore is still idempotent with the same provenance.
	again, err := s.RestoreSnapshot("doc", "dev-1", "restore-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if again.Created || again.Cursor != 3 || again.RestoredFrom != 1 {
		t.Fatalf("re-restore = %+v, want created=false cursor 3 from 1", again)
	}
	// A different snapshot cursor or device for the trimmed id conflicts.
	if _, err := s.RestoreSnapshot("doc", "dev-2", "restore-1", 1); err == nil {
		t.Fatal("restore with different device after trim did not conflict")
	}
	// The trimmed ordinary id conflicts with a restore attempt, exactly like
	// an online ordinary change would.
	if _, err := s.RestoreSnapshot("doc", "dev-1", "a-id", 1); err == nil {
		t.Fatal("restore over trimmed ordinary id did not conflict")
	}
}

func TestCompactDoesNotWakeLongPoll(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 2)
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}

	type pollOut struct {
		next     int64
		timedOut bool
		err      error
	}
	done := make(chan pollOut, 1)
	go func() {
		_, next, timedOut, err := s.WaitForChanges(context.Background(), "doc", 2, 100, 300*time.Millisecond)
		done <- pollOut{next, timedOut, err}
	}()

	// Compaction is not a change: the parked poll must not observe a wake.
	time.Sleep(50 * time.Millisecond)
	if _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case out := <-done:
		if out.err != nil {
			t.Fatal(out.err)
		}
		if !out.timedOut || out.next != 2 {
			t.Fatalf("poll = next %d timedOut %v, want 2/true", out.next, out.timedOut)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll did not return")
	}
}

func TestCompactGateEnforced(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	compactFixture(t, s, "doc", 1)
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	// Unregistered device: 404 semantics, nothing trimmed.
	if _, err := s.CompactChanges("doc", "ghost-device"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("compact with unknown device = %v, want ErrDeviceNotFound", err)
	}
	// Revoked device: 403 semantics, nothing trimmed.
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactChanges("doc", "dev-2"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("compact with revoked device = %v, want ErrPermissionDenied", err)
	}
	listed, _, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 {
		t.Fatalf("rejected compactions trimmed the log: %+v", listed)
	}
}
