package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The service-level prune keeps the newest cursors and reports the surviving
// maximum together with the deleted count; an unknown document is a successful
// no-op.
func TestPruneSnapshotsServiceBasic(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2", "c3", "c4")); err != nil {
		t.Fatal(err)
	}
	for c := 1; c <= 4; c++ {
		if _, err := s.PutSnapshot("doc", int64(c), json.RawMessage(`{"s":1}`)); err != nil {
			t.Fatal(err)
		}
	}

	res, err := s.PruneSnapshots("doc", "dev", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}
	if res.MaxCursor != 4 || res.Removed != 2 {
		t.Fatalf("prune = %+v, want max 4 removed 2", res)
	}
	// The older snapshots are gone.
	if _, err := s.GetSnapshot("doc", 2); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("snapshot 2 err = %v, want ErrSnapshotNotFound", err)
	}
	// A repeat is idempotent.
	res, err = s.PruneSnapshots("doc", "dev", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}
	if res.MaxCursor != 4 || res.Removed != 0 {
		t.Fatalf("repeat prune = %+v, want max 4 removed 0", res)
	}

	// Unknown document: both numbers zero.
	res, err = s.PruneSnapshots("ghost", "dev", json.RawMessage(`1`))
	if err != nil {
		t.Fatal(err)
	}
	if res.MaxCursor != 0 || res.Removed != 0 {
		t.Fatalf("unknown prune = %+v, want zeroes", res)
	}
}

// An invalid keep is rejected after the gate; the gate's own verdicts precede
// it regardless of the keep value.
func TestPruneSnapshotsServiceGateAndKeep(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}

	// Invalid keep for an authorized doc... first the gate: revoked wins.
	if _, err := s.PruneSnapshots("doc", "dev", json.RawMessage(`0`)); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked bad keep err = %v, want ErrPermissionDenied", err)
	}
	// Unregistered device wins over the keep too.
	if _, err := s.PruneSnapshots("doc", "ghost", json.RawMessage(`0`)); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered bad keep err = %v, want ErrDeviceNotFound", err)
	}

	// Reauthorize; the invalid keep now surfaces and writes nothing.
	if _, err := s.SetDocumentPermission("doc", "dev", true); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{``, `null`, `0`, `-1`, `1.5`, `2.0`, `"2"`, `true`, `[1]`, `1001`} {
		if _, err := s.PruneSnapshots("doc", "dev", json.RawMessage(raw)); !errors.Is(err, events.ErrPruneKeepInvalid) {
			t.Fatalf("keep %q err = %v, want ErrPruneKeepInvalid", raw, err)
		}
	}
	// The boundary values are accepted.
	if _, err := s.PruneSnapshots("doc", "dev", json.RawMessage(`1`)); err != nil {
		t.Fatalf("keep 1 err = %v", err)
	}
	if _, err := s.PruneSnapshots("doc", "dev", json.RawMessage(`1000`)); err != nil {
		t.Fatalf("keep 1000 err = %v", err)
	}
}

// After compaction deleted the restores row and left only the retained
// identity summary, pruning the snapshot a prior restore came from must not
// change that restore's idempotency and conflict verdicts: the summary's
// provenance answers. A fresh restore of the pruned cursor still misses.
func TestPruneSnapshotsAfterCompactionKeepsRestoreVerdict(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"from":"s2"}`)); err != nil {
		t.Fatal(err)
	}
	// Restore snapshot 2 as r1 (cursor 3), snapshot the cursor, then compact:
	// the restore leaves the online log and keeps only the provenance summary.
	if res, err := s.RestoreSnapshot("doc", "dev", "r1", 2); err != nil || !res.Created || res.Cursor != 3 || res.RestoredFrom != 2 {
		t.Fatalf("restore = %+v err = %v", res, err)
	}
	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`{"from":"s3"}`)); err != nil {
		t.Fatal(err)
	}
	if boundary, removed, err := s.CompactChanges("doc", "dev"); err != nil || boundary != 3 || removed != 3 {
		t.Fatalf("compact = %d %d err = %v", boundary, removed, err)
	}

	// Add a snapshot at the older cursor 1 (still an existing cursor in the
	// never-reset space), then keep only the newest snapshot (cursor 3):
	// snapshots 1 and 2 are pruned.
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"from":"s1"}`)); err != nil {
		t.Fatal(err)
	}
	if res, err := s.PruneSnapshots("doc", "dev", json.RawMessage(`1`)); err != nil || res.MaxCursor != 3 || res.Removed != 2 {
		t.Fatalf("prune = %+v err = %v", res, err)
	}

	// The accepted restore is still idempotent from the retained summary even
	// though its snapshot row is gone: created=false, first cursor 3.
	res, err := s.RestoreSnapshot("doc", "dev", "r1", 2)
	if err != nil {
		t.Fatalf("idempotent restore = %v", err)
	}
	if res.Created || res.Cursor != 3 || res.RestoredFrom != 2 {
		t.Fatalf("idempotent restore = %+v", res)
	}
	// A different source cursor for the same id stays a conflict, not a 404.
	var conflict *events.ErrRestoreConflict
	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 1); !errors.As(err, &conflict) {
		t.Fatalf("source mismatch err = %v, want restore conflict", err)
	}
	// A fresh id against the pruned snapshot misses.
	if _, err := s.RestoreSnapshot("doc", "dev", "r2", 2); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("fresh restore err = %v, want ErrSnapshotNotFound", err)
	}
	// An id held by an ordinary trimmed change (c1's retained summary has no
	// restore provenance) also misses rather than conflicting: the snapshot
	// miss keeps its pre-prune precedence.
	if _, err := s.RestoreSnapshot("doc", "dev", "c1", 2); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("ordinary trimmed id restore err = %v, want ErrSnapshotNotFound", err)
	}
}
