package events_test

import (
	"encoding/json"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// compactSeed commits three changes and a snapshot at cursor 2, so a
// compaction trims c1 and c2 and leaves c3 online.
func compactSeed(t *testing.T, s *app.App) {
	t.Helper()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2", "c3")); err != nil {
		t.Fatal(err)
	}
	created, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"s":1}`))
	if err != nil || !created {
		t.Fatalf("snapshot: created=%v err=%v", created, err)
	}
}

func TestCompactTrimsOnlineLogAndRetainsSummaries(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	compactSeed(t, s)

	boundary, removed, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if boundary != 2 || removed != 2 {
		t.Fatalf("compact = boundary %d removed %d, want 2/2", boundary, removed)
	}

	// The trimmed rows are gone from the online log; only the tail remains.
	var online int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM changes WHERE document_id = 'doc'`,
	).Scan(&online); err != nil {
		t.Fatal(err)
	}
	if online != 1 {
		t.Fatalf("online rows = %d, want 1", online)
	}

	// Each trimmed id keeps exactly one fixed-size summary: the source device
	// and a 32-byte digest stand in for the payload, so storage does not grow
	// back to the pre-compaction shape.
	var summaries int
	var maxDigest int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*), COALESCE(MAX(LENGTH(digest)), 0) FROM change_identities WHERE document_id = 'doc'`,
	).Scan(&summaries, &maxDigest); err != nil {
		t.Fatal(err)
	}
	if summaries != 2 || maxDigest != 32 {
		t.Fatalf("summaries = %d max digest %d, want 2 summaries of 32 bytes", summaries, maxDigest)
	}

	// A repeat compaction trims nothing and reports the same boundary.
	boundary, removed, err = s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if boundary != 2 || removed != 0 {
		t.Fatalf("re-compact = boundary %d removed %d, want 2/0", boundary, removed)
	}
}

func TestCompactBoundaryAndCursorSpaceSurviveFullTrim(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	boundary, removed, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if boundary != 2 || removed != 2 {
		t.Fatalf("compact = boundary %d removed %d, want 2/2", boundary, removed)
	}

	// The document stays known even with no online row: an empty read reports
	// the boundary, and the cursor space continues past it.
	exists, err := s.DocumentExists("doc")
	if err != nil || !exists {
		t.Fatalf("fully trimmed document exists = %v, err = %v", exists, err)
	}
	list, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 || next != 2 {
		t.Fatalf("list = %v next = %d, want empty with the boundary 2", list, next)
	}
	results, err := s.PostChanges("doc", changes("c3"))
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Created || results[0].Cursor != 3 {
		t.Fatalf("post after full trim = %+v, want created cursor 3", results[0])
	}
}

func TestCompactGateAndUnknownDocument(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// An unregistered device is rejected before anything is observed.
	if _, _, err := s.CompactChanges("doc", "ghost"); err == nil {
		t.Fatal("unregistered device compacted without error")
	}

	// An unknown document compacts successfully with boundary 0.
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	boundary, removed, err := s.CompactChanges("ghost-doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if boundary != 0 || removed != 0 {
		t.Fatalf("unknown compact = boundary %d removed %d, want 0/0", boundary, removed)
	}

	// A revoked device is denied and writes nothing.
	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompactChanges("doc", "dev-1"); err == nil {
		t.Fatal("revoked device compacted without error")
	}
}

// After a full trim the snapshot cursor judgment uses the never-reset cursor
// space: a cursor whose change left the online log is still an existing
// cursor, so creating a snapshot for it succeeds, a repeat reports
// created=false, and a restore of that snapshot keeps its idempotency.
func TestSnapshotCursorSurvivesFullTrim(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"s":2}`)); err != nil {
		t.Fatal(err)
	}
	boundary, removed, err := s.CompactChanges("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if boundary != 2 || removed != 2 {
		t.Fatalf("compact = boundary %d removed %d, want 2/2", boundary, removed)
	}

	// A trimmed cursor is still an existing cursor: the snapshot is created.
	created, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`))
	if err != nil || !created {
		t.Fatalf("snapshot at trimmed cursor: created=%v err=%v", created, err)
	}
	// A repeat with the same state is idempotent; a different state conflicts.
	created, err = s.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`))
	if err != nil || created {
		t.Fatalf("repeat snapshot: created=%v err=%v", created, err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"s":9}`)); err == nil {
		t.Fatal("conflicting snapshot at trimmed cursor succeeded")
	}
	// A cursor ahead of the high-water mark is still rejected.
	if _, err := s.PutSnapshot("doc", 3, json.RawMessage(`{"s":3}`)); err == nil {
		t.Fatal("snapshot beyond the boundary succeeded after full trim")
	}

	// The new snapshot restores like any other; the repeat restore returns
	// created=false with the first cursor.
	result, err := s.RestoreSnapshot("doc", "dev-1", "r1", 1)
	if err != nil || !result.Created || result.Cursor != 3 || result.RestoredFrom != 1 {
		t.Fatalf("restore = %+v err=%v, want created cursor 3 restoredFrom 1", result, err)
	}
	result, err = s.RestoreSnapshot("doc", "dev-1", "r1", 1)
	if err != nil || result.Created || result.Cursor != 3 || result.RestoredFrom != 1 {
		t.Fatalf("repeat restore = %+v err=%v, want not-created cursor 3", result, err)
	}
}
