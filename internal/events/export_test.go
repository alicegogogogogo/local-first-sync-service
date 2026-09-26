package events_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

func ptrInt64(v int64) *int64 { return &v }

// seedSnapshotAt appends one change (advancing the document cursor to cursor)
// and stores state as the snapshot there.
func seedSnapshotAt(t *testing.T, s *app.App, doc string, cursor int64, state string) {
	t.Helper()
	if _, err := s.PostChanges(doc, changes(fmt.Sprintf("c%d", cursor))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot(doc, cursor, json.RawMessage(state)); err != nil {
		t.Fatalf("put snapshot cursor=%d: %v", cursor, err)
	}
}

func TestExportSnapshotsRange(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-export"
	// Snapshots at cursors 1..4 with state covering every JSON shape.
	states := []string{"null", `42`, `"str"`, `[1,2]`}
	for i, st := range states {
		seedSnapshotAt(t, s, doc, int64(i+1), st)
	}

	// Whole range, no bounds: ascending and verbatim.
	got, err := s.ExportSnapshots(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("export len = %d, want 4: %+v", len(got), got)
	}
	for i, snap := range got {
		if snap.Cursor != int64(i+1) {
			t.Fatalf("item %d cursor = %d, want %d", i, snap.Cursor, i+1)
		}
		if string(snap.State) != states[i] {
			t.Fatalf("item %d state = %s, want %s", i, snap.State, states[i])
		}
	}

	// Closed interval: both endpoints included.
	got, err = s.ExportSnapshots(doc, 2, ptrInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cursor != 2 || got[1].Cursor != 3 {
		t.Fatalf("[2,3] = %+v", got)
	}

	// A degenerate interval still includes the single matching cursor.
	got, err = s.ExportSnapshots(doc, 2, ptrInt64(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Cursor != 2 || string(got[0].State) != `42` {
		t.Fatalf("[2,2] = %+v", got)
	}

	// Lower bound only.
	got, err = s.ExportSnapshots(doc, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Cursor != 3 || got[1].Cursor != 4 {
		t.Fatalf("from=3 = %+v", got)
	}

	// A range without a snapshot is an empty non-nil list, not an error.
	got, err = s.ExportSnapshots(doc, 99, ptrInt64(200))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty range = %+v, want empty non-nil", got)
	}

	// An unknown document is likewise an empty list.
	got, err = s.ExportSnapshots("never-heard-of-it", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("unknown doc = %+v, want empty non-nil", got)
	}
}

func TestExportSnapshotsIsReadOnly(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-ro"
	seedSnapshotAt(t, s, doc, 1, `{"v":1}`)

	if _, err := s.ExportSnapshots(doc, 0, nil); err != nil {
		t.Fatal(err)
	}

	// The change log cursor is unchanged (still 1) and the snapshot reads back
	// the same.
	rows, next, err := s.ListChanges(doc, 0, 100)
	if err != nil || len(rows) != 1 || next != 1 {
		t.Fatalf("changes after export = %+v next=%d err=%v", rows, next, err)
	}
	st, err := s.GetSnapshot(doc, 1)
	if err != nil || string(st) != `{"v":1}` {
		t.Fatalf("snapshot after export = %s err=%v", st, err)
	}
}

func TestExportSnapshotsConcurrentCreate(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	doc := "doc-conc"
	// Seed 400 changes so snapshots can be stored at cursors 1..400.
	ids := make([]string, 400)
	for i := range ids {
		ids[i] = fmt.Sprintf("c%d", i+1)
	}
	if _, err := s.PostChanges(doc, changes(ids...)); err != nil {
		t.Fatal(err)
	}

	const writers = 8
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(base int64) {
			defer wg.Done()
			for i := int64(0); i < 50; i++ {
				cursor := base + i // writers claim disjoint cursor windows
				if _, err := s.PutSnapshot(doc, cursor,
					json.RawMessage(fmt.Sprintf(`{"w":%d}`, cursor))); err != nil {
					t.Errorf("put %d: %v", cursor, err)
					return
				}
			}
		}(int64(w)*50 + 1)
	}

	// Concurrently export the whole range repeatedly. Every result must be a
	// self-consistent set: ascending, unique cursors, and each present row
	// complete. Counts can differ between runs (commits land in between), but
	// no half row can ever appear because each row is one committed INSERT
	// and the read is one statement.
	for i := 0; i < 200; i++ {
		got, err := s.ExportSnapshots(doc, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		var prev int64
		for _, snap := range got {
			if snap.Cursor <= prev {
				t.Fatalf("non-ascending/duplicate cursor %d after %d", snap.Cursor, prev)
			}
			prev = snap.Cursor
			var m map[string]any
			if err := json.Unmarshal(snap.State, &m); err != nil || m["w"] == nil {
				t.Fatalf("half/corrupt row at cursor %d: %s", snap.Cursor, snap.State)
			}
		}
	}
	wg.Wait()

	// After every writer committed, the full export is exactly 400 rows.
	got, err := s.ExportSnapshots(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 400 {
		t.Fatalf("final export len = %d, want 400", len(got))
	}
}

func TestExportSnapshotsPersistenceAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := "doc-restart"
	seedSnapshotAt(t, s, doc, 1, `{"k":"v"}`)
	seedSnapshotAt(t, s, doc, 2, `1`)
	seedSnapshotAt(t, s, doc, 3, `null`)

	before, err := s.ExportSnapshots(doc, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	after, err := s2.ExportSnapshots(doc, 1, ptrInt64(3))
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("after restart len = %d, want %d", len(after), len(before))
	}
	for i := range before {
		if before[i].Cursor != after[i].Cursor || string(before[i].State) != string(after[i].State) {
			t.Fatalf("row %d changed across restart: %+v vs %+v", i, before[i], after[i])
		}
	}
}
