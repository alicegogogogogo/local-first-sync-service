package events_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
)

// seedSnapshots gives doc a change at every cursor 1..n and a snapshot at each
// of the given cursors with distinct states.
func seedSnapshots(t *testing.T, s *app.App, doc string, n int, cursors ...int64) {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-c%d", doc, i+1)
	}
	if _, err := s.PostChanges(doc, changes(ids...)); err != nil {
		t.Fatalf("seed changes: %v", err)
	}
	for i, cur := range cursors {
		state := json.RawMessage(fmt.Sprintf(`{"k":%d}`, i+1))
		if _, err := s.PutSnapshot(doc, cur, state); err != nil {
			t.Fatalf("seed snapshot %d: %v", cur, err)
		}
	}
}

func snapshotCursors(snaps []events.ExportedSnapshot) []int64 {
	out := make([]int64, len(snaps))
	for i, snap := range snaps {
		out[i] = snap.Cursor
	}
	return out
}

func TestExportSnapshotsRangesAndOrder(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	seedSnapshots(t, s, "doc", 5, 1, 2, 4, 5)
	// A second document's snapshots must never leak into the export.
	seedSnapshots(t, s, "other", 2, 1, 2)

	all, err := s.ExportSnapshots("doc", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotCursors(all); fmt.Sprint(got) != "[1 2 4 5]" {
		t.Fatalf("default export cursors = %v, want [1 2 4 5]", got)
	}

	// Closed interval: both ends are inclusive.
	ranged, err := s.ExportSnapshots("doc", 2, ptr64(4))
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotCursors(ranged); fmt.Sprint(got) != "[2 4]" {
		t.Fatalf("2..4 = %v, want [2 4]", got)
	}

	// A single-cursor interval returns at most that one snapshot.
	one, err := s.ExportSnapshots("doc", 4, ptr64(4))
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotCursors(one); fmt.Sprint(got) != "[4]" {
		t.Fatalf("4..4 = %v, want [4]", got)
	}

	// from with no upper bound runs to the end.
	tail, err := s.ExportSnapshots("doc", 4, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshotCursors(tail); fmt.Sprint(got) != "[4 5]" {
		t.Fatalf("from 4 = %v, want [4 5]", got)
	}
}

func TestExportSnapshotsEmptyIsNotAnError(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	seedSnapshots(t, s, "doc", 3, 1, 2)

	// Unknown document: an empty slice, not an error.
	unknown, err := s.ExportSnapshots("ghost", 0, nil)
	if err != nil || len(unknown) != 0 {
		t.Fatalf("unknown document: rows=%d err=%v", len(unknown), err)
	}
	// Known document, no snapshot inside the interval: same treatment.
	miss, err := s.ExportSnapshots("doc", 9, nil)
	if err != nil || len(miss) != 0 {
		t.Fatalf("empty interval: rows=%d err=%v", len(miss), err)
	}
}

func TestExportSnapshotsStateVerbatimAndStableAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2", "c3", "c4", "c5")); err != nil {
		t.Fatal(err)
	}
	// Every JSON value shape is stored and returned byte-for-byte, including
	// null, a number, a string, an array and an object with non-alphabetic key
	// order and whitespace.
	states := map[int64]string{
		1: `null`,
		2: `12.50`,
		3: `"str"`,
		4: `[1, 2 ,3]`,
		5: `{"b":2,"a":1}`,
	}
	for cur, state := range states {
		if _, err := s.PutSnapshot("doc", cur, json.RawMessage(state)); err != nil {
			t.Fatalf("put %d: %v", cur, err)
		}
	}

	first, err := s.ExportSnapshots("doc", 0, nil)
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

	second, err := s2.ExportSnapshots("doc", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("row count changed across restart: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Cursor != second[i].Cursor {
			t.Fatalf("cursor %d vs %d after restart", first[i].Cursor, second[i].Cursor)
		}
		if string(first[i].State) != string(second[i].State) {
			t.Fatalf("state at cursor %d changed across restart: %q vs %q",
				first[i].Cursor, first[i].State, second[i].State)
		}
		if want := states[first[i].Cursor]; string(first[i].State) != want {
			t.Fatalf("cursor %d state = %q, want verbatim %q", first[i].Cursor, first[i].State, want)
		}
	}
}

func TestExportSnapshotsIsReadOnly(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	seedSnapshots(t, s, "doc", 2, 1, 2)

	for i := 0; i < 3; i++ {
		if _, err := s.ExportSnapshots("doc", 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Exports allocate no cursors: the next change takes cursor 3.
	results, err := s.PostChanges("doc", changes("doc-c3"))
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Cursor != 3 {
		t.Fatalf("cursor after exports = %d, want 3", results[0].Cursor)
	}
}

func ptr64(v int64) *int64 { return &v }
