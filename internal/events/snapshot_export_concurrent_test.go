package events_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
)

// TestExportSnapshotsConcurrentWriters verifies the read is a consistent view
// while snapshots are being committed: every export sees strictly ascending,
// unique cursors whose state matches exactly what the writer stored, so a
// snapshot appears in full or not at all — never half a row, never twice.
func TestExportSnapshotsConcurrentWriters(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	const n = 40
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("cc%d", i+1)
	}
	if _, err := s.PostChanges("doc", changes(ids...)); err != nil {
		t.Fatal(err)
	}

	states := make(map[int64]string, n)
	for i := int64(1); i <= n; i++ {
		states[i] = fmt.Sprintf(`{"v":%d}`, i)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := int64(1); i <= n; i++ {
			if _, err := s.PutSnapshot("doc", i, json.RawMessage(states[i])); err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			snaps, err := s.ExportSnapshots("doc", 0, nil)
			if err != nil {
				t.Errorf("export: %v", err)
				return
			}
			var prev int64 = -1
			for _, snap := range snaps {
				if snap.Cursor <= prev {
					t.Errorf("cursors not strictly ascending/unique: %d after %d", snap.Cursor, prev)
					return
				}
				prev = snap.Cursor
				if string(snap.State) != states[snap.Cursor] {
					t.Errorf("cursor %d state = %q, want %q", snap.Cursor, snap.State, states[snap.Cursor])
					return
				}
			}
		}
	}()

	wg.Wait()

	// After every writer commits, the full interval exports exactly n rows.
	snaps, err := s.ExportSnapshots("doc", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != n {
		t.Fatalf("final export = %d rows, want %d", len(snaps), n)
	}
}
