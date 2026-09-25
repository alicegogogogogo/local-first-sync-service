package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
)

// replayCRDT submits a single-op batch that must be accepted as an idempotent
// replay (created=false).
func replayCRDT(t *testing.T, s *Store, doc, typ string, op CRDTOp) {
	t.Helper()
	results, err := s.SubmitCRDTOps(doc, typ, []CRDTOp{op})
	if err != nil {
		t.Fatalf("replay %s/%s: %v", typ, op.ID, err)
	}
	if len(results) != 1 || results[0].ID != op.ID || results[0].Created {
		t.Fatalf("replay %s/%s = %+v, want created=false", typ, op.ID, results)
	}
}

// conflictCRDT submits a single-op batch that must be rejected as a conflict
// leaving the merged state untouched.
func conflictCRDT(t *testing.T, s *Store, doc, typ string, op CRDTOp) {
	t.Helper()
	_, err := s.SubmitCRDTOps(doc, typ, []CRDTOp{op})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("conflict %s/%s err = %v, want *ErrCRDTConflict", typ, op.ID, err)
	}
	if conflict.ID != op.ID {
		t.Fatalf("conflict %s/%s reported id %q", typ, op.ID, conflict.ID)
	}
}

func TestCompactedCounterIdStaysIdempotentAndConflicting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// a1 was trimmed: its contribution is below the device's retained maximum,
	// yet a faithful replay is an idempotent repeat, not a regression.
	replayCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	// The same id with a different contribution or a different device is a
	// conflict and changes nothing, even though the operation row is gone.
	conflictCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(6)})
	conflictCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-2", Value: rawInt(5)})
	if got := crdtStateValue(t, s, "doc"); got != "8" {
		t.Fatalf("state after replays and conflicts = %s, want 8", got)
	}
	snapshot, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || snapshot.Tombstones != 0 {
		t.Fatalf("snapshot after replays = %+v, want 1/0", snapshot)
	}
}

func TestCompactedGSetIdStaysIdempotentAndConflicting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g1", DeviceID: "dev-1", Elements: []string{"apple", "banana"}})
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-1", Elements: []string{"apple"}})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// g2 was trimmed (fully covered by g1): a faithful replay — element order
	// and duplicates irrelevant — is idempotent.
	replayCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-1", Elements: []string{"apple"}})
	// A different element set or a different device under the trimmed id is a
	// conflict and changes nothing.
	conflictCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-1", Elements: []string{"cherry"}})
	conflictCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-2", Elements: []string{"apple"}})
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana"]` {
		t.Fatalf("state after replays and conflicts = %s", got)
	}
	snapshot, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || snapshot.Tombstones != 0 {
		t.Fatalf("snapshot after replays = %+v, want 1/0", snapshot)
	}
}

func TestCompactedRegisterIdStaysIdempotentAndConflicting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"first"`)})
	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r2", DeviceID: "dev-1", Version: 2, Value: json.RawMessage(`"second"`)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// r1 was trimmed: its version is below the device's retained maximum, yet
	// a faithful replay is an idempotent repeat, not a stalled clock.
	replayCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"first"`)})
	// A different value, version or device under the trimmed id is a conflict
	// and changes nothing.
	conflictCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"other"`)})
	conflictCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 3, Value: json.RawMessage(`"first"`)})
	conflictCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-2", Version: 1, Value: json.RawMessage(`"first"`)})
	if got := crdtStateValue(t, s, "doc"); got != `"second"` {
		t.Fatalf("state after replays and conflicts = %s", got)
	}
	snapshot, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || snapshot.Tombstones != 0 {
		t.Fatalf("snapshot after replays = %+v, want 1/0", snapshot)
	}
}

func TestCompactedORSetIdStaysIdempotentAndConflicting(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o2", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	// The trim emptied the tag and tombstone tables.
	snapshot, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 0 || snapshot.Tombstones != 0 {
		t.Fatalf("snapshot after compaction = %+v, want 0/0", snapshot)
	}

	// Replaying the trimmed add resurrects nothing; replaying the trimmed
	// remove tombstones nothing that landed later.
	replayCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o3", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	replayCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o2", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"})
	if got := crdtStateValue(t, s, "doc"); got != `["apple"]` {
		t.Fatalf("state after replays = %s, want [\"apple\"]", got)
	}
	// A different action, element or device under a trimmed id is a conflict
	// and changes nothing.
	conflictCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "cherry"})
	conflictCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"})
	conflictCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-2", Action: CRDTORSetAdd, Element: "apple"})
	if got := crdtStateValue(t, s, "doc"); got != `["apple"]` {
		t.Fatalf("state after conflicts = %s, want [\"apple\"]", got)
	}
}

func TestCompactedDecisionsPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// The trimmed id's replay and conflict decisions survive the restart.
	replayCRDT(t, s2, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	conflictCRDT(t, s2, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(6)})
	conflictCRDT(t, s2, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-2", Value: rawInt(5)})
	if got := crdtStateValue(t, s2, "doc"); got != "8" {
		t.Fatalf("state after reopen = %s, want 8", got)
	}
}
