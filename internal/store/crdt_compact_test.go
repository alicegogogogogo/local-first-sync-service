package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func submitCounter(t *testing.T, s *Store, doc, device, id string, value int64) {
	t.Helper()
	if _, err := s.SubmitCRDTOps(doc, CRDTTypeCounter, []CRDTOp{
		{ID: id, DeviceID: device, Value: rawInt(value)},
	}); err != nil {
		t.Fatalf("submit counter %s=%d: %v", id, value, err)
	}
}

func submitGSet(t *testing.T, s *Store, doc, device, id string, elements ...string) {
	t.Helper()
	if _, err := s.SubmitCRDTOps(doc, CRDTTypeGSet, []CRDTOp{
		{ID: id, DeviceID: device, Elements: elements},
	}); err != nil {
		t.Fatalf("submit gset %s: %v", id, err)
	}
}

func submitRegister(t *testing.T, s *Store, doc, device, id string, version int64, value string) {
	t.Helper()
	if _, err := s.SubmitCRDTOps(doc, CRDTTypeRegister, []CRDTOp{
		{ID: id, DeviceID: device, Version: version, Value: []byte(value)},
	}); err != nil {
		t.Fatalf("submit register %s: %v", id, err)
	}
}

func submitORSetOp(t *testing.T, s *Store, doc, device, id, action, element string) {
	t.Helper()
	if _, err := s.SubmitCRDTOps(doc, CRDTTypeORSet, []CRDTOp{
		{ID: id, DeviceID: device, Action: action, Element: element},
	}); err != nil {
		t.Fatalf("submit orset %s %s %s: %v", id, action, element, err)
	}
}

func TestCRDTSnapshotNotFoundBeforeAnyOp(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.GetCRDTSnapshot("doc"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("snapshot err = %v, want ErrCRDTNotFound", err)
	}
	registerDevices(t, s, "dev-1")
	if _, err := s.CompactCRDT("doc", "dev-1"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("compact err = %v, want ErrCRDTNotFound", err)
	}
}

func TestCRDTCompactCounterDropsDominatedOps(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCounter(t, s, "doc", "dev-1", "a1", 3)
	submitCounter(t, s, "doc", "dev-2", "b1", 7)
	submitCounter(t, s, "doc", "dev-1", "a2", 5) // dev-1 max advances 3 -> 5

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 3 || before.Tombstones != 0 {
		t.Fatalf("before = %d ops %d tombstones, want 3/0", before.Operations, before.Tombstones)
	}
	if string(before.Value) != "12" {
		t.Fatalf("before value = %s, want 12", before.Value)
	}

	snap, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// a1 (3 < dev-1's max 5) is trimmed; a2 and b1 stay.
	if snap.Operations != 2 || snap.Tombstones != 0 {
		t.Fatalf("after = %d ops %d tombstones, want 2/0", snap.Operations, snap.Tombstones)
	}
	if string(snap.Value) != "12" {
		t.Fatalf("compacted value = %s, want 12", snap.Value)
	}

	// The merged state is byte-for-byte the pre-compaction one.
	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != "12" {
		t.Fatalf("state after compact = %s, want 12", state.Value)
	}

	// A repeated compact trims nothing further and reports the same counts.
	again, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Type != snap.Type || string(again.Value) != string(snap.Value) ||
		again.Operations != snap.Operations || again.Tombstones != snap.Tombstones {
		t.Fatalf("re-compact = %+v, want %+v", again, snap)
	}

	// The retained maxima still drive conflict decisions: a regression is
	// rejected and an advance is accepted.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a3", DeviceID: "dev-1", Value: rawInt(4)},
	}); err == nil {
		t.Fatal("regression after compact should conflict")
	} else {
		var conflict *ErrCRDTConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("regression err = %v, want *ErrCRDTConflict", err)
		}
	}
	// Re-posting a retained operation stays idempotent.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a2", DeviceID: "dev-1", Value: rawInt(5)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created {
		t.Fatal("re-posting a retained op should be idempotent")
	}
}

func TestCRDTCompactGSetDropsRedundantOps(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitGSet(t, s, "doc", "dev-1", "g1", "apple", "banana")
	submitGSet(t, s, "doc", "dev-1", "g2", "apple") // fully covered by g1

	snap, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 1 || snap.Tombstones != 0 {
		t.Fatalf("after = %d ops %d tombstones, want 1/0", snap.Operations, snap.Tombstones)
	}
	if string(snap.Value) != `["apple","banana"]` {
		t.Fatalf("compacted value = %s", snap.Value)
	}

	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `["apple","banana"]` {
		t.Fatalf("state after compact = %s", state.Value)
	}
	// Re-posting the retained op stays idempotent.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"apple", "banana"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created {
		t.Fatal("re-posting a retained op should be idempotent")
	}
}

func TestCRDTCompactRegisterKeepsPerDeviceMaxima(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitRegister(t, s, "doc", "dev-1", "r1", 1, `"old"`)
	submitRegister(t, s, "doc", "dev-1", "r2", 2, `"older"`)
	submitRegister(t, s, "doc", "dev-2", "s1", 1, `"other"`)
	submitRegister(t, s, "doc", "dev-1", "r3", 3, `"winner"`)

	snap, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// Only dev-1's r3 and dev-2's s1 still decide anything.
	if snap.Operations != 2 || snap.Tombstones != 0 {
		t.Fatalf("after = %d ops %d tombstones, want 2/0", snap.Operations, snap.Tombstones)
	}
	if string(snap.Value) != `"winner"` {
		t.Fatalf("compacted value = %s", snap.Value)
	}

	// The per-device maxima still gate versions: a stalled version conflicts.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		{ID: "r4", DeviceID: "dev-1", Version: 3, Value: []byte(`"stall"`)},
	}); err == nil {
		t.Fatal("stalled version after compact should conflict")
	} else {
		var conflict *ErrCRDTConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("stalled version err = %v, want *ErrCRDTConflict", err)
		}
	}
	// A strictly greater version is still accepted and wins.
	submitRegister(t, s, "doc", "dev-1", "r5", 4, `"next"`)
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `"next"` {
		t.Fatalf("state after advance = %s", state.Value)
	}
}

func TestCRDTCompactORSetDropsDeadTagsAndTombstones(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitORSetOp(t, s, "doc", "dev-1", "o1", CRDTORSetAdd, "apple")
	submitORSetOp(t, s, "doc", "dev-1", "o2", CRDTORSetAdd, "banana")
	submitORSetOp(t, s, "doc", "dev-1", "o3", CRDTORSetRemove, "apple")

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 3 || before.Tombstones != 1 {
		t.Fatalf("before = %d ops %d tombstones, want 3/1", before.Operations, before.Tombstones)
	}

	snap, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// The dead tag and its tombstone are gone; the operation records stay so
	// replays remain idempotent.
	if snap.Operations != 3 || snap.Tombstones != 0 {
		t.Fatalf("after = %d ops %d tombstones, want 3/0", snap.Operations, snap.Tombstones)
	}
	if string(snap.Value) != `["banana"]` {
		t.Fatalf("compacted value = %s", snap.Value)
	}

	// Replays of every original operation stay idempotent — in particular the
	// remove does not resurrect or re-tombstone anything.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"},
		{ID: "o3", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[1].Created {
		t.Fatalf("replays after compact = %+v, want both idempotent", results)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `["banana"]` {
		t.Fatalf("state after replays = %s", state.Value)
	}

	// A fresh remove of the fully removed element stays an accepted no-op.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		{ID: "o4", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"},
	}); err != nil {
		t.Fatalf("remove of removed element: %v", err)
	}
	state, _ = s.GetCRDTState("doc")
	if string(state.Value) != `["banana"]` {
		t.Fatalf("state after no-op remove = %s", state.Value)
	}
}

func TestCRDTCompactGatesDeviceAndPermission(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCounter(t, s, "doc", "dev-1", "a1", 5)

	if _, err := s.CompactCRDT("doc", "ghost"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unregistered device err = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactCRDT("doc", "dev-2"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked device err = %v, want ErrPermissionDenied", err)
	}

	// Neither rejection trimmed anything.
	snap, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 1 {
		t.Fatalf("ops after rejected compacts = %d, want 1", snap.Operations)
	}
}

func TestCRDTCompactLeavesChangeLogAlone(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")
	submitCounter(t, s, "doc", "dev-1", "a1", 5)

	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	// No change rows, no cursor allocation: the document stays unknown to the
	// change log.
	changes, nextCursor, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || nextCursor != 0 {
		t.Fatalf("change log after compact = %v/%d, want empty/0", changes, nextCursor)
	}
}

func TestCRDTCompactSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1")
	submitCounter(t, s, "doc", "dev-1", "a1", 3)
	submitCounter(t, s, "doc", "dev-1", "a2", 5)
	submitORSetOp(t, s, "set", "dev-1", "o1", CRDTORSetAdd, "apple")
	submitORSetOp(t, s, "set", "dev-1", "o2", CRDTORSetRemove, "apple")

	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactCRDT("set", "dev-1"); err != nil {
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

	counter, err := s2.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Operations != 1 || string(counter.Value) != "5" {
		t.Fatalf("counter after restart = %d ops value %s, want 1/5", counter.Operations, counter.Value)
	}
	orset, err := s2.GetCRDTSnapshot("set")
	if err != nil {
		t.Fatal(err)
	}
	if orset.Tombstones != 0 || string(orset.Value) != `[]` {
		t.Fatalf("orset after restart = %d tombstones value %s, want 0/[]", orset.Tombstones, orset.Value)
	}
	// Conflict decisions survive the restart too: the counter maximum is
	// still enforced.
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a3", DeviceID: "dev-1", Value: rawInt(4)},
	}); err == nil {
		t.Fatal("regression after restart should conflict")
	}
}
