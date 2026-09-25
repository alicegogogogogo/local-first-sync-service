package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func submitCRDT(t *testing.T, s *Store, doc, typ string, ops ...CRDTOp) {
	t.Helper()
	if _, err := s.SubmitCRDTOps(doc, typ, ops); err != nil {
		t.Fatalf("submit %s op: %v", typ, err)
	}
}

func crdtStateValue(t *testing.T, s *Store, doc string) string {
	t.Helper()
	state, err := s.GetCRDTState(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(state.Value)
}

func TestCompactCounterTrimsSupersededContributions(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "b1", DeviceID: "dev-2", Value: rawInt(3)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 3 || before.Tombstones != 0 {
		t.Fatalf("before compaction: operations=%d tombstones=%d, want 3/0", before.Operations, before.Tombstones)
	}
	if string(before.Value) != "11" {
		t.Fatalf("before compaction: value = %s, want 11", before.Value)
	}

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 2 || snapshot.Tombstones != 0 {
		t.Fatalf("after compaction: operations=%d tombstones=%d, want 2/0", snapshot.Operations, snapshot.Tombstones)
	}
	if snapshot.Type != CRDTTypeCounter || string(snapshot.Value) != "11" {
		t.Fatalf("after compaction: %s %s, want counter 11", snapshot.Type, snapshot.Value)
	}
	// The merged state read is untouched by the trim.
	if got := crdtStateValue(t, s, "doc"); got != "11" {
		t.Fatalf("state after compaction = %s, want 11", got)
	}

	// A repeat compaction trims nothing and reports the same snapshot.
	again, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Type != snapshot.Type || string(again.Value) != string(snapshot.Value) ||
		again.Operations != snapshot.Operations || again.Tombstones != snapshot.Tombstones {
		t.Fatalf("re-compaction = %+v, want %+v", again, snapshot)
	}

	// The regression gate still uses the retained per-device maximum.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a3", DeviceID: "dev-1", Value: rawInt(7)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("regression after compaction err = %v, want *ErrCRDTConflict", err)
	}
	// The retained op stays idempotent; a new advance still lands.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)},
	})
	if err != nil || results[0].Created {
		t.Fatalf("retained op replay = %v %+v, want idempotent", err, results)
	}
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a4", DeviceID: "dev-1", Value: rawInt(10)})
	if got := crdtStateValue(t, s, "doc"); got != "13" {
		t.Fatalf("state after new advance = %s, want 13", got)
	}
}

func TestCompactGSetTrimsCoveredOps(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g1", DeviceID: "dev-1", Elements: []string{"apple", "banana"}})
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-1", Elements: []string{"banana", "cherry"}})
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g3", DeviceID: "dev-1", Elements: []string{"apple"}})

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 3 {
		t.Fatalf("before compaction: operations = %d, want 3", before.Operations)
	}

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// g1 covers apple+banana, g2 adds cherry; g3 contributes nothing new.
	if snapshot.Operations != 2 || snapshot.Tombstones != 0 {
		t.Fatalf("after compaction: operations=%d tombstones=%d, want 2/0", snapshot.Operations, snapshot.Tombstones)
	}
	if string(snapshot.Value) != `["apple","banana","cherry"]` {
		t.Fatalf("merged value = %s", snapshot.Value)
	}
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana","cherry"]` {
		t.Fatalf("state after compaction = %s", got)
	}

	// New elements still merge after compaction.
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g4", DeviceID: "dev-1", Elements: []string{"date"}})
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana","cherry","date"]` {
		t.Fatalf("state after new element = %s", got)
	}
}

func TestCompactRegisterKeepsPerDeviceLatest(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"first"`)})
	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r2", DeviceID: "dev-1", Version: 2, Value: json.RawMessage(`"second"`)})
	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r3", DeviceID: "dev-2", Version: 1, Value: json.RawMessage(`"other"`)})

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 3 {
		t.Fatalf("before compaction: operations = %d, want 3", before.Operations)
	}

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 2 || snapshot.Tombstones != 0 {
		t.Fatalf("after compaction: operations=%d tombstones=%d, want 2/0", snapshot.Operations, snapshot.Tombstones)
	}
	if string(snapshot.Value) != `"second"` {
		t.Fatalf("winner after compaction = %s, want \"second\"", snapshot.Value)
	}

	// The version gate still derives from the retained per-device maxima.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		{ID: "r4", DeviceID: "dev-1", Version: 2, Value: json.RawMessage(`"stall"`)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("stalled version after compaction err = %v, want *ErrCRDTConflict", err)
	}
	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r5", DeviceID: "dev-1", Version: 3, Value: json.RawMessage(`"third"`)})
	if got := crdtStateValue(t, s, "doc"); got != `"third"` {
		t.Fatalf("state after new version = %s, want \"third\"", got)
	}
}

func TestCompactORSetDropsDeadTagsAndTombstones(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o2", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "banana"})
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o3", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"})

	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if before.Operations != 2 || before.Tombstones != 1 {
		t.Fatalf("before compaction: operations=%d tombstones=%d, want 2/1", before.Operations, before.Tombstones)
	}

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || snapshot.Tombstones != 0 {
		t.Fatalf("after compaction: operations=%d tombstones=%d, want 1/0", snapshot.Operations, snapshot.Tombstones)
	}
	if string(snapshot.Value) != `["banana"]` {
		t.Fatalf("merged value = %s, want [\"banana\"]", snapshot.Value)
	}

	// Replaying the compacted remove stays idempotent and must not tombstone
	// a tag added after the original remove.
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o4", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	results, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		{ID: "o3", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"},
	})
	if err != nil || results[0].Created {
		t.Fatalf("remove replay after compaction = %v %+v, want idempotent", err, results)
	}
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana"]` {
		t.Fatalf("state after replayed remove = %s, want [\"apple\",\"banana\"]", got)
	}
	// Replaying the compacted add stays idempotent and resurrects nothing.
	results, err = s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"},
	})
	if err != nil || results[0].Created {
		t.Fatalf("add replay after compaction = %v %+v, want idempotent", err, results)
	}
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana"]` {
		t.Fatalf("state after replayed add = %s", got)
	}
}

func TestCompactORSetFullyRemovedSet(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o1", DeviceID: "dev-1", Action: CRDTORSetAdd, Element: "apple"})
	submitCRDT(t, s, "doc", CRDTTypeORSet, CRDTOp{ID: "o2", DeviceID: "dev-1", Action: CRDTORSetRemove, Element: "apple"})

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 0 || snapshot.Tombstones != 0 {
		t.Fatalf("after compaction: operations=%d tombstones=%d, want 0/0", snapshot.Operations, snapshot.Tombstones)
	}
	if string(snapshot.Value) != `[]` {
		t.Fatalf("merged value = %s, want []", snapshot.Value)
	}
}

func TestCompactGateAndMissingState(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})

	// An unregistered device is rejected before any CRDT content is observed.
	if _, err := s.CompactCRDT("doc", "ghost"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unregistered device err = %v, want ErrDeviceNotFound", err)
	}
	// A revoked device is rejected and changes nothing.
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactCRDT("doc", "dev-2"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked device err = %v, want ErrPermissionDenied", err)
	}
	// A document with no CRDT operation has nothing to compact.
	if _, err := s.CompactCRDT("never", "dev-1"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("empty document err = %v, want ErrCRDTNotFound", err)
	}
	if _, err := s.GetCRDTSnapshot("never"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("empty snapshot err = %v, want ErrCRDTNotFound", err)
	}

	// None of the rejections trimmed anything.
	snapshot, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || string(snapshot.Value) != "5" {
		t.Fatalf("snapshot after rejections = %+v", snapshot)
	}
}

func TestCompactLeavesChangeLogAndSubscribersAlone(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// A change-log document shares the document id; compaction must not
	// allocate cursors or append changes to it.
	if _, err := s.PostChanges("doc", []Change{{ID: "c1", DeviceID: "dev-1", Payload: json.RawMessage(`{"k":1}`)}}); err != nil {
		t.Fatal(err)
	}
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})

	sub, unregister := s.AddCRDTSubscription("doc", "dev-1")
	defer unregister()

	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// No notification: the merged value never moved.
	if queued := sub.Drain(); len(queued) != 0 {
		t.Fatalf("compaction queued %d states, want none", len(queued))
	}
	select {
	case <-sub.Wake():
		t.Fatal("compaction woke a subscriber")
	default:
	}

	// No cursor and no change record: the log still holds exactly c1 at 1.
	changes, next, err := s.ListChanges("doc", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].ID != "c1" || next != 1 {
		t.Fatalf("changes after compaction = %+v next=%d", changes, next)
	}
}

func TestCompactConcurrentRunsSerialize(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})

	var wg sync.WaitGroup
	snapshots := make([]CRDTSnapshot, 4)
	for i := range snapshots {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			device := "dev-1"
			if i%2 == 1 {
				device = "dev-2"
			}
			snapshot, err := s.CompactCRDT("doc", device)
			if err != nil {
				t.Errorf("compact %d: %v", i, err)
				return
			}
			snapshots[i] = snapshot
		}(i)
	}
	wg.Wait()
	for i, snapshot := range snapshots {
		if snapshot.Operations != 1 || string(snapshot.Value) != "8" {
			t.Fatalf("snapshot %d = %+v, want one operation merging to 8", i, snapshot)
		}
	}
}

func TestCompactPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1")
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

	snapshot, err := s2.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Operations != 1 || snapshot.Tombstones != 0 || string(snapshot.Value) != "8" {
		t.Fatalf("snapshot after reopen = %+v", snapshot)
	}
	// The regression gate survives the restart exactly as before.
	_, err = s2.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a3", DeviceID: "dev-1", Value: rawInt(7)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("regression after reopen err = %v, want *ErrCRDTConflict", err)
	}
	// A retained op stays idempotent after the restart.
	results, err := s2.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)},
	})
	if err != nil || results[0].Created {
		t.Fatalf("retained op replay after reopen = %v %+v, want idempotent", err, results)
	}
}

// wantConflict submits ops and asserts an *ErrCRDTConflict that names id.
func wantConflict(t *testing.T, s *Store, doc, typ string, id string, ops ...CRDTOp) {
	t.Helper()
	_, err := s.SubmitCRDTOps(doc, typ, ops)
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("submit %s after compaction err = %v, want *ErrCRDTConflict", id, err)
	}
	if conflict.ID != id {
		t.Fatalf("conflict id = %q, want %q", conflict.ID, id)
	}
}

// replay submits ops and asserts the first result is an idempotent no-op.
func replay(t *testing.T, s *Store, doc, typ string, ops ...CRDTOp) {
	t.Helper()
	results, err := s.SubmitCRDTOps(doc, typ, ops)
	if err != nil {
		t.Fatalf("replay after compaction: %v", err)
	}
	if len(results) != len(ops) {
		t.Fatalf("replay results = %d, want %d", len(results), len(ops))
	}
	for i, result := range results {
		if result.ID != ops[i].ID || result.Created {
			t.Fatalf("replay result %d = %+v, want %s created=false", i, result, ops[i].ID)
		}
	}
}

func TestCompactCounterTrimmedIDStillDecides(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})

	snapshot, err := s.CompactCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// a1 (value 5) is trimmed; a2 (value 8) is retained.
	if snapshot.Operations != 1 || string(snapshot.Value) != "8" {
		t.Fatalf("snapshot = %+v, want one op merging to 8", snapshot)
	}

	// Same device and value: the trimmed id still replays idempotently with
	// its original result (created=false), and creates nothing.
	replay(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	if got := crdtStateValue(t, s, "doc"); got != "8" {
		t.Fatalf("state after trimmed replay = %s, want 8", got)
	}
	if after, _ := s.GetCRDTSnapshot("doc"); after.Operations != 1 {
		t.Fatalf("operations after trimmed replay = %d, want 1", after.Operations)
	}

	// Different value: 409 and nothing changes, even though the row is gone.
	wantConflict(t, s, "doc", CRDTTypeCounter, "a1", CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(9)})
	// Different device: likewise 409 with the state untouched.
	wantConflict(t, s, "doc", CRDTTypeCounter, "a1", CRDTOp{ID: "a1", DeviceID: "dev-2", Value: rawInt(5)})
	if got := crdtStateValue(t, s, "doc"); got != "8" {
		t.Fatalf("state after trimmed conflicts = %s, want 8", got)
	}
	if after, _ := s.GetCRDTSnapshot("doc"); after.Operations != 1 {
		t.Fatalf("operations after trimmed conflicts = %d, want 1", after.Operations)
	}

	// A genuinely new id still follows the normal path: a regression is
	// rejected by the retained per-device maximum, an advance is accepted.
	wantConflict(t, s, "doc", CRDTTypeCounter, "a3", CRDTOp{ID: "a3", DeviceID: "dev-1", Value: rawInt(7)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a4", DeviceID: "dev-1", Value: rawInt(10)})
	if got := crdtStateValue(t, s, "doc"); got != "10" {
		t.Fatalf("state after new advance = %s, want 10", got)
	}
}

func TestCompactGSetTrimmedIDStillDecides(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g1", DeviceID: "dev-1", Elements: []string{"apple", "banana"}})
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g2", DeviceID: "dev-1", Elements: []string{"cherry"}})
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g3", DeviceID: "dev-1", Elements: []string{"apple"}})

	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	// g3 is fully covered by g1 and trimmed.

	// Same elements, different order and duplicated: the element-set content
	// compares equal, so the trimmed id replays idempotently.
	replay(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g3", DeviceID: "dev-1", Elements: []string{"apple", "apple"}})
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana","cherry"]` {
		t.Fatalf("state after trimmed replay = %s", got)
	}

	// A different element set conflicts; so does a different device.
	wantConflict(t, s, "doc", CRDTTypeGSet, "g3", CRDTOp{ID: "g3", DeviceID: "dev-1", Elements: []string{"date"}})
	wantConflict(t, s, "doc", CRDTTypeGSet, "g3", CRDTOp{ID: "g3", DeviceID: "dev-2", Elements: []string{"apple"}})
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana","cherry"]` {
		t.Fatalf("state after trimmed conflicts = %s", got)
	}

	// A new element still merges through the normal path.
	submitCRDT(t, s, "doc", CRDTTypeGSet, CRDTOp{ID: "g4", DeviceID: "dev-1", Elements: []string{"date"}})
	if got := crdtStateValue(t, s, "doc"); got != `["apple","banana","cherry","date"]` {
		t.Fatalf("state after new element = %s", got)
	}
}

func TestCompactRegisterTrimmedIDStillDecides(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`{"b":2,"a":1}`)})
	submitCRDT(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r2", DeviceID: "dev-1", Version: 2, Value: json.RawMessage(`"second"`)})

	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	// r1 is the trimmed older version of dev-1; r2 is the retained latest.

	// The trimmed id replays idempotently even though version 1 is now below
	// the device's retained maximum — the identity is decided before the
	// monotonicity gate. Object key order (and 1 vs 1.0-style JSON equality)
	// is irrelevant to the comparison.
	replay(t, s, "doc", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`{"a":1,"b":2}`)})
	if got := crdtStateValue(t, s, "doc"); got != `"second"` {
		t.Fatalf("state after trimmed replay = %s, want \"second\"", got)
	}

	// Different version, different value or different device all conflict and
	// leave the winning value and the retained operation count untouched.
	wantConflict(t, s, "doc", CRDTTypeRegister, "r1", CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"other"`)})
	wantConflict(t, s, "doc", CRDTTypeRegister, "r1", CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 3, Value: json.RawMessage(`{"b":2,"a":1}`)})
	wantConflict(t, s, "doc", CRDTTypeRegister, "r1", CRDTOp{ID: "r1", DeviceID: "dev-2", Version: 1, Value: json.RawMessage(`{"b":2,"a":1}`)})
	if got := crdtStateValue(t, s, "doc"); got != `"second"` {
		t.Fatalf("state after trimmed conflicts = %s, want \"second\"", got)
	}
	if after, _ := s.GetCRDTSnapshot("doc"); after.Operations != 1 {
		t.Fatalf("operations after trimmed replays = %d, want 1", after.Operations)
	}
}

func TestCompactTrimmedIdentitiesPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1")
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", CRDTTypeCounter, CRDTOp{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	submitCRDT(t, s, "doc2", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"first"`)})
	submitCRDT(t, s, "doc2", CRDTTypeRegister, CRDTOp{ID: "r2", DeviceID: "dev-1", Version: 2, Value: json.RawMessage(`"second"`)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompactCRDT("doc2", "dev-1"); err != nil {
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

	// The trimmed ids keep deciding identically after the restart.
	replay(t, s2, "doc", CRDTTypeCounter, CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	wantConflict(t, s2, "doc", CRDTTypeCounter, "a1", CRDTOp{ID: "a1", DeviceID: "dev-1", Value: rawInt(6)})
	replay(t, s2, "doc2", CRDTTypeRegister, CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"first"`)})
	wantConflict(t, s2, "doc2", CRDTTypeRegister, "r1", CRDTOp{ID: "r1", DeviceID: "dev-1", Version: 1, Value: json.RawMessage(`"other"`)})
	if got := crdtStateValue(t, s2, "doc"); got != "8" {
		t.Fatalf("counter state after reopen = %s, want 8", got)
	}
	if got := crdtStateValue(t, s2, "doc2"); got != `"second"` {
		t.Fatalf("register state after reopen = %s, want \"second\"", got)
	}
}
