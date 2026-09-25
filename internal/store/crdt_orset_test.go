package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func orsetOp(id, device, action, element string) CRDTOp {
	return CRDTOp{ID: id, DeviceID: device, Action: action, Element: element}
}

func submitORSet(t *testing.T, s *Store, doc string, ops ...CRDTOp) []CRDTResult {
	t.Helper()
	results, err := s.SubmitCRDTOps(doc, CRDTTypeORSet, ops)
	if err != nil {
		t.Fatalf("submit %v: %v", ops, err)
	}
	return results
}

func orsetState(t *testing.T, s *Store, doc string) []string {
	t.Helper()
	state, err := s.GetCRDTState(doc)
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeORSet {
		t.Fatalf("type = %q, want orset", state.Type)
	}
	var elements []string
	if err := json.Unmarshal(state.Value, &elements); err != nil {
		t.Fatalf("value %s is not a string array: %v", state.Value, err)
	}
	return elements
}

func equalElements(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("elements = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("elements = %v, want %v", got, want)
		}
	}
}

func TestCRDTORSetAddAndRemove(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "banana"))
	submitORSet(t, s, "doc", orsetOp("a2", "dev-1", CRDTORSetAdd, "apple"))
	submitORSet(t, s, "doc", orsetOp("b1", "dev-2", CRDTORSetAdd, "cherry"))
	equalElements(t, orsetState(t, s, "doc"), "apple", "banana", "cherry")

	// A remove deletes exactly the adds it observes.
	submitORSet(t, s, "doc", orsetOp("r1", "dev-2", CRDTORSetRemove, "banana"))
	equalElements(t, orsetState(t, s, "doc"), "apple", "cherry")

	// Removing the last element leaves an empty set, rendered as [].
	submitORSet(t, s, "doc",
		orsetOp("r2", "dev-1", CRDTORSetRemove, "apple"),
		orsetOp("r3", "dev-1", CRDTORSetRemove, "cherry"),
	)
	equalElements(t, orsetState(t, s, "doc"))
	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != `[]` {
		t.Fatalf("empty state = %s, want []", state.Value)
	}
}

func TestCRDTORSetRemoveOnlyObservedAdds(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))
	submitORSet(t, s, "doc", orsetOp("r1", "dev-2", CRDTORSetRemove, "x"))
	equalElements(t, orsetState(t, s, "doc"))

	// An add landing after the remove carries a fresh tag and survives it.
	submitORSet(t, s, "doc", orsetOp("a2", "dev-1", CRDTORSetAdd, "x"))
	equalElements(t, orsetState(t, s, "doc"), "x")

	// A remove only tombstones the adds accepted before it: r2 observes a3
	// and deletes it, but a4 lands afterwards with a fresh tag and survives.
	submitORSet(t, s, "doc", orsetOp("a3", "dev-2", CRDTORSetAdd, "y"))
	submitORSet(t, s, "doc", orsetOp("r2", "dev-1", CRDTORSetRemove, "y"))
	submitORSet(t, s, "doc", orsetOp("a4", "dev-2", CRDTORSetAdd, "y"))
	equalElements(t, orsetState(t, s, "doc"), "x", "y")
}

func TestCRDTORSetRemoveNeverAddedIsNoOp(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// Removing an element that was never added is accepted and changes
	// nothing — no error, no state movement.
	results := submitORSet(t, s, "doc", orsetOp("r1", "dev-1", CRDTORSetRemove, "ghost"))
	if !results[0].Created {
		t.Fatal("a remove of a never-added element is still a created op")
	}
	equalElements(t, orsetState(t, s, "doc"))

	// The no-op remove must not have notified anyone: the merged set never
	// changed. A later add of the same element is unaffected by it.
	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "ghost"))
	equalElements(t, orsetState(t, s, "doc"), "ghost")
}

func TestCRDTORSetIdempotencyAndConflict(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))

	// Identical repeat is idempotent.
	results := submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))
	if results[0].Created {
		t.Fatal("identical repeat must report created=false")
	}

	var conflict *ErrCRDTConflict
	// Same id, different element -> 409.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetOp("a1", "dev-1", CRDTORSetAdd, "y"),
	}); !errors.As(err, &conflict) || conflict.ID != "a1" {
		t.Fatalf("different element err = %v, want *ErrCRDTConflict for a1", err)
	}
	// Same id, different action -> 409.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetOp("a1", "dev-1", CRDTORSetRemove, "x"),
	}); !errors.As(err, &conflict) {
		t.Fatalf("different action err = %v, want *ErrCRDTConflict", err)
	}
	// Same id, different device -> 409.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetOp("a1", "dev-2", CRDTORSetAdd, "x"),
	}); !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v, want *ErrCRDTConflict", err)
	}
	equalElements(t, orsetState(t, s, "doc"), "x")

	// A replayed remove is idempotent and must not tombstone adds accepted
	// after its first acceptance.
	submitORSet(t, s, "doc", orsetOp("r1", "dev-1", CRDTORSetRemove, "x"))
	submitORSet(t, s, "doc", orsetOp("a2", "dev-1", CRDTORSetAdd, "x"))
	results = submitORSet(t, s, "doc", orsetOp("r1", "dev-1", CRDTORSetRemove, "x"))
	if results[0].Created {
		t.Fatal("replayed remove must report created=false")
	}
	equalElements(t, orsetState(t, s, "doc"), "x")
}

func TestCRDTORSetMergeIsOrderIndependent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	// Adds commute freely: the same add operations accepted in two different
	// arrival orders converge to the same set, and idempotent replays mixed
	// into one order change nothing. The merge is a deterministic function of
	// the accepted operations, so every reader converges to the same value.
	submitORSet(t, s, "doc-a",
		orsetOp("a1", "dev-1", CRDTORSetAdd, "x"),
		orsetOp("a2", "dev-2", CRDTORSetAdd, "y"),
		orsetOp("a3", "dev-1", CRDTORSetAdd, "z"),
	)
	submitORSet(t, s, "doc-b",
		orsetOp("a3", "dev-1", CRDTORSetAdd, "z"),
		orsetOp("a1", "dev-1", CRDTORSetAdd, "x"),
		orsetOp("a3", "dev-1", CRDTORSetAdd, "z"), // idempotent replay
		orsetOp("a2", "dev-2", CRDTORSetAdd, "y"),
		orsetOp("a1", "dev-1", CRDTORSetAdd, "x"), // idempotent replay
	)
	equalElements(t, orsetState(t, s, "doc-a"), "x", "y", "z")
	equalElements(t, orsetState(t, s, "doc-b"), "x", "y", "z")
}

func TestCRDTORSetTypeFixation(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))

	var conflict *ErrCRDTConflict
	// Declaring any other type afterwards is a 409.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); !errors.As(err, &conflict) {
		t.Fatalf("counter after orset err = %v", err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	}); !errors.As(err, &conflict) {
		t.Fatalf("gset after orset err = %v", err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `1`),
	}); !errors.As(err, &conflict) {
		t.Fatalf("register after orset err = %v", err)
	}
	// And an orset declared after another type is likewise a 409.
	if _, err := s.SubmitCRDTOps("other", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("other", CRDTTypeORSet, []CRDTOp{
		orsetOp("o1", "dev-1", CRDTORSetAdd, "x"),
	}); !errors.As(err, &conflict) {
		t.Fatalf("orset after gset err = %v", err)
	}
	equalElements(t, orsetState(t, s, "doc"), "x")
}

func TestCRDTORSetNotificationOnlyOnRealChange(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	_, sub, unregister := openSub(t, s, "doc", "dev-1")
	defer unregister()

	// First add: the set changes, one state is queued.
	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))
	// Adding an already-present element with a new id: accepted, no change.
	submitORSet(t, s, "doc", orsetOp("a2", "dev-1", CRDTORSetAdd, "x"))
	// Removing a never-added element: accepted, no change.
	submitORSet(t, s, "doc", orsetOp("r1", "dev-1", CRDTORSetRemove, "ghost"))
	queued := sub.Drain()
	if len(queued) != 1 || string(queued[0].Value) != `["x"]` {
		t.Fatalf("queued = %v, want exactly one [x]", queued)
	}

	// A remove that empties the set pushes the empty set once.
	submitORSet(t, s, "doc", orsetOp("r2", "dev-1", CRDTORSetRemove, "x"))
	// Repeating the remove idempotently queues nothing more.
	submitORSet(t, s, "doc", orsetOp("r2", "dev-1", CRDTORSetRemove, "x"))
	queued = sub.Drain()
	if len(queued) != 1 || string(queued[0].Value) != `[]` {
		t.Fatalf("queued = %v, want exactly one []", queued)
	}
}

func TestCRDTORSetConcurrentAddsConverge(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	devices := make([]string, 8)
	for i := range devices {
		devices[i] = fmt.Sprintf("dev-%d", i)
	}
	registerDevices(t, s, devices...)

	// Every device adds the same elements concurrently; each add is an
	// independent tag, so the merge is exactly the element set.
	const elements = 10
	done := make(chan struct{})
	for _, d := range devices {
		go func(device string) {
			defer func() { done <- struct{}{} }()
			for e := 0; e < elements; e++ {
				if _, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
					orsetOp(fmt.Sprintf("%s-%d", device, e), device, CRDTORSetAdd, fmt.Sprintf("e%02d", e)),
				}); err != nil {
					t.Errorf("submit %s: %v", device, err)
					return
				}
			}
		}(d)
	}
	for range devices {
		<-done
	}

	want := make([]string, elements)
	for e := 0; e < elements; e++ {
		want[e] = fmt.Sprintf("e%02d", e)
	}
	equalElements(t, orsetState(t, s, "doc"), want...)
}

func TestCRDTORSetPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1", "dev-2")
	submitORSet(t, s, "doc",
		orsetOp("a1", "dev-1", CRDTORSetAdd, "b"),
		orsetOp("a2", "dev-1", CRDTORSetAdd, "a"),
	)
	submitORSet(t, s, "doc", orsetOp("r1", "dev-2", CRDTORSetRemove, "b"))
	submitORSet(t, s, "doc", orsetOp("a3", "dev-2", CRDTORSetAdd, "b"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// The merged state survives: the tombstoned add stays gone, the later
	// add stays live.
	equalElements(t, orsetState(t, s2, "doc"), "a", "b")

	// Idempotency decisions survive the restart.
	results, err := s2.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetOp("r1", "dev-2", CRDTORSetRemove, "b"),
	})
	if err != nil || results[0].Created {
		t.Fatalf("idempotent replay after restart = created:%v err:%v", results[0].Created, err)
	}
	// The replayed remove did not tombstone the later add.
	equalElements(t, orsetState(t, s2, "doc"), "a", "b")

	// Conflict decisions survive as well.
	var conflict *ErrCRDTConflict
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetOp("a1", "dev-1", CRDTORSetAdd, "different"),
	}); !errors.As(err, &conflict) {
		t.Fatalf("conflict after restart err = %v", err)
	}
	// Type fixation survives.
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); !errors.As(err, &conflict) {
		t.Fatalf("type change after restart err = %v", err)
	}
}

func TestCRDTORSetIsIndependentOfChangeLog(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitORSet(t, s, "doc", orsetOp("a1", "dev-1", CRDTORSetAdd, "x"))

	// The change log is untouched: no rows, no cursor movement.
	known, err := s.DocumentExists("doc")
	if err != nil {
		t.Fatal(err)
	}
	if known {
		t.Fatal("orset ops must not create change-log rows")
	}
	changes, nextCursor, err := s.ListChanges("doc", 0, 100)
	if err != nil || len(changes) != 0 || nextCursor != 0 {
		t.Fatalf("change log = %v cursor %d err %v", changes, nextCursor, err)
	}
}
