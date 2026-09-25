package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func orsetAdd(id, device string, elements ...string) CRDTOp {
	return CRDTOp{ID: id, DeviceID: device, Action: ORSetActionAdd, Elements: elements}
}

func orsetRemove(id, device string, elements ...string) CRDTOp {
	return CRDTOp{ID: id, DeviceID: device, Action: ORSetActionRemove, Elements: elements}
}

func orsetState(t *testing.T, s *Store, doc string) []string {
	t.Helper()
	state, err := s.GetCRDTState(doc)
	if err != nil {
		t.Fatalf("GetCRDTState: %v", err)
	}
	if state.Type != CRDTTypeORSet {
		t.Fatalf("type = %q, want orset", state.Type)
	}
	var elements []string
	if err := json.Unmarshal(state.Value, &elements); err != nil {
		t.Fatalf("orset value %s is not a string array: %v", state.Value, err)
	}
	return elements
}

func mustSubmitORSet(t *testing.T, s *Store, doc string, ops ...CRDTOp) []CRDTResult {
	t.Helper()
	results, err := s.SubmitCRDTOps(doc, CRDTTypeORSet, ops)
	if err != nil {
		t.Fatalf("submit %v: %v", ops, err)
	}
	return results
}

// Adds union into a sorted present-set; a remove drops the elements it
// observes; a remove of a never-added element is an accepted no-op.
func TestCRDTORSetAddRemoveSortedAndRemoveUnseen(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "banana", "apple"))
	mustSubmitORSet(t, s, "doc", orsetAdd("a2", "dev-2", "cherry", "apple"))
	if got := orsetState(t, s, "doc"); len(got) != 3 ||
		got[0] != "apple" || got[1] != "banana" || got[2] != "cherry" {
		t.Fatalf("elements = %v, want [apple banana cherry]", got)
	}

	// Removing an element never added neither errors nor changes the set.
	r := mustSubmitORSet(t, s, "doc", orsetRemove("r0", "dev-1", "ghost"))
	if !r[0].Created {
		t.Fatal("a new remove id must be created even when it observes nothing")
	}
	if got := orsetState(t, s, "doc"); len(got) != 3 {
		t.Fatalf("remove of unseen element moved the set: %v", got)
	}

	// Removing a present element drops just that one.
	mustSubmitORSet(t, s, "doc", orsetRemove("r1", "dev-1", "banana"))
	if got := orsetState(t, s, "doc"); len(got) != 2 || got[0] != "apple" || got[1] != "cherry" {
		t.Fatalf("elements after remove = %v, want [apple cherry]", got)
	}
}

// A remove tombstones only the adds present when it is first committed; a
// later add mints fresh tags and restores the element. The earlier remove
// never reaches the add it had not observed.
func TestCRDTORSetRemoveDoesNotAffectLaterAdds(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "x"))
	mustSubmitORSet(t, s, "doc", orsetRemove("r1", "dev-2", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 0 {
		t.Fatalf("elements after remove = %v, want []", got)
	}

	// A fresh add (concurrent with, or simply later than, the remove) is not
	// observed by r1 and brings the element back.
	mustSubmitORSet(t, s, "doc", orsetAdd("a2", "dev-1", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("elements after re-add = %v, want [x]", got)
	}

	// The new tag can itself be removed, and a further add returns again.
	mustSubmitORSet(t, s, "doc", orsetRemove("r2", "dev-2", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 0 {
		t.Fatalf("elements after second remove = %v, want []", got)
	}
	mustSubmitORSet(t, s, "doc", orsetAdd("a3", "dev-1", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("elements after third add = %v, want [x]", got)
	}
}

// A remove observes every tag currently contributing an element, so an
// element held by two live adds leaves only when both are observed.
func TestCRDTORSetRemoveObservesAllCurrentTags(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	mustSubmitORSet(t, s, "doc",
		orsetAdd("a1", "dev-1", "x"),
		orsetAdd("a2", "dev-2", "x"),
	)
	if got := orsetState(t, s, "doc"); len(got) != 1 {
		t.Fatalf("elements = %v, want [x]", got)
	}
	// One remove committed while both tags are live observes both.
	mustSubmitORSet(t, s, "doc", orsetRemove("r1", "dev-1", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 0 {
		t.Fatalf("elements = %v, want [] after observing both tags", got)
	}
}

// Repeated ids are idempotent only with the same device, action and element
// set (order irrelevant); any mismatch is a 409 that moves nothing.
func TestCRDTORSetIdempotencyAndConflict(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	add := orsetAdd("same-id", "dev-1", "banana", "apple")
	mustSubmitORSet(t, s, "doc", add)

	// Identical content in a different element order is idempotent.
	r, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetAdd("same-id", "dev-1", "apple", "banana"),
	})
	if err != nil || r[0].Created {
		t.Fatalf("identical repeat = created:%v err:%v", r[0].Created, err)
	}

	var conflict *ErrCRDTConflict
	// Same id, different elements -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetAdd("same-id", "dev-1", "apple", "cherry"),
	})
	if !errors.As(err, &conflict) || conflict.ID != "same-id" {
		t.Fatalf("different elements err = %v", err)
	}
	// Same id, add vs remove -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetRemove("same-id", "dev-1", "apple", "banana"),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different action err = %v", err)
	}
	// Same id, different device -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetAdd("same-id", "dev-2", "banana", "apple"),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v", err)
	}

	if got := orsetState(t, s, "doc"); len(got) != 2 || got[0] != "apple" || got[1] != "banana" {
		t.Fatalf("state after conflicts = %v, want [apple banana]", got)
	}
}

// A remove's observation is fixed on its first commit: re-posting it after a
// fresh add is idempotent and must not tombstone the tags it never observed.
func TestCRDTORSetRemoveReplayDoesNotReobserve(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "x"))
	remove := orsetRemove("r1", "dev-2", "x")
	mustSubmitORSet(t, s, "doc", remove)
	// A fresh add restores x.
	mustSubmitORSet(t, s, "doc", orsetAdd("a2", "dev-1", "x"))
	if got := orsetState(t, s, "doc"); len(got) != 1 {
		t.Fatalf("elements = %v, want [x]", got)
	}

	// Replaying the identical remove is idempotent and observes nothing new:
	// the fresh tag a2 keeps x present.
	r, err := s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{remove})
	if err != nil || r[0].Created {
		t.Fatalf("identical remove replay = created:%v err:%v", r[0].Created, err)
	}
	if got := orsetState(t, s, "doc"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("elements after replay = %v, want [x]", got)
	}
}

// Re-adding an element already present mints a tag but does not move
// membership, so the batch reports success without changing the merge.
func TestCRDTORSetReAddPresentElementIsNoOp(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "x"))
	r := mustSubmitORSet(t, s, "doc", orsetAdd("a2", "dev-1", "x"))
	if !r[0].Created {
		t.Fatal("a new add id is still created")
	}
	if got := orsetState(t, s, "doc"); len(got) != 1 || got[0] != "x" {
		t.Fatalf("elements = %v, want [x]", got)
	}
}

// A wholly-removed set reads back as an empty JSON array, not null.
func TestCRDTORSetEmptyStateIsEmptyArray(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "only"))
	mustSubmitORSet(t, s, "doc", orsetRemove("r1", "dev-1", "only"))
	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != "[]" {
		t.Fatalf("empty orset value = %s, want []", state.Value)
	}
}

// Type fixation includes orset in both directions.
func TestCRDTORSetTypeFixation(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	mustSubmitORSet(t, s, "doc", orsetAdd("a1", "dev-1", "x"))
	var conflict *ErrCRDTConflict
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); !errors.As(err, &conflict) {
		t.Fatalf("counter after orset err = %v", err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"y"}},
	}); !errors.As(err, &conflict) {
		t.Fatalf("gset after orset err = %v", err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `1`),
	}); !errors.As(err, &conflict) {
		t.Fatalf("register after orset err = %v", err)
	}

	// An orset declared after a fixed counter on another document is a 409.
	if _, err := s.SubmitCRDTOps("other", CRDTTypeCounter, counterOps("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("other", CRDTTypeORSet, []CRDTOp{
		orsetAdd("a1", "dev-1", "x"),
	}); !errors.As(err, &conflict) {
		t.Fatalf("orset after counter err = %v", err)
	}
}

// Operations, the remove decisions and the merged result are durable together:
// after a reopen the membership, idempotency and conflict decisions all hold.
func TestCRDTORSetPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "orset.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1", "dev-2")
	mustSubmitORSet(t, s, "doc",
		orsetAdd("a1", "dev-1", "b", "a"),
		orsetAdd("a2", "dev-2", "c"),
	)
	mustSubmitORSet(t, s, "doc", orsetRemove("r1", "dev-2", "a"))
	mustSubmitORSet(t, s, "doc", orsetAdd("a3", "dev-1", "a")) // observed-remove brings "a" back
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	state, err := s2.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeORSet || string(state.Value) != `["a","b","c"]` {
		t.Fatalf("state after restart = %s %s", state.Type, state.Value)
	}
	// Idempotent add and remove replays stay non-creating.
	for _, op := range []CRDTOp{
		orsetAdd("a1", "dev-1", "b", "a"),
		orsetRemove("r1", "dev-2", "a"),
	} {
		r, err := s2.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{op})
		if err != nil || r[0].Created {
			t.Fatalf("replay %v = created:%v err:%v", op.ID, r[0].Created, err)
		}
	}
	if got := orsetState(t, s2, "doc"); len(got) != 3 ||
		got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("membership changed on replay: %v", got)
	}
	// A conflicting re-post is still a 409 after restart.
	var conflict *ErrCRDTConflict
	_, err = s2.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
		orsetAdd("a1", "dev-2", "b", "a"),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("cross-device re-post after restart err = %v", err)
	}
	// Type fixation survives as well.
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); !errors.As(err, &conflict) {
		t.Fatalf("type change after restart err = %v", err)
	}
}

// Independent devices' adds union even under concurrency; every accepted add
// contributes a distinct tag and the derived membership is stable regardless
// of arrival order.
func TestCRDTORSetConcurrentAddsConverge(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	devices := make([]string, 6)
	for i := range devices {
		devices[i] = fmt.Sprintf("dev-%d", i)
	}
	registerDevices(t, s, devices...)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, d := range devices {
		wg.Add(1)
		go func(i int, device string) {
			defer wg.Done()
			<-start
			_, _ = s.SubmitCRDTOps("doc", CRDTTypeORSet, []CRDTOp{
				orsetAdd(fmt.Sprintf("add-%d", i), device, fmt.Sprintf("elem-%d", i)),
			})
		}(i, d)
	}
	close(start)
	wg.Wait()

	if got := orsetState(t, s, "doc"); len(got) != len(devices) {
		t.Fatalf("elements = %v, want %d distinct", got, len(devices))
	}
	// Re-deriving the merge never changes the answer.
	if got := orsetState(t, s, "doc"); len(got) != len(devices) {
		t.Fatalf("second read = %v", got)
	}
}
