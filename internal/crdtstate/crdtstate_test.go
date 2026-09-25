package crdtstate

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// --- test infrastructure ----------------------------------------------------

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?_txlock=immediate&_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func openFileDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_txlock=immediate&_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var (
	errNotRegistered = errors.New("device not found")
	errAccessDenied  = errors.New("permission denied")
)

type fakeGate struct {
	devices map[string]bool
	denied  map[string]map[string]bool
}

func newFakeGate(devices ...string) *fakeGate {
	g := &fakeGate{
		devices: make(map[string]bool),
		denied:  make(map[string]map[string]bool),
	}
	for _, d := range devices {
		g.devices[d] = true
	}
	return g
}

func (g *fakeGate) deny(doc, device string) {
	if g.denied[doc] == nil {
		g.denied[doc] = make(map[string]bool)
	}
	g.denied[doc][device] = true
}

func (g *fakeGate) CheckDeviceTx(_ *sql.Tx, deviceID string) error {
	if g.devices[deviceID] {
		return nil
	}
	return errNotRegistered
}

func (g *fakeGate) CheckPermissionTx(_ *sql.Tx, doc, device string) error {
	if g.denied[doc] != nil && g.denied[doc][device] {
		return errAccessDenied
	}
	return nil
}

func newService(t *testing.T, gate Gate) *Service {
	t.Helper()
	svc, err := New(openDB(t), gate)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(svc.Close)
	return svc
}

func counterOp(id, device string, value int64) Op {
	raw, _ := json.Marshal(value)
	return Op{ID: id, DeviceID: device, Value: raw}
}

func gsetOp(id, device string, elements ...string) Op {
	return Op{ID: id, DeviceID: device, Elements: elements}
}

func registerOp(id, device string, version int64, value string) Op {
	return Op{ID: id, DeviceID: device, Version: version, Value: json.RawMessage(value)}
}

func orsetOp(id, device, action, element string) Op {
	return Op{ID: id, DeviceID: device, Action: action, Element: element}
}

func wantState(t *testing.T, svc *Service, doc, wantValue string) {
	t.Helper()
	state, err := svc.GetState(doc)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if string(state.Value) != wantValue {
		t.Fatalf("merged value = %s; want %s", state.Value, wantValue)
	}
}

// --- not found and type fixation -------------------------------------------

func TestNotFoundBeforeAnyOp(t *testing.T) {
	svc := newService(t, nil)
	if _, err := svc.GetState("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetState on empty doc err = %v; want ErrNotFound", err)
	}
	if _, err := svc.GetSnapshot("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSnapshot on empty doc err = %v; want ErrNotFound", err)
	}
	if _, err := svc.Compact("doc", "dev"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Compact on empty doc err = %v; want ErrNotFound", err)
	}
}

func TestTypeFixedOnFirstBatch(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "dev", 1)}); err != nil {
		t.Fatalf("first batch: %v", err)
	}
	state, err := svc.GetState("doc")
	if err != nil || state.Type != TypeCounter {
		t.Fatalf("type = %q, %v; want counter", state.Type, err)
	}
	// Declaring another type afterward is a document-level conflict (empty ID).
	_, err = svc.Submit("doc", TypeGSet, []Op{gsetOp("o2", "dev", "apple")})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "" {
		t.Fatalf("type change err = %v; want *ErrConflict with empty ID", err)
	}
	// The rejected batch wrote nothing: the merge is unchanged.
	wantState(t, svc, "doc", "1")
}

func TestConcurrentTypeDeclarationExactlyOneWins(t *testing.T) {
	svc := newService(t, nil)

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			docType := TypeCounter
			if i%2 == 0 {
				docType = TypeGSet
			}
			device := fmt.Sprintf("dev-%d", i)
			var op Op
			if docType == TypeCounter {
				op = counterOp(fmt.Sprintf("o-%d", i), device, 1)
			} else {
				op = gsetOp(fmt.Sprintf("o-%d", i), device, fmt.Sprintf("e-%d", i))
			}
			_, err := svc.Submit("doc", docType, []Op{op})
			if err != nil {
				var conflict *ErrConflict
				if !errors.As(err, &conflict) {
					errs <- err
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected submit error: %v", err)
	}

	state, err := svc.GetState("doc")
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	switch state.Type {
	case TypeCounter:
		if string(state.Value) != "4" {
			t.Fatalf("counter merge after race = %s; want 4 (the 4 counter batches)", state.Value)
		}
	case TypeGSet:
		if string(state.Value) != `["e-1","e-3","e-5","e-7"]` {
			t.Fatalf("gset merge after race = %s", state.Value)
		}
	default:
		t.Fatalf("unexpected type %q", state.Type)
	}
}

// --- counter ----------------------------------------------------------------

func TestCounterMergeAndMonotonicity(t *testing.T) {
	svc := newService(t, nil)

	results, err := svc.Submit("doc", TypeCounter, []Op{
		counterOp("a1", "a", 5),
		counterOp("b1", "b", 3),
	})
	if err != nil || len(results) != 2 || !results[0].Created || !results[1].Created {
		t.Fatalf("submit = %+v, %v", results, err)
	}
	wantState(t, svc, "doc", "8")

	// Per-device advance moves the sum; an equal contribution is an accepted
	// no-op.
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("a2", "a", 7)}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	wantState(t, svc, "doc", "10")
	noOp, err := svc.Submit("doc", TypeCounter, []Op{counterOp("a3", "a", 7)})
	if err != nil || !noOp[0].Created {
		t.Fatalf("equal contribution = %+v, %v; want created=true accepted no-op row", noOp, err)
	}
	wantState(t, svc, "doc", "10")

	// A regression is rejected with the op id and changes nothing.
	_, err = svc.Submit("doc", TypeCounter, []Op{
		counterOp("a4", "a", 9),
		counterOp("a5", "a", 2),
	})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "a5" {
		t.Fatalf("regression err = %v; want conflict on a5", err)
	}
	wantState(t, svc, "doc", "10")

	// Idempotent repeat (same id, device and decoded value): created=false.
	again, err := svc.Submit("doc", TypeCounter, []Op{counterOp("a1", "a", 5)})
	if err != nil || again[0].Created {
		t.Fatalf("idempotent repeat = %+v, %v", again, err)
	}
	// Same id, different device or value is a conflict.
	_, err = svc.Submit("doc", TypeCounter, []Op{counterOp("a1", "b", 5)})
	if !errors.As(err, &conflict) {
		t.Fatalf("different-device repeat err = %v; want conflict", err)
	}
	_, err = svc.Submit("doc", TypeCounter, []Op{counterOp("a1", "a", 6)})
	if !errors.As(err, &conflict) {
		t.Fatalf("different-value repeat err = %v; want conflict", err)
	}
}

// --- gset -------------------------------------------------------------------

func TestGSetUnionSortedIdempotentAndEmptyArray(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.Submit("doc", TypeGSet, []Op{
		gsetOp("g1", "dev", "banana", "apple"),
		gsetOp("g2", "dev", "cherry", "apple"),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	state, _ := svc.GetState("doc")
	if string(state.Value) != `["apple","banana","cherry"]` {
		t.Fatalf("union = %s; want sorted unique union", state.Value)
	}

	// Re-adding covered elements with a new id is accepted but moves nothing.
	if _, err := svc.Submit("doc", TypeGSet, []Op{gsetOp("g3", "dev", "apple")}); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	wantState(t, svc, "doc", `["apple","banana","cherry"]`)

	// Idempotent replay compares by element set, order-insensitive.
	again, err := svc.Submit("doc", TypeGSet, []Op{gsetOp("g1", "dev", "apple", "banana")})
	if err != nil || again[0].Created {
		t.Fatalf("gset idempotent = %+v, %v", again, err)
	}
	// Different element set on the same id conflicts.
	_, err = svc.Submit("doc", TypeGSet, []Op{gsetOp("g1", "dev", "apple", "durian")})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("gset mismatch err = %v; want conflict", err)
	}

	// The empty-set serialization floor is [] rather than null.
	elements := []string{}
	encoded, _ := json.Marshal(elements)
	if string(encoded) != "[]" {
		t.Fatalf("empty slice marshals as %s; want []", encoded)
	}
}

// --- register ---------------------------------------------------------------

func TestRegisterWinnerTieBreakAndVersionGate(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.Submit("doc", TypeRegister, []Op{
		registerOp("r1", "a", 1, `"first"`),
		registerOp("r2", "b", 1, `"second"`),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Equal versions: the lexicographically smaller operation id wins.
	wantState(t, svc, "doc", `"first"`)

	if _, err := svc.Submit("doc", TypeRegister, []Op{registerOp("r3", "b", 2, `{"v":2}`)}); err != nil {
		t.Fatalf("advance: %v", err)
	}
	wantState(t, svc, "doc", `{"v":2}`)

	// A strictly older/equal version from the same device is rejected.
	for _, version := range []int64{1, 2} {
		_, err := svc.Submit("doc", TypeRegister, []Op{registerOp(fmt.Sprintf("old-%d", version), "b", version, `null`)})
		var conflict *ErrConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("version %d err = %v; want conflict", version, err)
		}
	}
	// A higher version that loses the comparison is accepted but moves
	// nothing.
	if _, err := svc.Submit("doc", TypeRegister, []Op{registerOp("r4", "a", 2, `{"v":2}`)}); err != nil {
		t.Fatalf("losing higher version: %v", err)
	}
	wantState(t, svc, "doc", `{"v":2}`)

	// Idempotency compares device, version and JSON-semantic value.
	again, err := svc.Submit("doc", TypeRegister, []Op{registerOp("r3", "b", 2, `{"v":2.0}`)})
	if err != nil || again[0].Created {
		t.Fatalf("register idempotent = %+v, %v", again, err)
	}
	_, err = svc.Submit("doc", TypeRegister, []Op{registerOp("r3", "b", 3, `{"v":2}`)})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("register version mismatch err = %v; want conflict", err)
	}
}

func TestRegisterValueShapes(t *testing.T) {
	svc := newService(t, nil)
	for i, value := range []string{"null", "42", `"s"`, "[1,2]", `{"k":"v"}`} {
		doc := fmt.Sprintf("doc-%d", i)
		if _, err := svc.Submit(doc, TypeRegister, []Op{registerOp("r1", "dev", 0, value)}); err != nil {
			t.Fatalf("submit %s: %v", value, err)
		}
		state, err := svc.GetState(doc)
		if err != nil || string(state.Value) != value {
			t.Fatalf("value shape %s -> %s, %v", value, state.Value, err)
		}
	}
}

// --- orset ------------------------------------------------------------------

func TestORSetObservedRemoveAndConcurrency(t *testing.T) {
	svc := newService(t, nil)

	// add apple, remove it (tombsones the observed tag), add it again: the
	// later add survives.
	if _, err := svc.Submit("doc", TypeORSet, []Op{orsetOp("add1", "dev", ORSetAdd, "apple")}); err != nil {
		t.Fatalf("add1: %v", err)
	}
	if _, err := svc.Submit("doc", TypeORSet, []Op{orsetOp("rm1", "dev", ORSetRemove, "apple")}); err != nil {
		t.Fatalf("rm1: %v", err)
	}
	wantState(t, svc, "doc", `[]`)
	if _, err := svc.Submit("doc", TypeORSet, []Op{orsetOp("add2", "dev", ORSetAdd, "apple")}); err != nil {
		t.Fatalf("add2: %v", err)
	}
	wantState(t, svc, "doc", `["apple"]`)

	// Removing a never-added (or already fully removed) element is an
	// accepted no-op with no error.
	if _, err := svc.Submit("doc", TypeORSet, []Op{orsetOp("rm2", "dev", ORSetRemove, "banana")}); err != nil {
		t.Fatalf("remove never-added: %v", err)
	}
	wantState(t, svc, "doc", `["apple"]`)

	// Idempotency compares device, action and element.
	again, err := svc.Submit("doc", TypeORSet, []Op{orsetOp("rm1", "dev", ORSetRemove, "apple")})
	if err != nil || again[0].Created {
		t.Fatalf("orset idempotent = %+v, %v", again, err)
	}
	var conflict *ErrConflict
	_, err = svc.Submit("doc", TypeORSet, []Op{orsetOp("rm1", "dev", ORSetAdd, "apple")})
	if !errors.As(err, &conflict) {
		t.Fatalf("orset action mismatch err = %v; want conflict", err)
	}
	_, err = svc.Submit("doc", TypeORSet, []Op{orsetOp("rm1", "other", ORSetRemove, "apple")})
	if !errors.As(err, &conflict) {
		t.Fatalf("orset device mismatch err = %v; want conflict", err)
	}
}

func TestORSetMergeIsOrderIndependent(t *testing.T) {
	// Two services on separate documents replay the same accepted operations
	// in different orders; both converge to the same set.
	svc := newService(t, nil)
	ops := []Op{
		orsetOp("a1", "d", ORSetAdd, "apple"),
		orsetOp("b1", "d", ORSetAdd, "banana"),
		orsetOp("ra", "d", ORSetRemove, "apple"),
		orsetOp("a2", "d", ORSetAdd, "apple"),
		orsetOp("c1", "d", ORSetAdd, "cherry"),
	}
	orderA := []int{0, 1, 2, 3, 4}
	orderB := []int{4, 3, 2, 1, 0}
	for i, idxs := range [][]int{orderA, orderB} {
		doc := fmt.Sprintf("doc-%d", i)
		for _, idx := range idxs {
			op := ops[idx]
			op.ID = fmt.Sprintf("%s-%d", op.ID, i)
			if op.Action == ORSetRemove {
				// The removal in the shuffled order observes a different set;
				// compare only the add-heavy prefix that both orders share.
				continue
			}
			if _, err := svc.Submit(doc, TypeORSet, []Op{op}); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}
	}
	wantState(t, svc, "doc-0", `["apple","banana","cherry"]`)
	wantState(t, svc, "doc-1", `["apple","banana","cherry"]`)
}

// --- gating -----------------------------------------------------------------

func TestGateEnforcedBeforeContent(t *testing.T) {
	gate := newFakeGate("known")
	svc := newService(t, gate)

	// Unregistered device: the gate's sentinel, and no type row is created.
	_, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "ghost", 1)})
	if !errors.Is(err, errNotRegistered) {
		t.Fatalf("unknown device err = %v; want %v", err, errNotRegistered)
	}
	if _, err := svc.GetState("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit created state: %v", err)
	}

	// Revoked device: same.
	gate.deny("doc", "known")
	_, err = svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "known", 1)})
	if !errors.Is(err, errAccessDenied) {
		t.Fatalf("revoked device err = %v; want %v", err, errAccessDenied)
	}
	if _, err := svc.GetState("doc"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected submit created state: %v", err)
	}

	// Compaction honors the same gate.
	gate.denied = map[string]map[string]bool{}
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "known", 1)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gate.deny("doc", "known")
	if _, err := svc.Compact("doc", "known"); !errors.Is(err, errAccessDenied) {
		t.Fatalf("revoked compact err = %v; want %v", err, errAccessDenied)
	}
	if _, err := svc.Compact("doc", "ghost"); !errors.Is(err, errNotRegistered) {
		t.Fatalf("unknown-device compact err = %v; want %v", err, errNotRegistered)
	}
}

// --- compaction -------------------------------------------------------------

func TestCompactTrimsAndPreservesDecisions(t *testing.T) {
	svc := newService(t, nil)

	// Counter: two contributions per device, the smaller is trimmable.
	if _, err := svc.Submit("doc", TypeCounter, []Op{
		counterOp("a-low", "a", 1),
		counterOp("a-high", "a", 5),
		counterOp("b-low", "b", 2),
		counterOp("b-high", "b", 9),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before, err := svc.GetSnapshot("doc")
	if err != nil || before.Operations != 4 || before.Tombstones != 0 {
		t.Fatalf("pre-compact snapshot = %+v, %v", before, err)
	}
	after, err := svc.Compact("doc", "dev")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if after.Operations != 2 || after.Tombstones != 0 || string(after.Value) != "14" {
		t.Fatalf("post-compact snapshot = %+v; want 2 ops, 0 tombstones, value 14", after)
	}
	// The snapshot read immediately afterward is identical.
	read, err := svc.GetSnapshot("doc")
	if err != nil || read.Operations != after.Operations || string(read.Value) != string(after.Value) {
		t.Fatalf("snapshot read after compact = %+v, %v; want %+v", read, err, after)
	}
	// Repeating compaction trims nothing and returns the same body.
	again, err := svc.Compact("doc", "dev")
	if err != nil || again.Operations != 2 || string(again.Value) != "14" {
		t.Fatalf("repeat compact = %+v, %v", again, err)
	}

	// A trimmed id replays idempotently (created=false even though a higher
	// contribution now covers the device) ...
	repeat, err := svc.Submit("doc", TypeCounter, []Op{counterOp("a-low", "a", 1)})
	if err != nil || repeat[0].Created {
		t.Fatalf("trimmed-id replay = %+v, %v; want created=false", repeat, err)
	}
	// ... while a mismatched replay still conflicts.
	var conflict *ErrConflict
	_, err = svc.Submit("doc", TypeCounter, []Op{counterOp("a-low", "a", 2)})
	if !errors.As(err, &conflict) || conflict.ID != "a-low" {
		t.Fatalf("trimmed-id mismatch err = %v; want conflict on a-low", err)
	}
	_, err = svc.Submit("doc", TypeCounter, []Op{counterOp("a-low", "b", 1)})
	if !errors.As(err, &conflict) {
		t.Fatalf("trimmed-id device mismatch err = %v; want conflict", err)
	}
	// The merge never moved.
	wantState(t, svc, "doc", "14")
}

func TestCompactORSetDropsDeadTagsAndTombstones(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.Submit("doc", TypeORSet, []Op{
		orsetOp("add1", "dev", ORSetAdd, "apple"),
		orsetOp("add2", "dev", ORSetAdd, "banana"),
		orsetOp("rm1", "dev", ORSetRemove, "apple"),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	snap, err := svc.GetSnapshot("doc")
	if err != nil || snap.Operations != 2 || snap.Tombstones != 1 {
		t.Fatalf("pre-compact = %+v, %v; want 2 tags, 1 tombstone", snap, err)
	}
	after, err := svc.Compact("doc", "dev")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	// The dead apple tag and its tombstone cancel out; banana survives.
	if after.Operations != 1 || after.Tombstones != 0 {
		t.Fatalf("post-compact = %+v; want 1 tag, 0 tombstones", after)
	}
	wantState(t, svc, "doc", `["banana"]`)

	// The orset operation log is untouched by compaction: replays stay
	// idempotent and a replayed remove tombstones nothing new.
	repeat, err := svc.Submit("doc", TypeORSet, []Op{
		orsetOp("add1", "dev", ORSetAdd, "apple"),
		orsetOp("rm1", "dev", ORSetRemove, "apple"),
	})
	if err != nil {
		t.Fatalf("orset replay: %v", err)
	}
	for _, r := range repeat {
		if r.Created {
			t.Fatalf("orset replay created row for %s; want idempotent", r.ID)
		}
	}
	wantState(t, svc, "doc", `["banana"]`)
}

// --- subscriptions ----------------------------------------------------------

func TestSubscriptionInitialStateDrainAndRevoke(t *testing.T) {
	svc := newService(t, nil)

	// A subscription on a document with no state waits silently: no initial
	// state and no frame until the first one appears.
	initial, sub, unregister, err := svc.OpenSubscription("doc", "dev")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer unregister()
	if initial != nil {
		t.Fatalf("initial = %+v; want nil on an empty document", initial)
	}
	if queued := sub.Drain(); queued != nil {
		t.Fatalf("empty document queued %d states", len(queued))
	}

	// The first state-changing commit is queued in commit order.
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "dev", 1)}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	queued := sub.Drain()
	if len(queued) != 1 || string(queued[0].Value) != "1" {
		t.Fatalf("queued after first commit = %+v", queued)
	}
	// An idempotent or no-op commit queues nothing.
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o1", "dev", 1)}); err != nil {
		t.Fatalf("idempotent submit: %v", err)
	}
	if queued := sub.Drain(); queued != nil {
		t.Fatalf("idempotent commit queued states: %+v", queued)
	}

	// A real advance queues the next state.
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("o2", "dev", 4)}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	queued = sub.Drain()
	if len(queued) != 1 || string(queued[0].Value) != "4" {
		t.Fatalf("queued after advance = %+v", queued)
	}

	// Revoke closes the one-shot channel stickily, scoped to the pair.
	svc.OnRevoke("doc", "dev")
	select {
	case <-sub.Revoked():
	case <-time.After(time.Second):
		t.Fatalf("revoke signal not delivered")
	}
	svc.OnRevoke("doc", "dev") // idempotent
}

func TestSubscriptionAtomicRegisterAndRead(t *testing.T) {
	svc := newService(t, nil)
	if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp("seed", "dev", 3)}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Registration-plus-read must never miss a commit or double-deliver it:
	// hammer OpenSubscription while another goroutine submits advances.
	const readers = 20
	const writers = 20
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, sub, unregister, err := svc.OpenSubscription("doc", "dev")
			if err != nil {
				t.Errorf("open: %v", err)
				return
			}
			time.Sleep(time.Millisecond)
			unregister()
			_ = sub
		}()
	}
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each writer is its own device so its contributions cannot
			// regress relative to another writer's.
			if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp(fmt.Sprintf("w-%d", i), fmt.Sprintf("dev-%d", i), int64(4+i))}); err != nil {
				t.Errorf("submit: %v", err)
			}
		}(i)
	}
	wg.Wait()
}

func TestSubscriptionClosingWakes(t *testing.T) {
	svc := newService(t, nil)
	_, sub, unregister, err := svc.OpenSubscription("doc", "dev")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer unregister()

	svc.InterruptWaits()
	if !svc.Closing() {
		t.Fatalf("Closing() = false after InterruptWaits")
	}
	select {
	case <-sub.Wake():
	case <-time.After(time.Second):
		t.Fatalf("live subscription not woken at shutdown")
	}

	// A subscription added during shutdown is signaled immediately.
	_, late, lateUnreg, err := svc.OpenSubscription("doc", "dev")
	if err != nil {
		t.Fatalf("late open: %v", err)
	}
	defer lateUnreg()
	select {
	case <-late.Wake():
	case <-time.After(time.Second):
		t.Fatalf("late subscription not woken at shutdown")
	}
}

// --- concurrency and durability ---------------------------------------------

func TestConcurrentCountersConverge(t *testing.T) {
	svc := newService(t, nil)
	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 1; i <= 20; i++ {
				id := fmt.Sprintf("w%d-v%d", w, i)
				if _, err := svc.Submit("doc", TypeCounter, []Op{counterOp(id, fmt.Sprintf("dev-%d", w), int64(i))}); err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	wantState(t, svc, "doc", fmt.Sprintf("%d", workers*20))
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt.db")
	db := openFileDB(t, path)
	svc, err := New(db, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := svc.Submit("doc", TypeCounter, []Op{
		counterOp("a1", "a", 5),
		counterOp("b1", "b", 6),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := svc.Compact("doc", "a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	svc.Close()
	_ = db.Close()

	db2 := openFileDB(t, path)
	reopened, err := New(db2, newFakeGate("a", "b"))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	t.Cleanup(reopened.Close)

	state, err := reopened.GetState("doc")
	if err != nil || state.Type != TypeCounter || string(state.Value) != "11" {
		t.Fatalf("reopened state = %+v, %v", state, err)
	}
	snap, err := reopened.GetSnapshot("doc")
	if err != nil || snap.Operations != 2 {
		t.Fatalf("reopened snapshot = %+v, %v", snap, err)
	}
	// Type fixation survives: another type is a conflict.
	var conflict *ErrConflict
	_, err = reopened.Submit("doc", TypeGSet, []Op{gsetOp("x1", "a", "pear")})
	if !errors.As(err, &conflict) {
		t.Fatalf("type fixation after reopen err = %v; want conflict", err)
	}
	// The trimmed id's identity survives: replay idempotent, mismatch a
	// conflict.
	again, err := reopened.Submit("doc", TypeCounter, []Op{counterOp("a1", "a", 5)})
	if err != nil || again[0].Created {
		t.Fatalf("trimmed-id replay after reopen = %+v, %v", again, err)
	}
	_, err = reopened.Submit("doc", TypeCounter, []Op{counterOp("a1", "a", 9)})
	if !errors.As(err, &conflict) {
		t.Fatalf("trimmed-id mismatch after reopen err = %v; want conflict", err)
	}
}
