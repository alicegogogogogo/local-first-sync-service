package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func rawInt(n int64) json.RawMessage {
	return json.RawMessage(fmt.Sprintf("%d", n))
}

func registerDevices(t *testing.T, s *Store, devices ...string) {
	t.Helper()
	for _, d := range devices {
		if _, err := s.RegisterDevice(d); err != nil {
			t.Fatalf("register %s: %v", d, err)
		}
	}
}

func counterOps(device string, vals ...int64) []CRDTOp {
	ops := make([]CRDTOp, len(vals))
	for i, v := range vals {
		ops[i] = CRDTOp{ID: fmt.Sprintf("%s-op-%d", device, i+1), DeviceID: device, Value: rawInt(v)}
	}
	return ops
}

func TestCRDTNotFoundBeforeAnyOp(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.GetCRDTState("doc"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("empty state err = %v, want ErrCRDTNotFound", err)
	}
}

func TestCRDTCounterMergeIsSumOfPerDeviceMaxima(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2", "dev-3")

	submit := func(device string, val int64, id string) {
		t.Helper()
		results, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
			{ID: id, DeviceID: device, Value: rawInt(val)},
		})
		if err != nil {
			t.Fatalf("submit %s=%d: %v", device, val, err)
		}
		if !results[0].Created {
			t.Fatalf("%s=%d should be created", device, val)
		}
	}

	// Interleave devices out of order; the merge depends only on each max.
	submit("dev-1", 5, "a1")
	submit("dev-2", 2, "b1")
	submit("dev-1", 9, "a2") // dev-1 max advances 5 -> 9
	submit("dev-3", 0, "c1") // zero still establishes the device
	submit("dev-1", 9, "a3") // equal maximum: accepted, created (new id), state unchanged

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeCounter {
		t.Fatalf("type = %q", state.Type)
	}
	if string(state.Value) != "11" { // 9 + 2 + 0
		t.Fatalf("merged value = %s, want 11", state.Value)
	}
}

func TestCRDTCounterRejectsRegression(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 5)); err != nil {
		t.Fatal(err)
	}
	// A regression is a 409 conflict and leaves the state untouched.
	_, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "dev-1-op-regress", DeviceID: "dev-1", Value: rawInt(4)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("regression err = %v, want *ErrCRDTConflict", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != "5" {
		t.Fatalf("state after regression = %s, want 5", state.Value)
	}

	// Another device advancing must not be blocked by the rejected batch.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-2", 7)); err != nil {
		t.Fatal(err)
	}
	state, _ = s.GetCRDTState("doc")
	if string(state.Value) != "12" {
		t.Fatalf("state = %s, want 12", state.Value)
	}
}

func TestCRDTTypeIsFixedOnFirstBatch(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	// Declaring the other type later is a conflict, even from another device.
	_, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-2", Elements: []string{"x"}},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("type change err = %v, want *ErrCRDTConflict", err)
	}
	if conflict.ID != "" {
		t.Fatalf("document-level conflict should carry no op id, got %q", conflict.ID)
	}
	// Re-declaring the fixed type still works.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-2", 3)); err != nil {
		t.Fatalf("same-type submit: %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if state.Type != CRDTTypeCounter || string(state.Value) != "4" {
		t.Fatalf("state = %s %s", state.Type, state.Value)
	}
}

func TestCRDTConcurrentTypeDeclarationExactlyOneWins(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	const n = 40
	var wg sync.WaitGroup
	wins := make(chan string, 2*n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := s.SubmitCRDTOps("race", CRDTTypeCounter, []CRDTOp{
				{ID: fmt.Sprintf("c-%d", i), DeviceID: "dev-1", Value: rawInt(int64(i + 1))},
			})
			if err == nil {
				wins <- "counter"
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := s.SubmitCRDTOps("race", CRDTTypeGSet, []CRDTOp{
				{ID: fmt.Sprintf("g-%d", i), DeviceID: "dev-2", Elements: []string{fmt.Sprintf("e%d", i)}},
			})
			if err == nil {
				wins <- "gset"
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(wins)

	counts := map[string]int{}
	for w := range wins {
		counts[w]++
	}
	// Exactly one type's first batch lands; every same-type batch that arrives
	// after it also succeeds, so one counter is exactly 1 (or one gset >= 1).
	if counts["counter"] > 0 && counts["gset"] > 0 {
		t.Fatalf("both types accepted: %v", counts)
	}
	if counts["counter"] == 0 && counts["gset"] == 0 {
		t.Fatal("neither type won the race")
	}
	// The losing type must have exactly zero accepted batches.
	state, err := s.GetCRDTState("race")
	if err != nil {
		t.Fatal(err)
	}
	if counts["counter"] > 0 && state.Type != CRDTTypeCounter {
		t.Fatalf("counter won but state type = %q", state.Type)
	}
	if counts["gset"] > 0 && state.Type != CRDTTypeGSet {
		t.Fatalf("gset won but state type = %q", state.Type)
	}
}

func TestCRDTGSetUnionSortedAndIdempotent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submit := func(device, id string, elements ...string) []CRDTResult {
		t.Helper()
		results, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
			{ID: id, DeviceID: device, Elements: elements},
		})
		if err != nil {
			t.Fatalf("submit %s: %v", id, err)
		}
		return results
	}

	submit("dev-1", "g1", "banana", "apple")
	submit("dev-2", "g2", "cherry", "apple") // apple is a duplicate element
	// Re-adding existing elements with a new id is accepted but changes nothing.
	r := submit("dev-1", "g3", "apple", "banana", "cherry")
	if !r[0].Created {
		t.Fatal("a new op id adding only existing elements is still created")
	}

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeGSet {
		t.Fatalf("type = %q", state.Type)
	}
	var got []string
	if err := json.Unmarshal(state.Value, &got); err != nil {
		t.Fatal(err)
	}
	want := []string{"apple", "banana", "cherry"}
	if len(got) != len(want) {
		t.Fatalf("elements = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("elements = %v, want sorted %v", got, want)
		}
	}
}

func TestCRDTGSetEmptyStateIsEmptyArray(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// A first batch always adds at least one element at the API boundary; at
	// the store layer the document exists once a batch lands, and the union is
	// presented as [] rather than null when nothing remains.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"only"}},
	}); err != nil {
		t.Fatal(err)
	}
	state, _ := s.GetCRDTState("doc")
	var got []string
	if err := json.Unmarshal(state.Value, &got); err != nil || len(got) != 1 || got[0] != "only" {
		t.Fatalf("state = %s", state.Value)
	}
}

func TestCRDTOpIdempotencyAndConflict(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	op := []CRDTOp{{ID: "same-id", DeviceID: "dev-1", Value: rawInt(5)}}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, op); err != nil {
		t.Fatal(err)
	}
	// Identical repeat is idempotent.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, op)
	if err != nil {
		t.Fatalf("identical repeat: %v", err)
	}
	if results[0].Created {
		t.Fatal("identical repeat must report created=false")
	}
	// Same id, different value -> 409, state unchanged.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "same-id", DeviceID: "dev-1", Value: rawInt(6)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) || conflict.ID != "same-id" {
		t.Fatalf("different value err = %v", err)
	}
	// Same id, different device -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "same-id", DeviceID: "dev-2", Value: rawInt(5)},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != "5" {
		t.Fatalf("state after conflicts = %s, want 5", state.Value)
	}
}

func TestCRDTPermissionAndRegistrationGating(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	// Unregistered device is 404 even with no document state.
	_, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "x", DeviceID: "ghost", Value: rawInt(1)},
	})
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unregistered err = %v, want ErrDeviceNotFound", err)
	}

	// Establish state with dev-1.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-2", 2))
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked err = %v, want ErrPermissionDenied", err)
	}
	// Re-grant restores the ability to submit.
	if _, err := s.SetDocumentPermission("doc", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-2", 2)); err != nil {
		t.Fatalf("after grant: %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != "3" {
		t.Fatalf("state = %s, want 3", state.Value)
	}
}

func TestCRDTConcurrentCountersConverge(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	devices := make([]string, 8)
	for i := range devices {
		devices[i] = fmt.Sprintf("dev-%d", i)
	}
	registerDevices(t, s, devices...)

	// Every device submits the same cumulative maximum from many goroutines;
	// the merge is commutative and associative, so the total is deterministic.
	const rounds = 25
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, d := range devices {
		for r := 0; r < rounds; r++ {
			wg.Add(1)
			go func(device string, r int) {
				defer wg.Done()
				<-start
				_, _ = s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
					{ID: fmt.Sprintf("%s-%d", device, r), DeviceID: device, Value: rawInt(int64(r + 1))},
				})
			}(d, r)
		}
	}
	close(start)
	wg.Wait()

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	// No regression can ever be accepted, so each device's max is rounds.
	if string(state.Value) != fmt.Sprintf("%d", len(devices)*rounds) {
		t.Fatalf("merged = %s, want %d", state.Value, len(devices)*rounds)
	}
}

func TestCRDTPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1", "dev-2")
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"b", "a"}},
	}); err != nil {
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

	state, err := s2.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeGSet || string(state.Value) != `["a","b"]` {
		t.Fatalf("state after restart = %s %s", state.Type, state.Value)
	}
	// Type fixation survives: declaring a counter now is a 409.
	_, err = s2.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		{ID: "c1", DeviceID: "dev-2", Value: rawInt(1)},
	})
	var conflict *ErrCRDTConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("type change after restart err = %v", err)
	}
	// Idempotency decisions survive as well.
	results, err := s2.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"a", "b"}},
	})
	if err != nil || results[0].Created {
		t.Fatalf("idempotent replay after restart = created:%v err:%v", results[0].Created, err)
	}
}

func TestCRDTIsIndependentOfChangeLog(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 4)); err != nil {
		t.Fatal(err)
	}
	// The change log is untouched: the document is unknown to it.
	known, err := s.DocumentExists("doc")
	if err != nil {
		t.Fatal(err)
	}
	if known {
		t.Fatal("CRDT ops must not create change-log rows")
	}
	changes, nextCursor, err := s.ListChanges("doc", 0, 100)
	if err != nil || len(changes) != 0 || nextCursor != 0 {
		t.Fatalf("change log = %v cursor %d err %v", changes, nextCursor, err)
	}
}

func registerOp(id, device string, version int64, value string) CRDTOp {
	return CRDTOp{ID: id, DeviceID: device, Version: version, Value: json.RawMessage(value)}
}

func TestCRDTRegisterMergeIsMaxVersionThenSmallerID(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submit := func(ops ...CRDTOp) {
		t.Helper()
		if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, ops); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	// Interleave devices and versions out of order; the merge depends only on
	// the largest version.
	submit(registerOp("a1", "dev-1", 3, `"low"`))
	submit(registerOp("b1", "dev-2", 7, `"high"`))
	submit(registerOp("a2", "dev-1", 5, `"mid"`)) // dev-1 advances 3 -> 5, still loses

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeRegister {
		t.Fatalf("type = %q", state.Type)
	}
	if string(state.Value) != `"high"` {
		t.Fatalf("merged value = %s, want \"high\"", state.Value)
	}

	// Equal versions tie-break on the smaller operation id.
	submit(registerOp("b2", "dev-2", 8, `"zebra"`))
	submit(registerOp("a3", "dev-1", 8, `"apple"`)) // same version, smaller id wins
	state, _ = s.GetCRDTState("doc")
	if string(state.Value) != `"apple"` {
		t.Fatalf("tie-break value = %s, want \"apple\"", state.Value)
	}
}

func TestCRDTRegisterMergeIsOrderIndependent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2", "dev-3")

	ops := []CRDTOp{
		registerOp("r1", "dev-1", 4, `{"k":1}`),
		registerOp("r2", "dev-2", 9, `[1,2]`),
		registerOp("r3", "dev-3", 9, `"tie-loser"`),
	}
	// The same operations land in every permutation on separate documents;
	// every document converges to the same winner (version 9, id r2).
	orders := [][]int{{0, 1, 2}, {2, 1, 0}, {1, 2, 0}, {2, 0, 1}}
	for i, order := range orders {
		doc := fmt.Sprintf("doc-%d", i)
		for _, idx := range order {
			if _, err := s.SubmitCRDTOps(doc, CRDTTypeRegister, []CRDTOp{ops[idx]}); err != nil {
				t.Fatalf("doc %s op %d: %v", doc, idx, err)
			}
		}
		state, err := s.GetCRDTState(doc)
		if err != nil {
			t.Fatal(err)
		}
		if string(state.Value) != `[1,2]` {
			t.Fatalf("doc %s merged = %s, want [1,2]", doc, state.Value)
		}
	}
}

func TestCRDTRegisterRejectsNonIncreasingVersion(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a1", "dev-1", 5, `"v5"`),
	}); err != nil {
		t.Fatal(err)
	}
	var conflict *ErrCRDTConflict
	// A regression is a conflict and leaves the state untouched.
	_, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a2", "dev-1", 4, `"v4"`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("regression err = %v, want *ErrCRDTConflict", err)
	}
	// An equal version is a conflict too: versions must strictly increase.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a3", "dev-1", 5, `"v5-again"`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("equal version err = %v, want *ErrCRDTConflict", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `"v5"` {
		t.Fatalf("state after conflicts = %s, want \"v5\"", state.Value)
	}

	// Other devices are unaffected by dev-1's rejected batches.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("b1", "dev-2", 1, `"other"`),
	}); err != nil {
		t.Fatal(err)
	}
	// A strictly larger version from dev-1 is accepted again.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a4", "dev-1", 6, `"v6"`),
	}); err != nil {
		t.Fatal(err)
	}
	state, _ = s.GetCRDTState("doc")
	if string(state.Value) != `"v6"` {
		t.Fatalf("state = %s, want \"v6\"", state.Value)
	}
}

func TestCRDTRegisterIdempotencyAndConflict(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	op := []CRDTOp{registerOp("same-id", "dev-1", 2, `{"x":1}`)}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, op); err != nil {
		t.Fatal(err)
	}
	// Identical repeat (device, value and version) is idempotent.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, op)
	if err != nil {
		t.Fatalf("identical repeat: %v", err)
	}
	if results[0].Created {
		t.Fatal("identical repeat must report created=false")
	}
	var conflict *ErrCRDTConflict
	// Same id, different value -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-1", 2, `{"x":2}`),
	})
	if !errors.As(err, &conflict) || conflict.ID != "same-id" {
		t.Fatalf("different value err = %v", err)
	}
	// Same id, different version -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-1", 3, `{"x":1}`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different version err = %v", err)
	}
	// Same id, different device -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-2", 2, `{"x":1}`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `{"x":1}` {
		t.Fatalf("state after conflicts = %s, want {\"x\":1}", state.Value)
	}
}

func TestCRDTRegisterValueShapesPreserved(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	values := []string{`null`, `0`, `-3.5`, `"text"`, `true`, `[1,"a",null]`, `{"a":[1,2],"b":{"c":null}}`}
	for i, v := range values {
		if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
			registerOp(fmt.Sprintf("r%d", i), "dev-1", int64(i), v),
		}); err != nil {
			t.Fatalf("submit %s: %v", v, err)
		}
		state, err := s.GetCRDTState("doc")
		if err != nil {
			t.Fatal(err)
		}
		if string(state.Value) != v {
			t.Fatalf("value = %s, want %s", state.Value, v)
		}
	}
}

func TestCRDTRegisterTypeFixation(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 0, `"first"`),
	}); err != nil {
		t.Fatal(err)
	}
	var conflict *ErrCRDTConflict
	// Declaring either of the other types later is a conflict.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1)); !errors.As(err, &conflict) {
		t.Fatalf("register->counter err = %v", err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	}); !errors.As(err, &conflict) {
		t.Fatalf("register->gset err = %v", err)
	}
	// And a register document cannot be hijacked the other way round either.
	if _, err := s.SubmitCRDTOps("doc2", CRDTTypeCounter, counterOps("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc2", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 0, `"x"`),
	}); !errors.As(err, &conflict) {
		t.Fatalf("counter->register err = %v", err)
	}
}

func TestCRDTRegisterPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1")
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 4, `{"k":"v"}`),
	}); err != nil {
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

	state, err := s2.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeRegister || string(state.Value) != `{"k":"v"}` {
		t.Fatalf("state after restart = %s %s", state.Type, state.Value)
	}
	// Idempotency decisions survive the restart.
	results, err := s2.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 4, `{"k":"v"}`),
	})
	if err != nil || results[0].Created {
		t.Fatalf("idempotent replay after restart = created:%v err:%v", results[0].Created, err)
	}
	// The version monotonicity survives too: a regressed or equal version is
	// still a conflict.
	var conflict *ErrCRDTConflict
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r2", "dev-1", 4, `"new"`),
	}); !errors.As(err, &conflict) {
		t.Fatalf("equal version after restart err = %v", err)
	}
	if _, err := s2.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r3", "dev-1", 5, `"advanced"`),
	}); err != nil {
		t.Fatalf("advance after restart: %v", err)
	}
	state, _ = s2.GetCRDTState("doc")
	if string(state.Value) != `"advanced"` {
		t.Fatalf("state = %s, want \"advanced\"", state.Value)
	}
}

func TestCRDTRegisterIsIndependentOfChangeLog(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `"x"`),
	}); err != nil {
		t.Fatal(err)
	}
	known, err := s.DocumentExists("doc")
	if err != nil {
		t.Fatal(err)
	}
	if known {
		t.Fatal("register ops must not create change-log rows")
	}
	changes, nextCursor, err := s.ListChanges("doc", 0, 100)
	if err != nil || len(changes) != 0 || nextCursor != 0 {
		t.Fatalf("change log = %v cursor %d err %v", changes, nextCursor, err)
	}
}
