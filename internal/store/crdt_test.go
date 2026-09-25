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

func TestCRDTRegisterLastWriterWins(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2", "dev-3")

	submit := func(op CRDTOp) {
		t.Helper()
		results, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{op})
		if err != nil {
			t.Fatalf("submit %s: %v", op.ID, err)
		}
		if !results[0].Created {
			t.Fatalf("%s should be created", op.ID)
		}
	}
	state := func() string {
		t.Helper()
		st, err := s.GetCRDTState("doc")
		if err != nil {
			t.Fatal(err)
		}
		if st.Type != CRDTTypeRegister {
			t.Fatalf("type = %q", st.Type)
		}
		return string(st.Value)
	}

	// The greatest version wins regardless of arrival order.
	submit(registerOp("b1", "dev-2", 5, `"five"`))
	submit(registerOp("a1", "dev-1", 2, `"two"`))   // loses to the existing v5
	submit(registerOp("c1", "dev-3", 7, `"seven"`)) // overtakes v5
	if got := state(); got != `"seven"` {
		t.Fatalf("state = %s, want \"seven\"", got)
	}

	// A losing op from a new device is accepted but does not move the merge.
	submit(registerOp("d1", "dev-1", 3, `"three"`))
	if got := state(); got != `"seven"` {
		t.Fatalf("state after losing op = %s, want \"seven\"", got)
	}
}

func TestCRDTRegisterVersionTieBreaksBySmallerID(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	// Same version from two devices: the lexicographically smaller op id wins,
	// independent of arrival order.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("z-op", "dev-1", 4, `"from-z"`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a-op", "dev-2", 4, `"from-a"`),
	}); err != nil {
		t.Fatal(err)
	}
	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if string(state.Value) != `"from-a"` {
		t.Fatalf("tie state = %s, want \"from-a\"", state.Value)
	}
}

func TestCRDTRegisterRejectsVersionRegressionAndStall(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a1", "dev-1", 5, `"five"`),
	}); err != nil {
		t.Fatal(err)
	}

	var conflict *ErrCRDTConflict
	// A lower version from the same device is a 409.
	_, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a2", "dev-1", 4, `"four"`),
	})
	if !errors.As(err, &conflict) || conflict.ID != "a2" {
		t.Fatalf("regression err = %v, want *ErrCRDTConflict for a2", err)
	}
	// An equal version (a stall) is a 409 too, even with a fresh id.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a3", "dev-1", 5, `"five-again"`),
	})
	if !errors.As(err, &conflict) || conflict.ID != "a3" {
		t.Fatalf("stall err = %v, want *ErrCRDTConflict for a3", err)
	}
	// Both rejections leave the state untouched.
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `"five"` {
		t.Fatalf("state after rejections = %s, want \"five\"", state.Value)
	}

	// Another device is unaffected by dev-1's rejected batches.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("b1", "dev-2", 6, `"six"`),
	}); err != nil {
		t.Fatal(err)
	}
	// And dev-1 can still move strictly forward.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("a4", "dev-1", 7, `"seven"`),
	}); err != nil {
		t.Fatal(err)
	}
	state, _ = s.GetCRDTState("doc")
	if string(state.Value) != `"seven"` {
		t.Fatalf("state = %s, want \"seven\"", state.Value)
	}
}

func TestCRDTRegisterIdempotencyAndConflict(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	op := registerOp("same-id", "dev-1", 3, `{"k":1}`)
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{op}); err != nil {
		t.Fatal(err)
	}
	// Identical repeat (device, version and value) is idempotent.
	results, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{op})
	if err != nil {
		t.Fatalf("identical repeat: %v", err)
	}
	if results[0].Created {
		t.Fatal("identical repeat must report created=false")
	}

	var conflict *ErrCRDTConflict
	// Same id, different value -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-1", 3, `{"k":2}`),
	})
	if !errors.As(err, &conflict) || conflict.ID != "same-id" {
		t.Fatalf("different value err = %v", err)
	}
	// Same id, different version -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-1", 4, `{"k":1}`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different version err = %v", err)
	}
	// Same id, different device -> 409.
	_, err = s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("same-id", "dev-2", 3, `{"k":1}`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if string(state.Value) != `{"k":1}` {
		t.Fatalf("state after conflicts = %s, want {\"k\":1}", state.Value)
	}
}

func TestCRDTRegisterValueShapes(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// Every JSON shape is stored verbatim and presented as-is when it wins.
	values := []string{
		`null`,
		`42`,
		`"text"`,
		`[1,"two",null]`,
		`{"nested":{"ok":true},"list":[1,2]}`,
	}
	for i, v := range values {
		if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
			registerOp(fmt.Sprintf("op-%d", i), "dev-1", int64(i+1), v),
		}); err != nil {
			t.Fatalf("submit %s: %v", v, err)
		}
		state, err := s.GetCRDTState("doc")
		if err != nil {
			t.Fatal(err)
		}
		if string(state.Value) != v {
			t.Fatalf("state = %s, want verbatim %s", state.Value, v)
		}
	}
}

func TestCRDTRegisterTypeFixation(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `1`),
	}); err != nil {
		t.Fatal(err)
	}
	// Declaring counter or gset afterwards is a 409.
	var conflict *ErrCRDTConflict
	_, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1))
	if !errors.As(err, &conflict) {
		t.Fatalf("counter after register err = %v", err)
	}
	_, err = s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("gset after register err = %v", err)
	}
	// And a register declared after a counter is likewise a 409.
	if _, err := s.SubmitCRDTOps("other", CRDTTypeCounter, counterOps("dev-1", 1)); err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitCRDTOps("other", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `1`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("register after counter err = %v", err)
	}
}

func TestCRDTRegisterPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s, "dev-1", "dev-2")
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 3, `{"a":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r2", "dev-2", 5, `"winner"`),
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
	if state.Type != CRDTTypeRegister || string(state.Value) != `"winner"` {
		t.Fatalf("state after restart = %s %s", state.Type, state.Value)
	}
	// Idempotency decisions survive the restart.
	results, err := s2.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r2", "dev-2", 5, `"winner"`),
	})
	if err != nil || results[0].Created {
		t.Fatalf("idempotent replay after restart = created:%v err:%v", results[0].Created, err)
	}
	// The per-device version clock survives: a stalled version is still a 409.
	var conflict *ErrCRDTConflict
	_, err = s2.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r3", "dev-1", 3, `"stall"`),
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("stalled version after restart err = %v", err)
	}
	// Type fixation survives as well.
	_, err = s2.SubmitCRDTOps("doc", CRDTTypeCounter, counterOps("dev-1", 1))
	if !errors.As(err, &conflict) {
		t.Fatalf("type change after restart err = %v", err)
	}
}

func TestCRDTRegisterIsIndependentOfChangeLog(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeRegister, []CRDTOp{
		registerOp("r1", "dev-1", 1, `"v"`),
	}); err != nil {
		t.Fatal(err)
	}
	// The change log is untouched: no rows, no cursor movement.
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
