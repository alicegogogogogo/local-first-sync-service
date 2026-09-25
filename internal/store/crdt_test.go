package store

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func openCRDTStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// A counter merges per-device cumulative maxima; each device's own value may
// only move upward.
func TestCRDTCounterMergeOfPerDeviceMaxima(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("b"); err != nil {
		t.Fatal(err)
	}

	submit := func(dev string, ops ...CRDTOp) []CRDTResult {
		t.Helper()
		results, err := s.SubmitCRDT("doc", dev, CRDTTypeCounter, ops)
		if err != nil {
			t.Fatalf("submit %v: %v", ops, err)
		}
		return results
	}
	submit("a", CRDTOp{ID: "a1", Value: "3"})
	submit("b", CRDTOp{ID: "b1", Value: "5"})
	submit("a", CRDTOp{ID: "a2", Value: "7"})

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Type != CRDTTypeCounter || state.Counter != 12 {
		t.Fatalf("state = %+v, want counter 12 (7+5)", state)
	}
}

// Re-posting the same op id with the same device and value is idempotent and
// does not move the counter; a different value (or device) is a conflict and
// writes nothing.
func TestCRDTCounterIdempotencyAndConflict(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("b"); err != nil {
		t.Fatal(err)
	}

	first, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "4"}})
	if err != nil || !first[0].Created {
		t.Fatalf("first = %+v err=%v", first, err)
	}
	again, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "4"}})
	if err != nil || again[0].Created {
		t.Fatalf("idempotent repost = %+v err=%v", again, err)
	}

	var conflict *ErrCRDTOpConflict
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "9"}}); !errors.As(err, &conflict) {
		t.Fatalf("different value err = %v, want *ErrCRDTOpConflict", err)
	}
	conflict = nil
	if _, err := s.SubmitCRDT("doc", "b", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "4"}}); !errors.As(err, &conflict) {
		t.Fatalf("different device err = %v, want *ErrCRDTOpConflict", err)
	}

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	if state.Counter != 4 {
		t.Fatalf("counter after conflicts = %d, want 4", state.Counter)
	}
}

// A device whose new cumulative contribution is below its accepted maximum is
// rejected; the rejected batch changes neither state nor that maximum.
func TestCRDTCounterMonotonicReject(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("b"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "a1", Value: "10"}}); err != nil {
		t.Fatal(err)
	}
	// Backward push, even with a fresh op id, is a 409-class reject.
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "a2", Value: "9"}}); !errors.Is(err, ErrCRDTValueRejected) {
		t.Fatalf("regression err = %v, want ErrCRDTValueRejected", err)
	}
	// The rejected op id was not stored, so it can be reused at a valid value.
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "a2", Value: "10"}}); err != nil {
		t.Fatalf("equal re-advance should be accepted: %v", err)
	}

	// A regression anywhere in a multi-op batch aborts the whole batch.
	_, err := s.SubmitCRDT("doc", "b", CRDTTypeCounter, []CRDTOp{
		{ID: "b1", Value: "2"},
		{ID: "b2", Value: "1"},
	})
	if !errors.Is(err, ErrCRDTValueRejected) {
		t.Fatalf("batch regression err = %v", err)
	}
	state, _ := s.GetCRDTState("doc")
	if state.Counter != 10 {
		t.Fatalf("counter = %d, want 10 (rejected batches wrote nothing)", state.Counter)
	}
}

// A grow-only set merges as the union of every accepted element, presented in
// ascending order; adding an existing element with a new op id is accepted
// but does not change the union.
func TestCRDTGSetUnionAscending(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("b"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeGSet, []CRDTOp{
		{ID: "a1", Value: "cherry"},
		{ID: "a2", Value: "apple"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("doc", "b", CRDTTypeGSet, []CRDTOp{
		{ID: "b1", Value: "banana"},
		{ID: "b2", Value: "apple"}, // duplicate element, fresh stable id
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("doc", "b", CRDTTypeGSet, []CRDTOp{
		{ID: "b1", Value: "banana"}, // idempotent repost
	}); err != nil {
		t.Fatal(err)
	}

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"apple", "banana", "cherry"}
	if state.Type != CRDTTypeGSet || len(state.Members) != len(want) {
		t.Fatalf("state = %+v", state)
	}
	for i := range want {
		if state.Members[i] != want[i] {
			t.Fatalf("members = %v, want %v", state.Members, want)
		}
	}
}

// The type is fixed by the first accepted batch; declaring the other type
// later is rejected and leaves the state intact. Two concurrent first-of-type
// declares serialize so exactly one wins.
func TestCRDTTypeFixedAndConcurrentDeclare(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "1"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeGSet, []CRDTOp{{ID: "o2", Value: "x"}}); !errors.Is(err, ErrCRDTTypeConflict) {
		t.Fatalf("type change err = %v, want ErrCRDTTypeConflict", err)
	}
	state, _ := s.GetCRDTState("doc")
	if state.Type != CRDTTypeCounter {
		t.Fatalf("type = %q, want counter", state.Type)
	}

	// Concurrent first declares on a fresh document: exactly one of the two
	// types takes effect. Same-type submits then commit normally, but every
	// request declaring the other type is rejected with ErrCRDTTypeConflict.
	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	var counterOK, gsetOK, typeConflicts int
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			typ := CRDTTypeCounter
			if g%2 == 0 {
				typ = CRDTTypeGSet
			}
			_, err := s.SubmitCRDT("race", "a", typ, []CRDTOp{{ID: "op-" + strconv.Itoa(g), Value: "1"}})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				if typ == CRDTTypeCounter {
					counterOK++
				} else {
					gsetOK++
				}
			case errors.Is(err, ErrCRDTTypeConflict):
				typeConflicts++
			default:
				t.Errorf("goroutine %d err = %v", g, err)
			}
		}(g)
	}
	wg.Wait()
	if !((counterOK == n/2 && gsetOK == 0) || (gsetOK == n/2 && counterOK == 0)) || typeConflicts != n/2 {
		t.Fatalf("counterOK=%d gsetOK=%d typeConflicts=%d: exactly one type must take effect and the other %d requests must 409",
			counterOK, gsetOK, typeConflicts, n/2)
	}
	established, _ := s.GetCRDTState("race")
	switch established.Type {
	case CRDTTypeCounter, CRDTTypeGSet:
	default:
		t.Fatalf("established type = %q", established.Type)
	}
}

// An unregistered device is 404-class and a revoked one 403-class, and neither
// gate failure writes anything or needs to reveal state.
func TestCRDTGateDeviceAndPermission(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o1", Value: "1"}}); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SubmitCRDT("doc", "ghost", CRDTTypeCounter, []CRDTOp{{ID: "g1", Value: "1"}}); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if changed, err := s.SetDocumentPermission("doc", "a", false); err != nil || !changed {
		t.Fatalf("revoke changed=%v err=%v", changed, err)
	}
	if _, err := s.SubmitCRDT("doc", "a", CRDTTypeCounter, []CRDTOp{{ID: "o2", Value: "2"}}); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revoked device err = %v, want ErrPermissionDenied", err)
	}
	state, _ := s.GetCRDTState("doc")
	if state.Counter != 1 {
		t.Fatalf("counter = %d, want 1 (rejected writes landed)", state.Counter)
	}
}

// A document with no accepted op has no type and no state; reads are a 404.
func TestCRDTStateMissing(t *testing.T) {
	s := openCRDTStore(t)
	if _, err := s.GetCRDTState("never"); !errors.Is(err, ErrCRDTNotFound) {
		t.Fatalf("err = %v, want ErrCRDTNotFound", err)
	}
}

// Submits and merged results are durable: after reopening the database file
// the fixed type, merged value and all conflict/idempotency judgments persist.
func TestCRDTPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("c", "a", CRDTTypeCounter, []CRDTOp{
		{ID: "a1", Value: "4"},
		{ID: "a2", Value: "9"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("c", "b", CRDTTypeCounter, []CRDTOp{{ID: "b1", Value: "2"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDT("g", "a", CRDTTypeGSet, []CRDTOp{{ID: "g1", Value: "z"}}); err != nil {
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

	counter, err := s2.GetCRDTState("c")
	if err != nil {
		t.Fatal(err)
	}
	if counter.Counter != 11 {
		t.Fatalf("counter after reopen = %d, want 11", counter.Counter)
	}
	gset, err := s2.GetCRDTState("g")
	if err != nil {
		t.Fatal(err)
	}
	if len(gset.Members) != 1 || gset.Members[0] != "z" {
		t.Fatalf("gset after reopen = %+v", gset)
	}

	// Post-restart judgments remain stable.
	if _, err := s2.SubmitCRDT("c", "a", CRDTTypeCounter, []CRDTOp{{ID: "a1", Value: "4"}}); err != nil {
		t.Fatalf("idempotency after restart failed: %v", err)
	}
	if _, err := s2.SubmitCRDT("c", "a", CRDTTypeCounter, []CRDTOp{{ID: "a3", Value: "8"}}); !errors.Is(err, ErrCRDTValueRejected) {
		t.Fatalf("monotonic judgment after restart err = %v", err)
	}
	if _, err := s2.SubmitCRDT("c", "a", CRDTTypeGSet, []CRDTOp{{ID: "x", Value: "x"}}); !errors.Is(err, ErrCRDTTypeConflict) {
		t.Fatalf("fixed type after restart err = %v", err)
	}
}

// Concurrent monotonic submits from many devices all land completely and the
// merge is the sum of maxima, independent of arrival order.
func TestCRDTConcurrentCounterCommutative(t *testing.T) {
	s := openCRDTStore(t)
	const devices = 12
	for d := 0; d < devices; d++ {
		if _, err := s.RegisterDevice("dev-" + strconv.Itoa(d)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for d := 0; d < devices; d++ {
		wg.Add(1)
		go func(d int) {
			defer wg.Done()
			dev := "dev-" + strconv.Itoa(d)
			// Each device pushes increasing contributions out of order across
			// several small batches.
			vals := []int64{int64(d + 1), int64((d + 1) * 3), int64((d + 1) * 7)}
			if d%2 == 0 {
				vals[0], vals[2] = vals[2], vals[0]
			}
			for i, v := range vals {
				op := CRDTOp{ID: dev + "-" + strconv.Itoa(i), Value: strconv.FormatInt(v, 10)}
				if _, err := s.SubmitCRDT("doc", dev, CRDTTypeCounter, []CRDTOp{op}); err != nil {
					// The shuffled batch may legitimately reject the lower
					// value; anything else is a real failure.
					if !errors.Is(err, ErrCRDTValueRejected) {
						t.Errorf("device %s op %v: %v", dev, op, err)
					}
				}
			}
		}(d)
	}
	wg.Wait()

	state, err := s.GetCRDTState("doc")
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	for d := 1; d <= devices; d++ {
		want += int64(d * 7)
	}
	if state.Counter != want {
		t.Fatalf("counter = %d, want %d (sum of per-device maxima)", state.Counter, want)
	}
}
