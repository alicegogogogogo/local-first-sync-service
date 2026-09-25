package store

import (
	"encoding/json"
	"strconv"
	"sync"
	"testing"
)

func counterSubOp(id, device string, value int64) CRDTOp {
	return CRDTOp{ID: id, DeviceID: device, Value: rawInt(value)}
}

// Opening a subscription returns the current state; a later state-changing
// commit is queued in order, while a no-op commit queues nothing.
func TestCRDTSubscriptionInitialStateAndDrain(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		counterSubOp("a1", "dev-1", 5),
	}); err != nil {
		t.Fatal(err)
	}

	initial, sub, unregister := openSub(t, s, "doc", "dev-1")
	if initial == nil || string(initial.Value) != "5" || initial.Type != CRDTTypeCounter {
		t.Fatalf("initial = %+v, want counter 5", initial)
	}
	if queued := sub.Drain(); queued != nil {
		t.Fatalf("fresh subscription queued %v, want nothing", queued)
	}

	// Advancing commit queues one state; an equal-value new op queues none.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		counterSubOp("a2", "dev-1", 8),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
		counterSubOp("a3", "dev-1", 8),
	}); err != nil {
		t.Fatal(err)
	}
	queued := sub.Drain()
	if len(queued) != 1 || string(queued[0].Value) != "8" {
		t.Fatalf("queued = %v, want exactly [8]", queued)
	}
	if queued := sub.Drain(); queued != nil {
		t.Fatalf("drain not emptied: %v", queued)
	}
	unregister()
}

// A subscription opened before the document's first state gets no initial
// state and then receives the first committed state through its queue.
func TestCRDTSubscriptionWaitsForFirstState(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	initial, sub, unregister := openSub(t, s, "virgin", "dev-1")
	if initial != nil {
		t.Fatalf("initial = %+v, want nil before any op", initial)
	}

	if _, err := s.SubmitCRDTOps("virgin", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	}); err != nil {
		t.Fatal(err)
	}
	queued := sub.Drain()
	if len(queued) != 1 || queued[0].Type != CRDTTypeGSet || string(queued[0].Value) != `["x"]` {
		t.Fatalf("queued = %v, want one gset [x]", queued)
	}
	unregister()
}

// Many concurrent state-changing commits reach every subscriber exactly once
// each, in strictly increasing order, ending at the total; a subscription on
// another document receives nothing.
func TestCRDTSubscriptionConcurrentCommitOrder(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	const devices = 6
	const perDevice = 4
	for d := 0; d < devices; d++ {
		if _, err := s.RegisterDevice("dev-" + strconv.Itoa(d)); err != nil {
			t.Fatal(err)
		}
	}

	subs := make([]*CRDTSubscription, 3)
	for i := range subs {
		_, sub, unregister := openSub(t, s, "doc", "dev-1")
		defer unregister()
		subs[i] = sub
	}
	other, otherUnregister := openSubOther(t, s, "other", "dev-1")
	defer otherUnregister()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for d := 0; d < devices; d++ {
		device := "dev-" + strconv.Itoa(d)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for v := 1; v <= perDevice; v++ {
				if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, []CRDTOp{
					counterSubOp(device+"-"+strconv.Itoa(v), device, int64(v)),
				}); err != nil {
					t.Errorf("submit %s %d: %v", device, v, err)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	wantTotal := int64(devices * perDevice)
	for i, sub := range subs {
		var last int64
		count := 0
		// Coalesced wakes are fine: Drain always returns the whole queue.
		for {
			for _, st := range sub.Drain() {
				var n int64
				if err := json.Unmarshal(st.Value, &n); err != nil {
					t.Fatal(err)
				}
				if n <= last {
					t.Fatalf("subscriber %d non-increasing %d after %d", i, n, last)
				}
				last = n
				count++
			}
			if last == wantTotal {
				break
			}
			<-sub.Wake()
		}
		if count != devices*perDevice {
			t.Fatalf("subscriber %d got %d states, want %d", i, count, devices*perDevice)
		}
	}
	if queued := other.Drain(); queued != nil {
		t.Fatalf("other-document subscription received %v", queued)
	}
}

// A revoke after opening closes the subscription's revoked channel once and
// stickily; a subsequent grant leaves it closed. Other devices are untouched.
func TestCRDTSubscriptionRevokeSignal(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	_, sub1, unreg1 := openSub(t, s, "doc", "dev-1")
	defer unreg1()
	_, sub2, unreg2 := openSub(t, s, "doc", "dev-2")
	defer unreg2()

	if changed, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil || !changed {
		t.Fatalf("revoke changed=%v err=%v", changed, err)
	}
	select {
	case <-sub1.Revoked():
	default:
		t.Fatal("revoked channel not closed for dev-1")
	}
	select {
	case <-sub2.Revoked():
		t.Fatal("dev-2 subscription was revoked by dev-1's revoke")
	default:
	}

	// A grant must not reopen the sticky channel.
	if _, err := s.SetDocumentPermission("doc", "dev-1", true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub1.Revoked():
	default:
		t.Fatal("sticky revoked channel reopened after grant")
	}
}

// After the store starts closing, a new subscription wakes immediately and a
// live one receives a final wake, both mapping to the going-away close.
func TestCRDTSubscriptionClosingWakes(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	_, live, unreg := openSub(t, s, "doc", "dev-1")

	s.InterruptWaits()

	select {
	case <-live.Wake():
	default:
		t.Fatal("live subscription not woken on close")
	}
	unreg()

	_, late, lateUnreg := openSub(t, s, "doc", "dev-1")
	defer lateUnreg()
	select {
	case <-late.Wake():
	default:
		t.Fatal("late subscription not woken immediately on closing store")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func openSub(t *testing.T, s *Store, doc, device string) (*CRDTState, *CRDTSubscription, func()) {
	t.Helper()
	state, sub, unregister, err := s.OpenCRDTSubscription(doc, device)
	if err != nil {
		t.Fatal(err)
	}
	return state, sub, unregister
}

func openSubOther(t *testing.T, s *Store, doc, device string) (*CRDTSubscription, func()) {
	t.Helper()
	_, sub, unregister, err := s.OpenCRDTSubscription(doc, device)
	if err != nil {
		t.Fatal(err)
	}
	return sub, unregister
}
