package store

import (
	"fmt"
	"testing"
	"time"
)

// waitCrdtWake blocks until a queued state (or a closing signal) wakes sub.
func waitCrdtWake(t *testing.T, sub *CRDTSubscription, what string) {
	t.Helper()
	select {
	case <-sub.Wakes():
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not wake the crdt subscriber", what)
	}
}

// assertNoCrdtWake fails if sub receives a wake within a short window.
func assertNoCrdtWake(t *testing.T, sub *CRDTSubscription, what string) {
	t.Helper()
	select {
	case <-sub.Wakes():
		t.Fatalf("%s woke the crdt subscriber unexpectedly", what)
	case <-time.After(50 * time.Millisecond):
	}
}

// drainCrdtWake consumes a pending coalesced wake without draining states.
func drainCrdtWake(sub *CRDTSubscription) {
	select {
	case <-sub.Wakes():
	default:
	}
}

func counterBatch(device string, id string, val int64) []CRDTOp {
	return []CRDTOp{{ID: id, DeviceID: device, Value: rawInt(val)}}
}

// A subscription on a document with no CRDT operations reports no initial
// state and stays quiet; the first accepted batch becomes its first queued
// state, and later merged values queue in commit order.
func TestCRDTSubscriptionFirstStateThenChanges(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	sub, _, hasState, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()
	if hasState {
		t.Fatal("a document with no CRDT operations reported an initial state")
	}

	// No state yet: an unrelated batch elsewhere is silent on this document.
	assertNoCrdtWake(t, sub, "before any state")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 5)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "first batch")
	states := sub.Drain()
	if len(states) != 1 || states[0].Type != CRDTTypeCounter || string(states[0].Value) != "5" {
		t.Fatalf("first queued states = %+v, want one counter/5", states)
	}

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a2", 8)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "advance batch")
	states = sub.Drain()
	if len(states) != 1 || string(states[0].Value) != "8" {
		t.Fatalf("advance queued states = %+v, want one counter/8", states)
	}
}

// Only batches that actually change the merged value notify: an equal
// contribution under a new id, an idempotent repost and a rejected regression
// queue nothing.
func TestCRDTSubscriptionNoNotificationForNoopBatches(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	sub, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 5)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "first batch")
	sub.Drain()

	// Equal contribution with a new id: accepted but the merged sum is
	// unchanged, so no notification.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a2", 5)); err != nil {
		t.Fatal(err)
	}
	assertNoCrdtWake(t, sub, "equal contribution")

	// Idempotent repost of the same id/value: no notification.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 5)); err != nil {
		t.Fatal(err)
	}
	assertNoCrdtWake(t, sub, "idempotent repost")

	// Rejected regression: an error and no notification.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a3", 4)); err == nil {
		t.Fatal("regression was accepted")
	}
	assertNoCrdtWake(t, sub, "rejected regression")

	// The next genuine advance still queues exactly one state.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a4", 7)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "genuine advance")
	states := sub.Drain()
	if len(states) != 1 || string(states[0].Value) != "7" {
		t.Fatalf("queued states = %+v, want one counter/7", states)
	}
}

// A gset subscription queues a state only when the union gains elements;
// re-adding an existing element (even under a new op id) changes nothing.
func TestCRDTSubscriptionGSetOnlyUnionGrowthNotifies(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	sub, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()

	gset := func(id string, elements ...string) []CRDTOp {
		return []CRDTOp{{ID: id, DeviceID: "dev-1", Elements: elements}}
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, gset("g1", "banana", "apple")); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "first gset batch")
	states := sub.Drain()
	if len(states) != 1 || string(states[0].Value) != `["apple","banana"]` {
		t.Fatalf("first gset state = %+v, want sorted [apple banana]", states)
	}

	// All elements already present: new id, no union growth, no notification.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, gset("g2", "apple")); err != nil {
		t.Fatal(err)
	}
	assertNoCrdtWake(t, sub, "re-added element")

	// One old and one new element: only one queued state, sorted.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, gset("g3", "banana", "cherry")); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "union growth")
	states = sub.Drain()
	if len(states) != 1 || string(states[0].Value) != `["apple","banana","cherry"]` {
		t.Fatalf("grown gset state = %+v, want three sorted elements", states)
	}
}

// A subscription taken after the document already has state sees that state
// as its initial value, and not again in the queue.
func TestCRDTSubscriptionInitialStateIsCurrent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 5)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-2", "b1", 3)); err != nil {
		t.Fatal(err)
	}

	sub, initial, hasState, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()
	if !hasState || initial.Type != CRDTTypeCounter || string(initial.Value) != "8" {
		t.Fatalf("initial state = %+v has=%v, want counter/8", initial, hasState)
	}
	if queued := sub.Drain(); len(queued) != 0 {
		t.Fatalf("initial state was also queued: %+v", queued)
	}
	drainCrdtWake(sub)

	// A later advance is queued once.
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-2", "b2", 10)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "post-subscribe advance")
	states := sub.Drain()
	if len(states) != 1 || string(states[0].Value) != "15" {
		t.Fatalf("queued states = %+v, want one counter/15", states)
	}
}

// Every subscriber on the document (any device) receives every changed
// state; subscribers of another document receive none.
func TestCRDTSubscriptionFanOutScopedToDocument(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	subA1, _, _, err := s.SubscribeCRDT("docA", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer subA1.Unregister()
	subA2, _, _, err := s.SubscribeCRDT("docA", "dev-2")
	if err != nil {
		t.Fatal(err)
	}
	defer subA2.Unregister()
	subB, _, _, err := s.SubscribeCRDT("docB", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer subB.Unregister()

	if _, err := s.SubmitCRDTOps("docA", CRDTTypeCounter, counterBatch("dev-1", "a1", 2)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, subA1, "docA subscriber 1")
	waitCrdtWake(t, subA2, "docA subscriber 2")
	assertNoCrdtWake(t, subB, "docB subscriber")

	for _, sub := range []*CRDTSubscription{subA1, subA2} {
		states := sub.Drain()
		if len(states) != 1 || string(states[0].Value) != "2" {
			t.Fatalf("docA fan-out states = %+v, want one counter/2", states)
		}
	}
}

// Rapid successive commits are all delivered to one subscriber, once each, in
// commit order, even though wake signals may coalesce.
func TestCRDTSubscriptionCommitOrderNoLoss(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	sub, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()

	for i, v := range []int64{1, 2, 2, 3, 5, 8} {
		if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter,
			counterBatch("dev-1", opID("a", i), v)); err != nil {
			t.Fatal(err)
		}
	}
	waitCrdtWake(t, sub, "batch of commits")

	// Only distinct merged values are queued: 1,2,3,5,8 (the equal 2 is a
	// no-op), in that order.
	states := sub.Drain()
	want := []string{"1", "2", "3", "5", "8"}
	if len(states) != len(want) {
		t.Fatalf("got %d queued states %+v, want %d", len(states), states, len(want))
	}
	for i, w := range want {
		if string(states[i].Value) != w {
			t.Fatalf("queued state %d = %s, want %s (all: %+v)", i, states[i].Value, w, states)
		}
	}
}

func opID(prefix string, i int) string {
	return fmt.Sprintf("%s-%d", prefix, i)
}

// A batch rejected for a fixed-type mismatch commits nothing and notifies
// nobody.
func TestCRDTSubscriptionRejectedTypeMismatchNoNotification(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	sub, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 1)); err != nil {
		t.Fatal(err)
	}
	waitCrdtWake(t, sub, "counter first batch")
	sub.Drain()

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeGSet, []CRDTOp{
		{ID: "g1", DeviceID: "dev-1", Elements: []string{"x"}},
	}); err == nil {
		t.Fatal("a gset batch against a counter document was accepted")
	}
	assertNoCrdtWake(t, sub, "rejected type-mismatch batch")
}

// A revoke closes only the matching (document, device) revoked channels; a
// later grant never reopens them.
func TestCRDTSubscriptionRevokeScopedAndSticky(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	target, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Unregister()
	otherDevice, _, _, err := s.SubscribeCRDT("doc", "dev-2")
	if err != nil {
		t.Fatal(err)
	}
	defer otherDevice.Unregister()
	otherDoc, _, _, err := s.SubscribeCRDT("other", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer otherDoc.Unregister()

	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.Revoked():
	case <-time.After(2 * time.Second):
		t.Fatal("revoked pair was not signaled")
	}
	assertNoCrdtWake(t, otherDevice, "another device")
	assertNoCrdtWake(t, otherDoc, "another document")

	if _, err := s.SetDocumentPermission("doc", "dev-1", true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-target.Revoked():
		// Still closed (a receive on a closed channel succeeds): sticky.
	default:
		t.Fatal("revoked channel reopened after a grant")
	}
}

// Unregister is idempotent, stops further queueing, and Close returns once
// every live subscription is unregistered.
func TestCRDTSubscriptionUnregisterAndClose(t *testing.T) {
	s, _ := Open("")
	registerDevices(t, s, "dev-1")

	sub, _, _, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	sub.Unregister()
	sub.Unregister() // must not panic or double-decrement

	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 1)); err != nil {
		t.Fatal(err)
	}
	if queued := sub.Drain(); len(queued) != 0 {
		t.Fatalf("an unregistered subscription was queued to: %+v", queued)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close after unregister: %v", err)
	}
}

// InterruptWaits marks the CRDT registry closing and wakes live subscribers
// once so the server can end them with 1001; committed state stays readable.
func TestCRDTSubscriptionInterruptWaitsWakesAndCloses(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")
	if _, err := s.SubmitCRDTOps("doc", CRDTTypeCounter, counterBatch("dev-1", "a1", 4)); err != nil {
		t.Fatal(err)
	}

	sub, initial, hasState, err := s.SubscribeCRDT("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unregister()
	if !hasState || string(initial.Value) != "4" {
		t.Fatalf("initial = %+v has=%v, want counter/4", initial, hasState)
	}

	s.InterruptWaits()
	waitCrdtWake(t, sub, "closing store")
	if !s.Closing() {
		t.Fatal("Closing() = false after InterruptWaits")
	}
	state, err := s.GetCRDTState("doc")
	if err != nil || string(state.Value) != "4" {
		t.Fatalf("committed state after interrupt = %+v err=%v", state, err)
	}
}
