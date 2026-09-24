package store

import (
	"testing"
	"time"
)

// A subscription registered for a document is woken when a change-bearing
// commit lands on that document, and not woken by a commit to another
// document.
func TestSubscriptionWakesOnCommit(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	sub, ok := s.Subscribe("doc", "dev")
	if !ok {
		t.Fatal("Subscribe reported closing store")
	}
	defer sub.Close()

	select {
	case <-sub.Signal():
		t.Fatal("subscription signaled before any commit")
	default:
	}

	if _, err := s.PostChanges("other", changes("x")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Signal():
		t.Fatal("commit to another document woke the subscription")
	default:
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Signal():
	case <-time.After(time.Second):
		t.Fatal("commit did not wake the subscription")
	}
}

// The signal is coalescing: a burst of commits collapses into a single pending
// marker, and a re-read from the last cursor still observes every row — so no
// committed change is lost.
func TestSubscriptionSignalCoalescesButNoRowLost(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	sub, ok := s.Subscribe("doc", "dev")
	if !ok {
		t.Fatal("Subscribe failed")
	}
	defer sub.Close()

	// Drain nothing; commit three times without consuming. Each notify is a
	// non-blocking send on a capacity-one channel.
	for _, id := range []string{"c1", "c2", "c3"} {
		if _, err := s.PostChanges("doc", changes(id)); err != nil {
			t.Fatal(err)
		}
	}

	<-sub.Signal()
	select {
	case <-sub.Signal():
		t.Fatal("expected the burst to coalesce into one marker")
	default:
	}

	// The handler model: re-read from the last delivered cursor. All three
	// rows are present in ascending order.
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || next != 3 {
		t.Fatalf("rows=%d next=%d, want 3/3", len(rows), next)
	}
	for i, r := range rows {
		if r.Cursor != int64(i+1) {
			t.Fatalf("row %d cursor = %d, want ascending", i, r.Cursor)
		}
	}
}

// Merge, restore and replay commits all wake a subscription.
func TestSubscriptionWakesOnMergeRestoreReplay(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	sub, ok := s.Subscribe("doc", "dev")
	if !ok {
		t.Fatal("Subscribe failed")
	}
	defer sub.Close()

	if _, err := s.MergeChange("doc", 0, Change{ID: "m", DeviceID: "dev", Payload: []byte(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(sub, time.Second) {
		t.Fatal("merge did not wake subscription")
	}

	if _, err := s.PutSnapshot("doc", 1, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestoreSnapshot("doc", "dev", "rs", 1); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(sub, time.Second) {
		t.Fatal("restore did not wake subscription")
	}

	if _, err := s.ReplayChanges("doc", []Change{{ID: "rp", DeviceID: "dev", Payload: []byte(`{"b":2}`)}}); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(sub, time.Second) {
		t.Fatal("replay did not wake subscription")
	}
}

// Only the revoked device's subscriptions are signaled; another device's
// subscription keeps running.
func TestSubscriptionRevocationTargetsDevice(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("other"); err != nil {
		t.Fatal(err)
	}

	a, ok := s.Subscribe("doc", "dev")
	if !ok {
		t.Fatal("Subscribe a failed")
	}
	defer a.Close()
	b, ok := s.Subscribe("doc", "other")
	if !ok {
		t.Fatal("Subscribe b failed")
	}
	defer b.Close()

	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	if !waitSignal(a, time.Second) {
		t.Fatal("revoked device subscription was not signaled")
	}
	select {
	case <-b.Signal():
		t.Fatal("the other device's subscription was signaled")
	case <-time.After(50 * time.Millisecond):
	}
}

// Once the store is closing, Subscribe refuses new subscriptions and existing
// ones are pinged; Close waits for them to release.
func TestSubscriptionShutdownSignalsAndDrains(t *testing.T) {
	s, _ := Open("")

	sub, ok := s.Subscribe("doc", "dev")
	if !ok {
		t.Fatal("Subscribe failed")
	}

	released := make(chan struct{})
	go func() {
		<-sub.Signal()
		if !s.Closing() {
			t.Error("woken subscription did not observe a closing store")
		}
		sub.Close()
		close(released)
	}()

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()

	select {
	case <-released:
	case <-time.After(2 * time.Second):
		_ = s.Close()
		t.Fatal("handler did not release the subscription")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the subscription released")
	}

	if _, ok := s.Subscribe("doc", "dev"); ok {
		t.Fatal("Subscribe succeeded after close")
	}
}

func waitSignal(sub *Subscription, d time.Duration) bool {
	select {
	case <-sub.Signal():
		return true
	case <-time.After(d):
		return false
	}
}
