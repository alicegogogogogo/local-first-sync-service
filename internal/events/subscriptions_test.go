package events_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"time"
)

// drainWait consumes any pending wake (the channel is buffered) so each test
// starts from a quiet state.
func drainWait(ch <-chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

func waitWake(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not wake the subscriber", what)
	}
}

func assertNoWake(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s was woken unexpectedly", what)
	case <-time.After(50 * time.Millisecond):
	}
}

// Every change-producing write path signals a live subscription immediately
// after commit.
func TestSubscriptionWakesOnAllCommitPaths(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	// Seed c1 at cursor 1 and a snapshot at 1 so merge/restore have a base.
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"n":9}`)); err != nil {
		t.Fatal(err)
	}

	ch, _, unregister := s.AddSubscription("doc", "dev-1")
	defer unregister()
	drainWait(ch)

	// Ordinary commit.
	if _, err := s.PostChanges("doc", changes("c2")); err != nil {
		t.Fatal(err)
	}
	waitWake(t, ch, "post")
	drainWait(ch)

	// Merge appends a change.
	if _, err := s.MergeChange("doc", 1, events.Change{ID: "m1", DeviceID: "dev-1", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	waitWake(t, ch, "merge")
	drainWait(ch)

	// Restore appends a change.
	if _, err := s.RestoreSnapshot("doc", "dev-1", "r1", 1); err != nil {
		t.Fatal(err)
	}
	waitWake(t, ch, "restore")
	drainWait(ch)

	// Replay appends a change and shares the cursor space (cursor 5:
	// c1=1, c2=2, merge=3, restore=4).
	results, err := s.ReplayChanges("doc", []events.Change{
		{ID: "rp1", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":1}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].Created || results[0].Cursor != 5 {
		t.Fatalf("replay results = %+v", results)
	}
	waitWake(t, ch, "replay")

	// The observed cursor is a valid read/restart position verbatim.
	rows, next, err := s.ListChanges("doc", 5, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 || next != 5 {
		t.Fatalf("read from observed cursor = %+v next=%d, want empty/5", rows, next)
	}
}

// Idempotent writes add no row and do not signal, so a subscription is pushed
// new changes only.
func TestSubscriptionIdempotentWritesDoNotWake(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	ch, _, unregister := s.AddSubscription("doc", "dev-1")
	defer unregister()
	drainWait(ch)

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	assertNoWake(t, ch, "idempotent repost")

	if _, err := s.MergeChange("doc", 1, events.Change{ID: "c1", DeviceID: "dev-1", Payload: json.RawMessage(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}
	assertNoWake(t, ch, "idempotent merge")
}

// A commit to one document wakes every device subscription on that document
// and none on another document.
func TestSubscriptionCommitScopedToDocument(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	chA, _, unregA := s.AddSubscription("docA", "dev-1")
	defer unregA()
	chAOther, _, unregOther := s.AddSubscription("docA", "other")
	defer unregOther()
	chB, _, unregB := s.AddSubscription("docB", "dev-1")
	defer unregB()
	for _, c := range []<-chan struct{}{chA, chAOther, chB} {
		drainWait(c)
	}

	if _, err := s.PostChanges("docA", changes("a1")); err != nil {
		t.Fatal(err)
	}
	waitWake(t, chA, "docA subscriber")
	waitWake(t, chAOther, "docA other-device subscriber")
	assertNoWake(t, chB, "unrelated document")
}

// A revoke wakes only the one (document, device) pair, not other devices or
// other documents.
func TestSubscriptionRevokeScopedToPair(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("other"); err != nil {
		t.Fatal(err)
	}

	chTarget, revoked, unregTarget := s.AddSubscription("doc", "dev-1")
	defer unregTarget()
	chOtherDevice, _, unregOther := s.AddSubscription("doc", "other")
	defer unregOther()
	chOtherDoc, _, unregOtherDoc := s.AddSubscription("other-doc", "dev-1")
	defer unregOtherDoc()
	for _, c := range []<-chan struct{}{chTarget, chOtherDevice, chOtherDoc} {
		drainWait(c)
	}

	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	// Revoke closes the one-shot revoked channel of only the targeted pair.
	waitWake(t, revoked, "revoked pair")
	assertNoWake(t, chTarget, "wake channel on revoke")
	assertNoWake(t, chOtherDevice, "another device on the same document")
	assertNoWake(t, chOtherDoc, "another document of the revoked device")

	// Stickiness: a racing re-grant does not reopen revoked, so the
	// subscription still must end.
	if _, err := s.SetDocumentPermission("doc", "dev-1", true); err != nil {
		t.Fatal(err)
	}
	select {
	case <-revoked:
	default:
		t.Fatal("revoked channel reopened after a grant")
	}

	// A repeat revoke is idempotent and must not close the channel twice.
	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
}

// Unregister is idempotent, stops further wakes and lets Close return.
func TestSubscriptionUnregisterAndClose(t *testing.T) {
	s, _ := app.Open("")
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	_, _, unregister := s.AddSubscription("doc", "dev-1")
	unregister()
	unregister() // must not panic or double-decrement

	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close after unregister: %v", err)
	}
}

// InterruptWaits/Close wakes every live subscription and Closing reports it.
func TestSubscriptionWokenByClose(t *testing.T) {
	s, _ := app.Open("")
	ch, _, unregister := s.AddSubscription("doc", "dev-1")
	defer unregister()

	go func() {
		time.Sleep(20 * time.Millisecond)
		_ = s.Close()
	}()
	waitWake(t, ch, "store close")
	if !s.Closing() {
		t.Fatal("Closing() = false after close")
	}
}

// A subscription opened while the store is closing is signaled immediately.
func TestSubscriptionAddedWhileClosing(t *testing.T) {
	s, _ := app.Open("")
	s.InterruptWaits()
	ch, _, unregister := s.AddSubscription("doc", "dev-1")
	defer unregister()
	waitWake(t, ch, "late subscription")
}

// Many live subscribers all receive the one commit signal concurrently.
func TestSubscriptionManyConcurrent(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	const n = 50
	chs := make([]<-chan struct{}, n)
	unregs := make([]func(), n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		ch, _, unregister := s.AddSubscription("doc", "dev-1")
		chs[i] = ch
		unregs[i] = unregister
		drainWait(ch)
	}
	defer func() {
		for _, u := range unregs {
			u()
		}
	}()

	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev-1", Payload: json.RawMessage(`{"i":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	for i, ch := range chs {
		wg.Add(1)
		go func(i int, ch <-chan struct{}) {
			defer wg.Done()
			waitWake(t, ch, fmt.Sprintf("subscriber %d", i))
		}(i, ch)
	}
	wg.Wait()
}

// Subscribing, waking and unsubscribing leave no rows, no cursor advance and
// no permission deviation.
func TestSubscriptionWritesNothing(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	ch, _, unregister := s.AddSubscription("doc", "dev-1")
	drainWait(ch)
	if _, err := s.PostChanges("doc", changes("c2")); err != nil {
		t.Fatal(err)
	}
	waitWake(t, ch, "post while subscribed")

	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("subscription changed the log: rows=%d next=%d, want 2/2", len(rows), next)
	}
	ok, err := s.DocumentAuthorized("doc", "dev-1")
	if err != nil || !ok {
		t.Fatalf("subscription changed permission: ok=%v err=%v", ok, err)
	}
	unregister()
}
