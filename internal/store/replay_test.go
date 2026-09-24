package store

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestReplayChangesCreatesAndIsIdempotent(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	results, err := s.ReplayChanges("doc", "dev", []Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"n":1}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`[1,true,null]`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || !results[0].Created || results[0].Cursor != 1 ||
		!results[1].Created || results[1].Cursor != 2 {
		t.Fatalf("results = %+v", results)
	}

	// Identical retry: both idempotent with the first cursors, no new rows.
	again, err := s.ReplayChanges("doc", "dev", []Change{
		{ID: "a", DeviceID: "dev", Payload: json.RawMessage(`{"n":1.0}`)},
		{ID: "b", DeviceID: "dev", Payload: json.RawMessage(`[1,true,null]`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range again {
		if r.Created || r.Cursor != int64(i+1) {
			t.Fatalf("idempotent item %d = %+v", i, r)
		}
	}
	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || next != 2 {
		t.Fatalf("log grew on idempotent replay: %d/%d", len(rows), next)
	}
}

func TestReplayChangesConflictsAndGuards(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []Change{
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"k":"v"}`)},
	}); err != nil {
		t.Fatal(err)
	}

	// Payload mismatch -> ErrConflict, whole batch rejected.
	_, err = s.ReplayChanges("doc", "dev", []Change{
		{ID: "new", DeviceID: "dev", Payload: json.RawMessage(`1`)},
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"k":"other"}`)},
	})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "x" {
		t.Fatalf("err = %v, want ErrConflict{x}", err)
	}
	if rows, next, _ := s.ListChanges("doc", 0, 100); len(rows) != 1 || next != 1 {
		t.Fatalf("conflict batch wrote: %d/%d", len(rows), next)
	}

	// A different device replaying the same (id, payload) is a conflict too.
	if _, err := s.RegisterDevice("other"); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReplayChanges("doc", "other", []Change{
		{ID: "x", DeviceID: "other", Payload: json.RawMessage(`{"k":"v"}`)},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("device mismatch err = %v, want conflict", err)
	}

	// Unregistered device -> ErrDeviceNotFound, nothing written.
	_, err = s.ReplayChanges("doc", "ghost", []Change{
		{ID: "g", DeviceID: "ghost", Payload: json.RawMessage(`1`)},
	})
	if !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}

	// Revoked device -> ErrPermissionDenied, nothing written.
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	_, err = s.ReplayChanges("doc", "dev", []Change{
		{ID: "z", DeviceID: "dev", Payload: json.RawMessage(`1`)},
	})
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	if rows, next, _ := s.ListChanges("doc", 0, 100); len(rows) != 1 || next != 1 {
		t.Fatalf("guarded replay wrote: %d/%d", len(rows), next)
	}
}

func TestReplayChangesConcurrentCursors(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	const n = 30
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.ReplayChanges("doc", "dev", []Change{
				{ID: "r" + strconv.Itoa(i), DeviceID: "dev", Payload: json.RawMessage(`1`)},
			})
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}

	rows, next, err := s.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != n || next != n {
		t.Fatalf("rows=%d next=%d, want %d/%d", len(rows), next, n, n)
	}
	seen := make(map[int64]bool, n)
	for _, r := range rows {
		if r.Cursor < 1 || r.Cursor > n || seen[r.Cursor] {
			t.Fatalf("bad/duplicate cursor %d", r.Cursor)
		}
		seen[r.Cursor] = true
	}
}

// A subscription receives a signal after a normal batch, a merge and a
// restore commit; signals coalesce on the capacity-1 channel.
func TestChangeNotifierWakesOnCommits(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	ch := make(chan struct{}, 1)
	unsub := s.SubscribeChanges("doc", ch)

	notify := func(label string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf("no wake after %s", label)
		}
	}

	if _, err := s.PostChanges("doc", []Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	notify("post")

	if _, err := s.MergeChange("doc", 1, Change{
		ID: "c2", DeviceID: "dev", Payload: json.RawMessage(`{"b":2}`),
	}); err != nil {
		t.Fatal(err)
	}
	notify("merge")

	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestoreSnapshot("doc", "dev", "r1", 2); err != nil {
		t.Fatal(err)
	}
	notify("restore")

	// Another document's commits do not wake this subscription.
	if _, err := s.PostChanges("other", []Change{
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`1`)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("subscription woke for a different document")
	case <-time.After(50 * time.Millisecond):
	}

	unsub()
	if _, err := s.PostChanges("doc", []Change{
		{ID: "c3", DeviceID: "dev", Payload: json.RawMessage(`1`)},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
		t.Fatal("subscription woke after unsubscribe")
	case <-time.After(50 * time.Millisecond):
	}
}

// BeginShutdown (and Close) fans out to every waiter and closes the shutdown
// channel, so long polls cancel instead of waiting out their deadline.
func TestChangeNotifierShutdownWakesWaiters(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{}, 1)
	_ = s.SubscribeChanges("doc", ch)

	s.BeginShutdown()

	select {
	case <-s.ShutdownChannel():
	case <-time.After(time.Second):
		t.Fatal("shutdown channel not closed")
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("waiter not woken on shutdown")
	}

	// Idempotent: a second BeginShutdown and Close must not panic.
	s.BeginShutdown()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// A subscription after shutdown signals immediately rather than blocking.
	ch2 := make(chan struct{}, 1)
	_ = s.SubscribeChanges("doc2", ch2)
	select {
	case <-ch2:
	case <-time.After(time.Second):
		t.Fatal("post-shutdown subscription did not signal immediately")
	}
}
