package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// waitResult captures one WaitForChanges outcome for the goroutine tests.
type waitResult struct {
	changes  []ListedChange
	next     int64
	timedOut bool
	err      error
}

func waitInBackground(ctx context.Context, s *Store, doc string, after, limit int64, wait time.Duration) <-chan waitResult {
	ch := make(chan waitResult, 1)
	go func() {
		changes, next, timedOut, err := s.WaitForChanges(ctx, doc, after, limit, wait)
		ch <- waitResult{changes: changes, next: next, timedOut: timedOut, err: err}
	}()
	return ch
}

func TestPollReturnsExistingRowsImmediately(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}

	ch := waitInBackground(context.Background(), s, "doc", 0, 100, 5*time.Second)
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("err = %v", res.err)
		}
		if len(res.changes) != 2 || res.next != 2 || res.timedOut {
			t.Fatalf("result = %+v rows=%v", res, res.changes)
		}
	case <-time.After(time.Second):
		t.Fatal("poll blocked despite existing rows")
	}
}

func TestPollUnknownDocumentReturnsImmediately(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// A long wait must return at once for a document that has no rows.
	ch := waitInBackground(context.Background(), s, "ghost", 0, 100, 30*time.Second)
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("err = %v", res.err)
		}
		if len(res.changes) != 0 || res.next != 0 || res.timedOut {
			t.Fatalf("unknown doc result = %+v, want empty/0/false", res)
		}
	case <-time.After(time.Second):
		t.Fatal("poll blocked for unknown document")
	}
}

func TestPollWakesOnNewCommit(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	ch := waitInBackground(context.Background(), s, "doc", 1, 100, 5*time.Second)

	// Sanity: it parks rather than answering immediately.
	select {
	case res := <-ch:
		t.Fatalf("poll returned before the commit: %+v", res)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := s.PostChanges("doc", changes("c2")); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("err = %v", res.err)
		}
		if res.timedOut {
			t.Fatal("timedOut = true, want false after wake")
		}
		if len(res.changes) != 1 || res.changes[0].ID != "c2" || res.next != 2 {
			t.Fatalf("woken page = %+v next=%d", res.changes, res.next)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("commit did not wake the poll")
	}
}

func TestPollWakesOnMergeAndRestore(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}
	// Snapshot at cursor 1 so a restore can follow.
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"n":9}`)); err != nil {
		t.Fatal(err)
	}

	ch := waitInBackground(context.Background(), s, "doc", 1, 100, 5*time.Second)
	if _, err := s.MergeChange("doc", 1, Change{ID: "m1", DeviceID: "dev-1", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-ch:
		if res.err != nil || len(res.changes) != 1 || res.changes[0].ID != "m1" {
			t.Fatalf("merge wake = %+v %v", res.changes, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("merge did not wake the poll")
	}

	ch = waitInBackground(context.Background(), s, "doc", 2, 100, 5*time.Second)
	if _, err := s.RestoreSnapshot("doc", "dev-1", "r1", 1); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-ch:
		if res.err != nil || len(res.changes) != 1 || res.changes[0].ID != "r1" {
			t.Fatalf("restore wake = %+v %v", res.changes, res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not wake the poll")
	}
}

func TestPollTimeoutEchoesCursor(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1", "c2")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	changes, next, timedOut, err := s.WaitForChanges(context.Background(), "doc", 2, 100, 60*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("poll returned before its wait elapsed")
	}
	if len(changes) != 0 || next != 2 || !timedOut {
		t.Fatalf("timeout result = rows=%v next=%d timedOut=%v, want empty/2/true", changes, next, timedOut)
	}
}

func TestPollZeroWaitOnCaughtUpDocument(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	changes, next, timedOut, err := s.WaitForChanges(context.Background(), "doc", 1, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 0 || next != 1 || !timedOut {
		t.Fatalf("zero wait = rows=%v next=%d timedOut=%v, want empty/1/true", changes, next, timedOut)
	}
}

func TestPollCanceledByClient(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := waitInBackground(ctx, s, "doc", 1, 100, 30*time.Second)
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case res := <-ch:
		if !errors.Is(res.err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not unblock the poll")
	}

	// A later commit must not panic by closing a channel whose waiter left.
	if _, err := s.PostChanges("doc", changes("c2")); err != nil {
		t.Fatal(err)
	}
}

func TestPollWokenByStoreClose(t *testing.T) {
	s, _ := Open("")
	if _, err := s.PostChanges("doc", changes("c1")); err != nil {
		t.Fatal(err)
	}

	ch := waitInBackground(context.Background(), s, "doc", 1, 100, 30*time.Second)
	time.Sleep(50 * time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case res := <-ch:
		if !errors.Is(res.err, ErrStoreClosing) {
			t.Fatalf("err = %v, want ErrStoreClosing", res.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not wake the poll")
	}
}

func TestReplayNewBatchSharesCursorSpace(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	// One ordinary commit first: replay must continue the cursor sequence.
	if _, err := s.PostChanges("doc", []Change{
		{ID: "p1", DeviceID: "dev", Payload: json.RawMessage(`{"n":0}`)},
	}); err != nil {
		t.Fatal(err)
	}

	results, err := s.ReplayChanges("doc", []Change{
		{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{"n":1}`)},
		{ID: "r2", DeviceID: "dev", Payload: json.RawMessage(`{"n":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %v", results)
	}
	for i, r := range results {
		want := int64(i + 2)
		if !r.Created || r.Cursor != want {
			t.Fatalf("result %d = %+v, want created cursor %d", i, r, want)
		}
	}

	// Repeating the same batch is idempotent: no new cursors, original ones.
	again, err := s.ReplayChanges("doc", []Change{
		{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{ "n": 1 }`)},
		{ID: "r2", DeviceID: "dev", Payload: json.RawMessage(`{"n":2.0}`)},
	})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if again[0].Created || again[0].Cursor != 2 || again[1].Created || again[1].Cursor != 3 {
		t.Fatalf("idempotent results = %+v", again)
	}

	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || next != 3 {
		t.Fatalf("log = %+v next=%d, want 3 contiguous rows", rows, next)
	}
}

func TestReplayUnknownDeviceAndRevoked(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []Change{
		{ID: "p1", DeviceID: "dev", Payload: json.RawMessage(`{"n":0}`)},
	}); err != nil {
		t.Fatal(err)
	}

	ops := []Change{{ID: "r1", DeviceID: "stranger", Payload: json.RawMessage(`{"n":1}`)}}

	// Unregistered device: 404-class error, zero writes.
	if _, err := s.ReplayChanges("doc", ops); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}

	// Revoke the registered device's permission: 403-class error, zero writes.
	if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	revoked := []Change{{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{"n":1}`)}}
	if _, err := s.ReplayChanges("doc", revoked); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}

	// Neither rejected replay wrote anything.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("writes leaked: rows=%d next=%d", len(rows), next)
	}

	// Grant restores access; the same op now commits with cursor 2.
	if _, err := s.SetDocumentPermission("doc", "dev", true); err != nil {
		t.Fatal(err)
	}
	results, err := s.ReplayChanges("doc", revoked)
	if err != nil {
		t.Fatalf("replay after grant: %v", err)
	}
	if !results[0].Created || results[0].Cursor != 2 {
		t.Fatalf("post-grant result = %+v", results[0])
	}
}

func TestReplayConflictAndBatchZeroWrite(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("other"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", []Change{
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
	}); err != nil {
		t.Fatal(err)
	}

	batch := []Change{
		{ID: "new1", DeviceID: "dev", Payload: json.RawMessage(`{"v":2}`)},
		{ID: "x", DeviceID: "dev", Payload: json.RawMessage(`{"v":99}`)},
	}
	_, err := s.ReplayChanges("doc", batch)
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "x" {
		t.Fatalf("err = %v, want ErrConflict{x}", err)
	}

	// The whole batch aborted, including the otherwise-new id.
	rows, next, _ := s.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("conflict leaked writes: rows=%d next=%d", len(rows), next)
	}

	// A device mismatch on the existing id is likewise a conflict.
	_, err = s.ReplayChanges("doc", []Change{
		{ID: "x", DeviceID: "other", Payload: json.RawMessage(`{"v":1}`)},
	})
	if !errors.As(err, &conflict) {
		t.Fatalf("device mismatch err = %v", err)
	}
}

func TestReplayConcurrentCursorsContiguous(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	const writers = 20
	const perWriter = 5
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			ops := make([]Change, 0, perWriter)
			for i := 0; i < perWriter; i++ {
				ops = append(ops, Change{
					ID:       fmt.Sprintf("w%02d-o%02d", w, i),
					DeviceID: "dev",
					Payload:  json.RawMessage(fmt.Sprintf(`{"w":%d,"i":%d}`, w, i)),
				})
			}
			if _, err := s.ReplayChanges("doc", ops); err != nil {
				errs <- err
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	rows, next, err := s.ListChanges("doc", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != writers*perWriter || next != int64(writers*perWriter) {
		t.Fatalf("rows=%d next=%d, want %d/%d", len(rows), next, writers*perWriter, writers*perWriter)
	}
	seenCursors := make(map[int64]bool, len(rows))
	for _, r := range rows {
		if seenCursors[r.Cursor] {
			t.Fatalf("cursor %d allocated twice", r.Cursor)
		}
		seenCursors[r.Cursor] = true
	}
	for c := int64(1); c <= next; c++ {
		if !seenCursors[c] {
			t.Fatalf("cursor %d missing from the contiguous range", c)
		}
	}
}

func TestReplayPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReplayChanges("doc", []Change{
		{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{"v":1}`)},
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

	// Identical replay is still idempotent with the first cursor.
	results, err := s2.ReplayChanges("doc", []Change{
		{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{"v":1.0}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("post-restart idempotent = %+v", results[0])
	}

	// A differing payload is still a conflict after restart.
	if _, err := s2.ReplayChanges("doc", []Change{
		{ID: "r1", DeviceID: "dev", Payload: json.RawMessage(`{"v":2}`)},
	}); err == nil {
		t.Fatal("conflict expected after restart")
	}

	// Registration and the revoked/granted state also survive.
	if _, err := s2.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	if _, err := s3.ReplayChanges("doc", []Change{
		{ID: "r2", DeviceID: "dev", Payload: json.RawMessage(`{"v":3}`)},
	}); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("revocation did not survive restart: %v", err)
	}
}
