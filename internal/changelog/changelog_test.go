package changelog

import (
	"context"
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

// --- test infrastructure ---------------------------------------------------

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

// fakeGate accepts the devices it knows about and denies the documents in
// denied; every decision is observable through the recorded calls.
type fakeGate struct {
	mu              sync.Mutex
	devices         map[string]bool
	denied          map[string]map[string]bool // document -> device set
	permissionCalls int
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

func (g *fakeGate) deny(documentID, deviceID string) {
	if g.denied[documentID] == nil {
		g.denied[documentID] = make(map[string]bool)
	}
	g.denied[documentID][deviceID] = true
}

func (g *fakeGate) CheckDeviceTx(_ *sql.Tx, deviceID string) error {
	if g.devices[deviceID] {
		return nil
	}
	return errNotRegistered
}

func (g *fakeGate) CheckPermissionTx(_ *sql.Tx, documentID, deviceID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.permissionCalls++
	if g.denied[documentID] != nil && g.denied[documentID][deviceID] {
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

func ch(id, device string, payload string) Change {
	return Change{ID: id, DeviceID: device, Payload: json.RawMessage(payload)}
}

// --- normal path: post / list / idempotency --------------------------------

func TestPostAndListBasic(t *testing.T) {
	svc := newService(t, nil)

	results, err := svc.PostChanges("doc", []Change{
		ch("c1", "dev", `{"a":1}`),
		ch("c2", "dev", `[1,2]`),
	})
	if err != nil {
		t.Fatalf("PostChanges: %v", err)
	}
	if len(results) != 2 ||
		!results[0].Created || results[0].Cursor != 1 || results[0].ID != "c1" ||
		!results[1].Created || results[1].Cursor != 2 || results[1].ID != "c2" {
		t.Fatalf("unexpected results: %+v", results)
	}

	listed, next, err := svc.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatalf("ListChanges: %v", err)
	}
	if next != 2 || len(listed) != 2 {
		t.Fatalf("next = %d, len = %d; want 2, 2", next, len(listed))
	}
	if listed[0].ID != "c1" || listed[0].DeviceID != "dev" ||
		string(listed[0].Payload) != `{"a":1}` || listed[0].Cursor != 1 {
		t.Fatalf("unexpected first row: %+v", listed[0])
	}
}

func TestIdempotentRepost(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `{"a":1,"b":2}`)}); err != nil {
		t.Fatalf("PostChanges: %v", err)
	}
	// Reordered keys and integer-vs-float formatting compare by decoded value.
	results, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `{"b":2.0,"a":1}`)})
	if err != nil {
		t.Fatalf("idempotent repost: %v", err)
	}
	if len(results) != 1 || results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("repost results = %+v; want created=false cursor=1", results)
	}

	listed, next, _ := svc.ListChanges("doc", 0, 100)
	if len(listed) != 1 || next != 1 {
		t.Fatalf("idempotent repost changed the log: %d rows next=%d", len(listed), next)
	}
}

func TestDocumentsAreIndependent(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.PostChanges("doc-a", []Change{ch("c1", "dev", `1`)}); err != nil {
		t.Fatalf("post a: %v", err)
	}
	results, err := svc.PostChanges("doc-b", []Change{ch("c1", "dev", `1`), ch("c2", "dev", `2`)})
	if err != nil {
		t.Fatalf("post b: %v", err)
	}
	if results[0].Cursor != 1 || results[1].Cursor != 2 {
		t.Fatalf("doc-b cursors = %+v; want 1,2 (independent cursor space)", results)
	}
	listed, _, _ := svc.ListChanges("doc-a", 0, 100)
	if len(listed) != 1 {
		t.Fatalf("doc-a rows = %d; want 1", len(listed))
	}
}

// --- boundary inputs -------------------------------------------------------

func TestListUnknownDocumentAndPagination(t *testing.T) {
	svc := newService(t, nil)

	// Unknown document: empty list, nextCursor 0.
	listed, next, err := svc.ListChanges("ghost", 0, 100)
	if err != nil || len(listed) != 0 || next != 0 {
		t.Fatalf("unknown doc = %v rows, next %d, err %v; want 0 rows, 0, nil", len(listed), next, err)
	}

	for i := 1; i <= 3; i++ {
		if _, err := svc.PostChanges("doc", []Change{ch(fmt.Sprintf("c%d", i), "dev", `1`)}); err != nil {
			t.Fatalf("post: %v", err)
		}
	}
	// Pagination respects after and limit and reports the high-water mark.
	page, next, err := svc.ListChanges("doc", 1, 1)
	if err != nil || len(page) != 1 || page[0].ID != "c2" || next != 2 {
		t.Fatalf("page after=1 limit=1 = %+v next %d err %v", page, next, err)
	}
	// A known document caught up: empty page echoes after.
	page, next, err = svc.ListChanges("doc", 3, 10)
	if err != nil || len(page) != 0 || next != 3 {
		t.Fatalf("caught-up page = %+v next %d err %v; want empty, 3, nil", page, next, err)
	}
	exists, _ := svc.DocumentExists("doc")
	if !exists {
		t.Fatalf("DocumentExists(doc) = false; want true")
	}
}

func TestPostResultOrderFollowsRequest(t *testing.T) {
	svc := newService(t, nil)

	first, err := svc.PostChanges("doc", []Change{ch("a", "dev", `1`)})
	if err != nil {
		t.Fatalf("post a: %v", err)
	}
	_ = first
	// Mixed new and idempotent ids: results come back in request order.
	results, err := svc.PostChanges("doc", []Change{
		ch("b", "dev", `2`),
		ch("a", "dev", `1`),
		ch("c", "dev", `3`),
	})
	if err != nil {
		t.Fatalf("mixed batch: %v", err)
	}
	want := []struct {
		id      string
		created bool
		cursor  int64
	}{{"b", true, 2}, {"a", false, 1}, {"c", true, 3}}
	for i, w := range want {
		if results[i].ID != w.id || results[i].Created != w.created || results[i].Cursor != w.cursor {
			t.Fatalf("result %d = %+v; want %+v", i, results[i], w)
		}
	}
}

// --- failure branches: conflicts and zero write -----------------------------

func TestConflictZeroWrite(t *testing.T) {
	svc := newService(t, nil)

	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `{"v":1}`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	// Different device conflicts.
	_, err := svc.PostChanges("doc", []Change{ch("c1", "other", `{"v":1}`)})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "c1" {
		t.Fatalf("err = %v; want *ErrConflict{ID:c1}", err)
	}
	// Different payload conflicts.
	_, err = svc.PostChanges("doc", []Change{ch("c1", "dev", `{"v":2}`)})
	if !errors.As(err, &conflict) {
		t.Fatalf("err = %v; want *ErrConflict", err)
	}
	// A multi-change batch with a later conflict writes nothing, even for its
	// earlier new ids.
	_, err = svc.PostChanges("doc", []Change{ch("c2", "dev", `1`), ch("c1", "dev", `2`)})
	if !errors.As(err, &conflict) {
		t.Fatalf("batch err = %v; want *ErrConflict", err)
	}
	listed, next, _ := svc.ListChanges("doc", 0, 100)
	if len(listed) != 1 || next != 1 {
		t.Fatalf("conflict wrote rows: %d rows next %d", len(listed), next)
	}
}

func TestReplayGateFailuresAndZeroWrite(t *testing.T) {
	gate := newFakeGate("dev")
	svc := newService(t, gate)

	// Unknown device: the gate error is propagated verbatim.
	_, err := svc.ReplayChanges("doc", []Change{ch("c1", "ghost", `1`)})
	if !errors.Is(err, errNotRegistered) {
		t.Fatalf("unknown device err = %v; want %v", err, errNotRegistered)
	}

	// Revoked device: same, with the permission sentinel.
	gate.deny("doc", "dev")
	_, err = svc.ReplayChanges("doc", []Change{ch("c1", "dev", `1`)})
	if !errors.Is(err, errAccessDenied) {
		t.Fatalf("revoked err = %v; want %v", err, errAccessDenied)
	}

	// Nothing leaked: the document is still unknown to the log.
	if exists, _ := svc.DocumentExists("doc"); exists {
		t.Fatalf("rejected replay created document rows")
	}

	// Allowed replay shares the contiguous cursor space.
	gate.denied["doc"]["dev"] = false
	results, err := svc.ReplayChanges("doc", []Change{ch("c1", "dev", `1`)})
	if err != nil {
		t.Fatalf("allowed replay: %v", err)
	}
	if len(results) != 1 || !results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("replay results = %+v", results)
	}

	// Replay conflict keeps the batch semantics: mismatched id, whole batch.
	_, err = svc.ReplayChanges("doc", []Change{
		ch("c2", "dev", `2`),
		ch("c1", "other", `1`),
	})
	var conflict *ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "c1" {
		t.Fatalf("replay conflict err = %v; want conflict on c1", err)
	}
	listed, next, _ := svc.ListChanges("doc", 0, 100)
	if len(listed) != 1 || next != 1 {
		t.Fatalf("conflicting replay wrote rows: %d rows next %d", len(listed), next)
	}
}

// --- merge ------------------------------------------------------------------

func TestMergeOutcomes(t *testing.T) {
	svc := newService(t, nil)

	// Applied at the current cursor of an unknown document.
	r, err := svc.MergeChange("doc", 0, ch("m1", "dev", `{"a":1}`))
	if err != nil || r.Outcome != "applied" || r.Cursor != 1 {
		t.Fatalf("first merge = %+v, %v", r, err)
	}
	// A non-zero base for an (other) unknown document is a stale cursor.
	_, err = svc.MergeChange("ghost", 1, ch("m9", "dev", `{"a":1}`))
	if !errors.Is(err, ErrStaleCursor) {
		t.Fatalf("unknown-doc base=1 err = %v; want ErrStaleCursor", err)
	}
	// A base ahead of the current cursor is stale.
	_, err = svc.MergeChange("doc", 9, ch("m9", "dev", `{"a":1}`))
	if !errors.Is(err, ErrStaleCursor) {
		t.Fatalf("ahead base err = %v; want ErrStaleCursor", err)
	}

	// Idempotent existing id.
	r, err = svc.MergeChange("doc", 1, ch("m1", "dev", `{"a":1}`))
	if err != nil || r.Outcome != "idempotent" || r.Cursor != 1 ||
		r.Result == nil || r.Result.Created || r.Result.Cursor != 1 {
		t.Fatalf("idempotent merge = %+v, %v", r, err)
	}
	// A different payload on the same id conflicts.
	_, err = svc.MergeChange("doc", 1, ch("m1", "dev", `{"a":2}`))
	var conflict *ErrConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("merge conflict err = %v; want *ErrConflict", err)
	}

	// Behind base: disjoint top-level keys merge; a non-object later payload
	// or a colliding key conflicts.
	if _, err := svc.MergeChange("doc", 1, ch("m2", "dev", `{"b":2}`)); err != nil {
		t.Fatalf("applied m2: %v", err)
	}
	r, err = svc.MergeChange("doc", 1, ch("m3", "dev", `{"c":3}`))
	if err != nil || r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("merged m3 = %+v, %v", r, err)
	}
	_, err = svc.MergeChange("doc", 1, ch("m4", "dev", `{"b":4}`))
	if !errors.As(err, &conflict) {
		t.Fatalf("colliding-key merge err = %v; want *ErrConflict", err)
	}
	// A scalar new payload is itself a conflict in the behind case.
	_, err = svc.MergeChange("doc", 1, ch("m5", "dev", `5`))
	if !errors.As(err, &conflict) {
		t.Fatalf("scalar merge err = %v; want *ErrConflict", err)
	}
}

// --- snapshots and restore --------------------------------------------------

func TestSnapshotsAndRestore(t *testing.T) {
	svc := newService(t, nil)

	// Snapshot on an unknown document / cursor 0 / ahead-of-current are all
	// ErrSnapshotBase.
	for _, cursor := range []int64{0, 1} {
		if _, err := svc.PutSnapshot("doc", cursor, json.RawMessage(`{}`)); !errors.Is(err, ErrSnapshotBase) {
			t.Fatalf("PutSnapshot cursor=%d on empty doc err = %v; want ErrSnapshotBase", cursor, err)
		}
	}

	if _, err := svc.PostChanges("doc", []Change{
		ch("c1", "dev", `{"v":1}`),
		ch("c2", "dev", `{"v":2}`),
	}); err != nil {
		t.Fatalf("post: %v", err)
	}

	created, err := svc.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`))
	if err != nil || !created {
		t.Fatalf("first snapshot = %v, %v", created, err)
	}
	created, err = svc.PutSnapshot("doc", 1, json.RawMessage(`{"v":1.0}`))
	if err != nil || created {
		t.Fatalf("idempotent snapshot = %v, %v", created, err)
	}
	_, err = svc.PutSnapshot("doc", 1, json.RawMessage(`{"v":9}`))
	var snapshotConflict *ErrSnapshotConflict
	if !errors.As(err, &snapshotConflict) || snapshotConflict.Cursor != 1 {
		t.Fatalf("snapshot conflict err = %v; want *ErrSnapshotConflict{1}", err)
	}

	state, err := svc.GetSnapshot("doc", 1)
	if err != nil || string(state) != `{"v":1}` {
		t.Fatalf("GetSnapshot = %s, %v", state, err)
	}
	if _, err := svc.GetSnapshot("doc", 2); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("missing snapshot err = %v; want ErrSnapshotNotFound", err)
	}

	// Restore appends the snapshot state as an ordinary change.
	restored, err := svc.RestoreSnapshot("doc", "dev", "r1", 1)
	if err != nil || !restored.Created || restored.Cursor != 3 || restored.RestoredFrom != 1 {
		t.Fatalf("restore = %+v, %v", restored, err)
	}
	listed, next, _ := svc.ListChanges("doc", 2, 10)
	if next != 3 || len(listed) != 1 || listed[0].ID != "r1" ||
		!jsonEqual(listed[0].Payload, json.RawMessage(`{"v":1}`)) {
		t.Fatalf("restored row = %+v", listed)
	}
	// Idempotent repeat restore.
	again, err := svc.RestoreSnapshot("doc", "dev", "r1", 1)
	if err != nil || again.Created || again.Cursor != 3 || again.RestoredFrom != 1 {
		t.Fatalf("repeat restore = %+v, %v", again, err)
	}
	// A snapshot miss is a not-found.
	_, err = svc.RestoreSnapshot("doc", "dev", "r2", 99)
	if !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("restore missing snapshot err = %v; want ErrSnapshotNotFound", err)
	}
	// A restore id taken by an ordinary change conflicts.
	_, err = svc.RestoreSnapshot("doc", "dev", "c1", 1)
	var restoreConflict *ErrRestoreConflict
	if !errors.As(err, &restoreConflict) || restoreConflict.ID != "c1" {
		t.Fatalf("restore over ordinary change err = %v; want *ErrRestoreConflict{c1}", err)
	}
	// A repeated restore with a different source conflicts. (A snapshot at
	// cursor 2 must exist first: the snapshot lookup precedes the provenance
	// check, so a missing snapshot is a not-found.)
	if _, err := svc.PutSnapshot("doc", 2, json.RawMessage(`{"v":2}`)); err != nil {
		t.Fatalf("second snapshot: %v", err)
	}
	_, err = svc.RestoreSnapshot("doc", "dev", "r1", 2)
	if !errors.As(err, &restoreConflict) {
		t.Fatalf("restore provenance mismatch err = %v; want *ErrRestoreConflict", err)
	}
}

// --- long polling -----------------------------------------------------------

func TestPollImmediateAndTimeout(t *testing.T) {
	svc := newService(t, nil)

	// Unknown document returns at once: empty page, cursor 0, not timed out.
	listed, next, timedOut, err := svc.WaitForChanges(context.Background(), "ghost", 0, 100, time.Second)
	if err != nil || len(listed) != 0 || next != 0 || timedOut {
		t.Fatalf("unknown-doc wait = %v rows next %d timedOut %v err %v", len(listed), next, timedOut, err)
	}

	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `1`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	// Rows already past the cursor come back immediately even with a wait.
	listed, next, timedOut, err = svc.WaitForChanges(context.Background(), "doc", 0, 100, time.Second)
	if err != nil || len(listed) != 1 || next != 1 || timedOut {
		t.Fatalf("immediate wait = %+v next %d timedOut %v err %v", listed, next, timedOut, err)
	}
	// Caught-up known document with a zero wait expires immediately, echoing
	// the cursor without advancing it.
	listed, next, timedOut, err = svc.WaitForChanges(context.Background(), "doc", 1, 100, 0)
	if err != nil || len(listed) != 0 || next != 1 || !timedOut {
		t.Fatalf("zero wait = %+v next %d timedOut %v err %v", listed, next, timedOut, err)
	}
}

func TestPollWakesOnCommitAndInterrupt(t *testing.T) {
	svc := newService(t, nil)
	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `1`)}); err != nil {
		t.Fatalf("post: %v", err)
	}

	type outcome struct {
		rows     int
		next     int64
		timedOut bool
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		listed, next, timedOut, err := svc.WaitForChanges(context.Background(), "doc", 1, 100, 5*time.Second)
		done <- outcome{len(listed), next, timedOut, err}
	}()

	// Let the goroutine park, then commit.
	time.Sleep(50 * time.Millisecond)
	if _, err := svc.PostChanges("doc", []Change{ch("c2", "dev", `2`), ch("c3", "dev", `3`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.rows != 2 || got.next != 3 || got.timedOut {
			t.Fatalf("woken wait = %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("wait was not woken by the commit")
	}

	// A parked wait interrupted at shutdown answers ErrStoreClosing.
	stopped := make(chan error, 1)
	go func() {
		_, _, _, err := svc.WaitForChanges(context.Background(), "doc", 3, 100, time.Minute)
		stopped <- err
	}()
	time.Sleep(50 * time.Millisecond)
	svc.InterruptWaits()
	select {
	case err := <-stopped:
		if !errors.Is(err, ErrStoreClosing) {
			t.Fatalf("interrupted wait err = %v; want ErrStoreClosing", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("interrupted wait did not return")
	}
}

func TestPollCanceledContext(t *testing.T) {
	svc := newService(t, nil)
	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `1`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan error, 1)
	go func() {
		_, _, _, err := svc.WaitForChanges(ctx, "doc", 1, 100, time.Minute)
		stopped <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-stopped:
		if err == nil {
			t.Fatalf("canceled wait err = nil; want context.Canceled")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("canceled wait did not return")
	}
}

// --- subscriptions ----------------------------------------------------------

func TestSubscriptionCommitAndRevoke(t *testing.T) {
	svc := newService(t, nil)

	wakeA, revokedA, unregA := svc.AddSubscription("doc", "dev-a")
	defer unregA()
	_, revokedB, unregB := svc.AddSubscription("doc", "dev-b")
	defer unregB()

	// A commit wakes every subscription under the document.
	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev-a", `1`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	select {
	case <-wakeA:
	case <-time.After(time.Second):
		t.Fatalf("subscription not woken by commit")
	}
	select {
	case <-revokedB:
		t.Fatalf("dev-b revoked channel fired on a plain commit")
	default:
	}

	// Revoke is scoped to the (document, device) pair and closes its channel
	// once (stickily).
	svc.OnRevoke("doc", "dev-b")
	select {
	case <-revokedB:
	case <-time.After(time.Second):
		t.Fatalf("dev-b subscription not revoked")
	}
	select {
	case <-revokedA:
		t.Fatalf("dev-a subscription revoked by another device's revoke")
	default:
	}
	// Calling OnRevoke again is harmless: the channel is already closed.
	svc.OnRevoke("doc", "dev-b")
}

func TestSubscriptionUnregisterAndClosing(t *testing.T) {
	svc := newService(t, nil)

	_, _, unregister := svc.AddSubscription("doc", "dev")
	unregister()
	unregister() // idempotent

	// A subscription added while interrupting is signaled immediately.
	svc.InterruptWaits()
	if !svc.Closing() {
		t.Fatalf("Closing() = false after InterruptWaits")
	}
	wake, _, lateUnreg := svc.AddSubscription("doc", "dev")
	defer lateUnreg()
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatalf("late subscription not signaled at shutdown")
	}
}

// --- concurrency and durability ---------------------------------------------

func TestConcurrentBatchesNoDuplicateCursors(t *testing.T) {
	svc := newService(t, nil)

	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			changes := make([]Change, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				changes = append(changes, ch(fmt.Sprintf("w%d-c%d", w, i), "dev", `1`))
			}
			if _, err := svc.PostChanges("doc", changes); err != nil {
				errs <- err
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent post: %v", err)
	}

	listed, next, err := svc.ListChanges("doc", 0, workers*perWorker+1)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != workers*perWorker || next != int64(workers*perWorker) {
		t.Fatalf("rows = %d, next = %d; want %d", len(listed), next, workers*perWorker)
	}
	seen := make(map[int64]bool, len(listed))
	for _, c := range listed {
		if seen[c.Cursor] {
			t.Fatalf("duplicate cursor %d", c.Cursor)
		}
		if c.Cursor < 1 || c.Cursor > next {
			t.Fatalf("cursor %d outside 1..%d", c.Cursor, next)
		}
		seen[c.Cursor] = true
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "changelog.db")
	db := openFileDB(t, path)
	svc, err := New(db, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := svc.PostChanges("doc", []Change{ch("c1", "dev", `{"v":1}`)}); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err := svc.MergeChange("doc", 1, ch("m1", "dev", `{"a":1}`)); err != nil {
		t.Fatalf("merge: %v", err)
	}
	if _, err := svc.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, err := svc.RestoreSnapshot("doc", "dev", "r1", 1); err != nil {
		t.Fatalf("restore: %v", err)
	}
	svc.Close()
	_ = db.Close()

	db2 := openFileDB(t, path)
	reopened, err := New(db2, newFakeGate("dev"))
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	t.Cleanup(reopened.Close)

	listed, next, err := reopened.ListChanges("doc", 0, 100)
	if err != nil || len(listed) != 3 || next != 3 {
		t.Fatalf("reopened rows = %d next %d err %v; want 3 rows, 3", len(listed), next, err)
	}
	// Idempotency decisions survive.
	again, err := reopened.PostChanges("doc", []Change{ch("c1", "dev", `{"v":1}`)})
	if err != nil || again[0].Created || again[0].Cursor != 1 {
		t.Fatalf("reopened idempotency = %+v, %v", again, err)
	}
	// The snapshot and its idempotency survive.
	created, err := reopened.PutSnapshot("doc", 1, json.RawMessage(`{"v":1}`))
	if err != nil || created {
		t.Fatalf("reopened snapshot idempotency = %v, %v", created, err)
	}
	state, err := reopened.GetSnapshot("doc", 1)
	if err != nil || string(state) != `{"v":1}` {
		t.Fatalf("reopened snapshot = %s, %v", state, err)
	}
	// Replay gating still works against the reopened log.
	results, err := reopened.ReplayChanges("doc", []Change{ch("c1", "dev", `{"v":1}`)})
	if err != nil || results[0].Created || results[0].Cursor != 1 {
		t.Fatalf("reopened replay idempotency = %+v, %v", results, err)
	}
}
