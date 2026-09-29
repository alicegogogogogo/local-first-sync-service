package events_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// statusService opens an event service over an in-memory kernel with an
// allowing gate, the shape the gated status read runs against.
func statusService(t *testing.T) (*store.Store, *events.Service) {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	svc, err := events.New(kernel, fakeGate{})
	if err != nil {
		t.Fatal(err)
	}
	return kernel, svc
}

// An unknown document answers four zeros: the empty status is a successful
// read, not an error, and it creates no state.
func TestChangeStatusUnknownDocument(t *testing.T) {
	_, svc := statusService(t)

	status, err := svc.GetChangeStatus("never-heard-of-it", "dev")
	if err != nil {
		t.Fatal(err)
	}
	if status != (events.ChangeStatus{}) {
		t.Fatalf("status = %+v, want all zero", status)
	}
	if ok, _ := svc.DocumentExists("never-heard-of-it"); ok {
		t.Fatal("the status read created document state")
	}
}

// The four numbers follow the durable data: commits grow the online count and
// maximum cursor; a snapshot sets the boundary without moving anything; a
// compaction moves the at-or-below-boundary rows into the compacted count.
func TestChangeStatusReflectsCommitsSnapshotsAndCompaction(t *testing.T) {
	_, svc := statusService(t)
	commit := func(ids ...string) {
		t.Helper()
		changes := make([]events.Change, len(ids))
		for i, id := range ids {
			changes[i] = events.Change{ID: id, DeviceID: "dev", Payload: []byte(`{"n":1}`)}
		}
		if _, err := svc.PostChanges("doc", changes); err != nil {
			t.Fatal(err)
		}
	}

	commit("c1", "c2", "c3")
	if got, err := svc.GetChangeStatus("doc", "dev"); err != nil || got != (events.ChangeStatus{OnlineCount: 3, Boundary: 0, MaxCursor: 3, Compacted: 0}) {
		t.Fatalf("after commits = %+v err = %v", got, err)
	}

	// A snapshot pins the boundary to its cursor; the online log is untouched.
	if _, err := svc.PutSnapshot("doc", 2, []byte(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	if got, err := svc.GetChangeStatus("doc", "dev"); err != nil || got != (events.ChangeStatus{OnlineCount: 3, Boundary: 2, MaxCursor: 3, Compacted: 0}) {
		t.Fatalf("after snapshot = %+v err = %v", got, err)
	}

	// Compaction moves the two rows at or below the boundary out: they count as
	// compacted and leave the online count and maximum cursor behind.
	boundary, removed, err := svc.CompactChanges("doc", "dev")
	if err != nil || boundary != 2 || removed != 2 {
		t.Fatalf("compact boundary=%d removed=%d err=%v", boundary, removed, err)
	}
	if got, err := svc.GetChangeStatus("doc", "dev"); err != nil || got != (events.ChangeStatus{OnlineCount: 1, Boundary: 2, MaxCursor: 3, Compacted: 2}) {
		t.Fatalf("after compaction = %+v err = %v", got, err)
	}

	// A later commit grows only the online side; the compacted total persists.
	commit("c4")
	if got, err := svc.GetChangeStatus("doc", "dev"); err != nil || got != (events.ChangeStatus{OnlineCount: 2, Boundary: 2, MaxCursor: 4, Compacted: 2}) {
		t.Fatalf("after later commit = %+v err = %v", got, err)
	}

	// A repeat compaction removes nothing and changes no number.
	if _, removed, err := svc.CompactChanges("doc", "dev"); err != nil || removed != 0 {
		t.Fatalf("repeat compact removed=%d err=%v", removed, err)
	}
	if got, err := svc.GetChangeStatus("doc", "dev"); err != nil || got != (events.ChangeStatus{OnlineCount: 2, Boundary: 2, MaxCursor: 4, Compacted: 2}) {
		t.Fatalf("after repeat compact = %+v err = %v", got, err)
	}
}

// A document compacted all the way to its boundary reports no online rows and
// a zero maximum online cursor, while the boundary and compacted total remain.
func TestChangeStatusFullyCompacted(t *testing.T) {
	_, svc := statusService(t)
	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`1`)},
		{ID: "c2", DeviceID: "dev", Payload: []byte(`2`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PutSnapshot("doc", 2, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.CompactChanges("doc", "dev"); err != nil {
		t.Fatal(err)
	}

	got, err := svc.GetChangeStatus("doc", "dev")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 0, Boundary: 2, MaxCursor: 0, Compacted: 2}) {
		t.Fatalf("fully compacted = %+v", got)
	}
}

// The gate runs before any statistic is observed: an unregistered device is
// ErrDeviceNotFound and a revoked one ErrPermissionDenied, and neither writes
// anything.
func TestChangeStatusGate(t *testing.T) {
	kernel, _ := store.Open("")
	t.Cleanup(func() { _ = kernel.Close() })

	// Ordinary posts never consult the gate, so an allowing-gate service can
	// seed a document the denying-gate services then fail to observe.
	seeder, err := events.New(kernel, fakeGate{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seeder.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`1`)},
	}); err != nil {
		t.Fatal(err)
	}

	notFound, err := events.New(kernel, fakeGate{err: store.ErrDeviceNotFound})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := notFound.GetChangeStatus("doc", "ghost"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered err = %v, want ErrDeviceNotFound", err)
	}

	revoked, err := events.New(kernel, fakeGate{err: store.ErrPermissionDenied})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := revoked.GetChangeStatus("doc", "dev"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked err = %v, want ErrPermissionDenied", err)
	}

	// Neither rejected read changed the seeded document.
	got, err := seeder.GetChangeStatus("doc", "dev")
	if err != nil {
		t.Fatal(err)
	}
	if got.OnlineCount != 1 || got.MaxCursor != 1 {
		t.Fatalf("rejected reads changed state: %+v", got)
	}
}

// Repeated reads are pure: they allocate no cursor, so a commit landing after
// several status reads gets the cursor it would have received anyway.
func TestChangeStatusReadOnly(t *testing.T) {
	_, svc := statusService(t)
	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`1`)},
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := svc.GetChangeStatus("doc", "dev"); err != nil {
			t.Fatal(err)
		}
	}
	results, err := svc.PostChanges("doc", []events.Change{
		{ID: "c2", DeviceID: "dev", Payload: []byte(`2`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Cursor != 2 {
		t.Fatalf("status reads moved the cursor: %+v", results)
	}
}
