package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// seedSnapshots posts n changes and stores one snapshot at every cursor 1..n.
func seedSnapshots(t *testing.T, svc *events.Service, doc, device string, n int) {
	t.Helper()
	changes := make([]events.Change, n)
	for i := range changes {
		changes[i] = events.Change{
			ID:       "c" + string(rune('1'+i)),
			DeviceID: device,
			Payload:  json.RawMessage(`{}`),
		}
	}
	if _, err := svc.PostChanges(doc, changes); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		created, err := svc.PutSnapshot(doc, int64(i), json.RawMessage(`{"s":1}`))
		if err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		if !created {
			t.Fatalf("snapshot %d was not created", i)
		}
	}
}

func TestServicePruneSnapshotsRetention(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{})

	// A snapshot-less document prunes successfully with both numbers zero.
	res, err := svc.PruneSnapshots("ghost", "dev", json.RawMessage(`3`))
	if err != nil || res.MaxCursor != 0 || res.Deleted != 0 {
		t.Fatalf("unknown prune = %+v, %v", res, err)
	}

	seedSnapshots(t, svc, "doc", "dev", 5)
	res, err = svc.PruneSnapshots("doc", "dev", json.RawMessage(`2`))
	if err != nil {
		t.Fatal(err)
	}
	if res.MaxCursor != 5 || res.Deleted != 3 {
		t.Fatalf("prune = %+v, want maxCursor 5 deleted 3", res)
	}
	snaps, err := svc.ExportSnapshots("doc", 0, nil)
	if err != nil || len(snaps) != 2 || snaps[0].Cursor != 4 || snaps[1].Cursor != 5 {
		t.Fatalf("survivors = %+v, err %v", snaps, err)
	}
	// A repeat prune deletes nothing.
	res, err = svc.PruneSnapshots("doc", "dev", json.RawMessage(`2`))
	if err != nil || res.MaxCursor != 5 || res.Deleted != 0 {
		t.Fatalf("repeat prune = %+v, %v", res, err)
	}
}

func TestServicePruneSnapshotsGateBeforeKeep(t *testing.T) {
	// The gate verdict precedes keep validation: an unregistered device with an
	// illegal keep is still a not-found error.
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{err: store.ErrDeviceNotFound})
	if _, err := svc.PruneSnapshots("doc", "ghost", json.RawMessage(`0`)); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}

	// A revoked device with an otherwise fine keep is a permission error.
	kernel2, _ := store.Open("")
	defer func() { _ = kernel2.Close() }()
	svc2, _ := events.New(kernel2, fakeGate{err: store.ErrPermissionDenied})
	if _, err := svc2.PruneSnapshots("doc", "dev", json.RawMessage(`1`)); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
}

func TestServicePruneSnapshotsInvalidKeep(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{})
	seedSnapshots(t, svc, "doc", "dev", 3)

	for _, raw := range []string{
		"",     // missing field
		`0`,    // zero
		`-1`,   // negative
		`1001`, // above the maximum
		`1.0`,  // fraction
		`"1"`,  // string
		`true`, // boolean
		`null`, // null
		`1e2`,  // exponent
	} {
		if _, err := svc.PruneSnapshots("doc", "dev", json.RawMessage(raw)); !errors.Is(err, events.ErrInvalidKeep) {
			t.Fatalf("keep %q err = %v, want ErrInvalidKeep", raw, err)
		}
	}
	// Every rejection wrote nothing: all three snapshots remain.
	snaps, err := svc.ExportSnapshots("doc", 0, nil)
	if err != nil || len(snaps) != 3 {
		t.Fatalf("snapshots after rejections = %d, %v", len(snaps), err)
	}
}

// A recorded restore keeps its idempotency/conflict verdict from stored
// provenance after the snapshot it came from is pruned, while a new restore
// of the pruned cursor misses.
func TestServicePruneSnapshotsRestoreProvenance(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{})
	seedSnapshots(t, svc, "doc", "dev", 3)

	res, err := svc.RestoreSnapshot("doc", "dev", "r1", 2)
	if err != nil || !res.Created || res.Cursor != 4 || res.RestoredFrom != 2 {
		t.Fatalf("restore = %+v, %v", res, err)
	}
	if _, err := svc.PutSnapshot("doc", 4, json.RawMessage(`null`)); err != nil {
		t.Fatal(err)
	}
	// Compact trims the restore change into a retained summary, then prune
	// removes the source snapshot; the boundary stays at 4.
	if boundary, removed, err := svc.CompactChanges("doc", "dev"); err != nil || boundary != 4 || removed != 4 {
		t.Fatalf("compact = %d/%d, %v", boundary, removed, err)
	}
	if res, err := svc.PruneSnapshots("doc", "dev", json.RawMessage(`2`)); err != nil ||
		res.MaxCursor != 4 || res.Deleted != 2 {
		t.Fatalf("prune = %+v, %v", res, err)
	}
	if boundary, removed, err := svc.CompactChanges("doc", "dev"); err != nil || boundary != 4 || removed != 0 {
		t.Fatalf("re-compact = %d/%d, %v", boundary, removed, err)
	}

	// The recorded restore replays idempotently even though snapshot 2 is
	// gone; a mismatch conflicts; a new id at the pruned cursor misses.
	res, err = svc.RestoreSnapshot("doc", "dev", "r1", 2)
	if err != nil || res.Created || res.Cursor != 4 {
		t.Fatalf("idempotent restore = %+v, %v", res, err)
	}
	if _, err := svc.RestoreSnapshot("doc", "other", "r1", 2); err == nil {
		t.Fatal("mismatched restore succeeded, want ErrRestoreConflict")
	} else {
		var conflict *events.ErrRestoreConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("mismatched restore err = %v, want ErrRestoreConflict", err)
		}
	}
	if _, err := svc.RestoreSnapshot("doc", "dev", "r2", 2); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("new restore err = %v, want ErrSnapshotNotFound", err)
	}
}
