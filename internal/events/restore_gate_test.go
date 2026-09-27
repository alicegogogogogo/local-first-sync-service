package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// seedSnapshotWithGate opens a service over gate, posts one change and stores a
// snapshot at cursor 1, returning the service and a cleanup.
func seedSnapshotWithGate(t *testing.T, gate events.Gate) *events.Service {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	svc, err := events.New(kernel, gate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"n":1}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	return svc
}

// A gate denial (unregistered device) rejects the authorized restore even
// though the snapshot exists, and writes nothing.
func TestServiceStandaloneRestoreAuthorizedGateNotFound(t *testing.T) {
	svc := seedSnapshotWithGate(t, fakeGate{err: store.ErrDeviceNotFound})

	_, err := svc.RestoreSnapshotAuthorized("doc", "ghost", "r1", 1)
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("restore err = %v, want ErrDeviceNotFound", err)
	}
	rows, next, err := svc.ListChanges("doc", 0, 100)
	if err != nil || len(rows) != 1 || next != 1 {
		t.Fatalf("denied restore wrote content: rows=%v next=%d err=%v", rows, next, err)
	}
}

// A revoked gate rejects the authorized restore with 403 semantics and writes
// nothing.
func TestServiceStandaloneRestoreAuthorizedGateRevoked(t *testing.T) {
	svc := seedSnapshotWithGate(t, fakeGate{err: store.ErrPermissionDenied})

	_, err := svc.RestoreSnapshotAuthorized("doc", "dev", "r1", 1)
	if !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("restore err = %v, want ErrPermissionDenied", err)
	}
	rows, next, _ := svc.ListChanges("doc", 0, 100)
	if len(rows) != 1 || next != 1 {
		t.Fatalf("denied restore wrote content: rows=%v next=%d", rows, next)
	}
}

// The gate is consulted before the snapshot is looked up: a denying gate wins
// over a missing snapshot rather than returning ErrSnapshotNotFound.
func TestServiceStandaloneRestoreAuthorizedGateBeforeSnapshotMiss(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, fakeGate{err: store.ErrPermissionDenied})

	_, err := svc.RestoreSnapshotAuthorized("ghost", "dev", "r1", 42)
	if !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("restore err = %v, want ErrPermissionDenied to win over the snapshot miss", err)
	}
}

// Through an allowing gate the restore appends with the next cursor, and an
// identical repeat is idempotent and allocates nothing further.
func TestServiceStandaloneRestoreAuthorizedSuccessAndIdempotent(t *testing.T) {
	svc := seedSnapshotWithGate(t, fakeGate{})

	result, err := svc.RestoreSnapshotAuthorized("doc", "dev", "r1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || result.Cursor != 2 || result.RestoredFrom != 1 || result.ID != "r1" {
		t.Fatalf("restore result = %+v", result)
	}

	again, err := svc.RestoreSnapshotAuthorized("doc", "dev", "r1", 1)
	if err != nil {
		t.Fatal(err)
	}
	if again.Created || again.Cursor != 2 || again.RestoredFrom != 1 {
		t.Fatalf("idempotent restore = %+v", again)
	}

	rows, next, _ := svc.ListChanges("doc", 0, 100)
	if len(rows) != 2 || next != 2 {
		t.Fatalf("rows = %d next = %d, want exactly 2", len(rows), next)
	}
}
