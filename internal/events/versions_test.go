package events_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// newVersionService opens a standalone service with no gate and seeds one
// document ("doc") with two changes and snapshots at both cursors.
func newVersionService(t *testing.T, gate events.Gate) (*events.Service, func()) {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	svc, err := events.New(kernel, gate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PostChanges("doc", []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{"n":1}`)},
		{ID: "c2", DeviceID: "dev", Payload: []byte(`{"n":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PutSnapshot("doc", 1, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PutSnapshot("doc", 2, []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	return svc, func() { _ = kernel.Close() }
}

func TestSnapshotVersionRegisterListGet(t *testing.T) {
	svc, closeFn := newVersionService(t, nil)
	defer closeFn()

	v, created, err := svc.RegisterSnapshotVersion("doc", "v1", 1)
	if err != nil || !created || v.Name != "v1" || v.Cursor != 1 {
		t.Fatalf("register = %+v created=%v err=%v", v, created, err)
	}
	// Same binding: idempotent, created false, identical result.
	v2, created, err := svc.RegisterSnapshotVersion("doc", "v1", 1)
	if err != nil || created || v2 != v {
		t.Fatalf("idempotent register = %+v created=%v err=%v", v2, created, err)
	}
	// Other cursor: conflict, zero writes.
	_, _, err = svc.RegisterSnapshotVersion("doc", "v1", 2)
	var conflict *events.ErrVersionConflict
	if !errors.As(err, &conflict) || conflict.Name != "v1" {
		t.Fatalf("rebind err = %v", err)
	}
	// Target snapshot missing / unknown document: snapshot not found.
	if _, _, err := svc.RegisterSnapshotVersion("doc", "v9", 9); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("missing snapshot err = %v", err)
	}
	if _, _, err := svc.RegisterSnapshotVersion("ghost", "v1", 1); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("unknown doc err = %v", err)
	}

	// List comes back ascending with only name and cursor.
	if _, _, err := svc.RegisterSnapshotVersion("doc", "aaa", 2); err != nil {
		t.Fatal(err)
	}
	list, err := svc.ListSnapshotVersions("doc")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].Name != "aaa" || list[0].Cursor != 2 ||
		list[1].Name != "v1" || list[1].Cursor != 1 {
		t.Fatalf("list = %+v", list)
	}
	// Empty for an unknown document.
	if empty, err := svc.ListSnapshotVersions("ghost"); err != nil || len(empty) != 0 {
		t.Fatalf("empty list = %v err=%v", empty, err)
	}
	// Get by name then read state: the state is the cursor snapshot's.
	got, err := svc.GetSnapshotVersion("doc", "v1")
	if err != nil || got.Cursor != 1 {
		t.Fatalf("get = %+v err=%v", got, err)
	}
	state, err := svc.GetSnapshot("doc", got.Cursor)
	if err != nil || string(state) != `{"n":1}` {
		t.Fatalf("state = %s err=%v", state, err)
	}
	if _, err := svc.GetSnapshotVersion("doc", "missing"); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("missing name err = %v", err)
	}
}

func TestSnapshotVersionRenameDelete(t *testing.T) {
	svc, closeFn := newVersionService(t, nil)
	defer closeFn()
	if _, _, err := svc.RegisterSnapshotVersion("doc", "v1", 1); err != nil {
		t.Fatal(err)
	}

	// Move to cursor 2; then same-cursor rename is idempotent.
	if v, moved, err := svc.RenameSnapshotVersion("doc", "v1", 2); err != nil || !moved || v.Cursor != 2 {
		t.Fatalf("rename = %+v moved=%v err=%v", v, moved, err)
	}
	if _, moved, err := svc.RenameSnapshotVersion("doc", "v1", 2); err != nil || moved {
		t.Fatalf("same-cursor rename moved=%v err=%v", moved, err)
	}
	// Unknown name and missing snapshot are 404s.
	if _, _, err := svc.RenameSnapshotVersion("doc", "ghost", 1); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("rename unknown = %v", err)
	}
	if _, _, err := svc.RenameSnapshotVersion("doc", "v1", 9); !errors.Is(err, events.ErrSnapshotNotFound) {
		t.Fatalf("rename missing snapshot = %v", err)
	}
	// A failed rename left the marker at 2.
	if got, _ := svc.GetSnapshotVersion("doc", "v1"); got.Cursor != 2 {
		t.Fatalf("marker = %d", got.Cursor)
	}

	// Delete frees the name; repeat delete misses; re-register creates anew.
	if err := svc.DeleteSnapshotVersion("doc", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := svc.DeleteSnapshotVersion("doc", "v1"); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("repeat delete = %v", err)
	}
	if _, created, err := svc.RegisterSnapshotVersion("doc", "v1", 1); err != nil || !created {
		t.Fatalf("re-register created=%v err=%v", created, err)
	}
	// The snapshots themselves are untouched.
	if _, err := svc.GetSnapshot("doc", 2); err != nil {
		t.Fatalf("snapshot 2 lost: %v", err)
	}
}

func TestSnapshotVersionRestoreByName(t *testing.T) {
	svc, closeFn := newVersionService(t, nil)
	defer closeFn()
	if _, _, err := svc.RegisterSnapshotVersion("doc", "v1", 2); err != nil {
		t.Fatal(err)
	}

	// Restore by name: one ordinary change at the next cursor, provenance
	// recorded against cursor 2.
	res, err := svc.RestoreSnapshotVersion("doc", "dev", "r1", "v1")
	if err != nil || !res.Created || res.Cursor != 3 || res.RestoredFrom != 2 {
		t.Fatalf("restore = %+v err=%v", res, err)
	}
	// Idempotent repeat.
	again, err := svc.RestoreSnapshotVersion("doc", "dev", "r1", "v1")
	if err != nil || again.Created || again.Cursor != 3 {
		t.Fatalf("repeat = %+v err=%v", again, err)
	}
	// Moving the marker to another cursor changes what the name resolves to;
	// the restore provenance is pinned to cursor 2, so the same id via the
	// renamed (cursor 1) name is now a conflict, not a silent idempotent hit.
	if _, _, err := svc.RenameSnapshotVersion("doc", "v1", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RestoreSnapshotVersion("doc", "dev", "r1", "v1"); err == nil {
		t.Fatal("restore after rename must conflict on provenance")
	} else {
		var rc *events.ErrRestoreConflict
		if !errors.As(err, &rc) {
			t.Fatalf("want restore conflict, got %v", err)
		}
	}
	// Unknown name is a version miss.
	if _, err := svc.RestoreSnapshotVersion("doc", "dev", "r2", "ghost"); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("unknown name restore = %v", err)
	}
	// An id occupied by an ordinary change is a restore conflict.
	if _, err := svc.RestoreSnapshotVersion("doc", "dev", "c1", "v1"); err == nil {
		t.Fatal("ordinary change id must conflict")
	} else {
		var rc *events.ErrRestoreConflict
		if !errors.As(err, &rc) || rc.ID != "c1" {
			t.Fatalf("conflict = %v", err)
		}
	}
}

// The gate is enforced before the snapshot/version lookup for every gated
// entry, and a denial writes nothing.
func TestSnapshotVersionAuthorizedGateOrder(t *testing.T) {
	for _, gateErr := range []error{store.ErrDeviceNotFound, store.ErrPermissionDenied} {
		svc, closeFn := newVersionService(t, fakeGate{err: gateErr})
		if _, _, err := svc.RegisterSnapshotVersionAuthorized("doc", "dev", "v1", 1); !errors.Is(err, gateErr) {
			t.Fatalf("gated register = %v", err)
		}
		if _, _, err := svc.RenameSnapshotVersionAuthorized("doc", "dev", "v1", 2); !errors.Is(err, gateErr) {
			t.Fatalf("gated rename = %v", err)
		}
		if err := svc.DeleteSnapshotVersionAuthorized("doc", "dev", "v1"); !errors.Is(err, gateErr) {
			t.Fatalf("gated delete = %v", err)
		}
		if _, err := svc.RestoreSnapshotVersionAuthorized("doc", "dev", "r", "v1"); !errors.Is(err, gateErr) {
			t.Fatalf("gated restore = %v", err)
		}
		closeFn()
	}

	// A denied registration wrote no marker (verifiable with an open service).
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	deny, _ := events.New(kernel, fakeGate{err: store.ErrPermissionDenied})
	if _, err := deny.PostChanges("d", []events.Change{{ID: "x", DeviceID: "p", Payload: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := deny.PutSnapshot("d", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := deny.RegisterSnapshotVersionAuthorized("d", "p", "v", 1); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("deny = %v", err)
	}
	check, _ := events.New(kernel, nil)
	if _, err := check.GetSnapshotVersion("d", "v"); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("denied registration wrote a marker: %v", err)
	}
}

// The version markers of a document are counted as document data and are
// removed with it; the rebuilt id inherits none.
func TestSnapshotVersionDocumentCascade(t *testing.T) {
	kernel, _ := store.Open("")
	defer func() { _ = kernel.Close() }()
	svc, _ := events.New(kernel, nil)
	if _, err := svc.PostChanges("doc", []events.Change{{ID: "c1", DeviceID: "dev", Payload: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PutSnapshot("doc", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.RegisterSnapshotVersion("doc", "v1", 1); err != nil {
		t.Fatal(err)
	}

	tx, err := kernel.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	exists, err := svc.DocumentDataExistsTx(tx, "doc")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("document with only a version marker must count as existing")
	}
	if err := svc.DeleteDocumentDataTx(tx, "doc"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.GetSnapshotVersion("doc", "v1"); !errors.Is(err, events.ErrVersionNotFound) {
		t.Fatalf("version survived delete: %v", err)
	}
	if rows, _ := svc.ListSnapshotVersions("doc"); len(rows) != 0 {
		t.Fatalf("versions after delete = %v", rows)
	}
}
