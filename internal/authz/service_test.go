package authz_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// openKernelPerms constructs the kernel and the permission service directly,
// without the composition root, proving the service is reusable on its own.
func openKernelPerms(t *testing.T) (*store.Store, *authz.Service) {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	perms, err := authz.New(kernel)
	if err != nil {
		t.Fatal(err)
	}
	return kernel, perms
}

// Normal path: the default is authorized, the first revoke flips it, a repeat
// is idempotent and a grant restores it.
func TestServiceStandaloneLifecycle(t *testing.T) {
	kernel, perms := openKernelPerms(t)
	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	ok, err := perms.DocumentAuthorized("doc", "dev")
	if err != nil || !ok {
		t.Fatalf("default authorized=%v err=%v", ok, err)
	}
	changed, err := perms.SetDocumentPermission("doc", "dev", true)
	if err != nil || changed {
		t.Fatalf("grant on default changed=%v err=%v", changed, err)
	}
	if changed, err := perms.SetDocumentPermission("doc", "dev", false); err != nil || !changed {
		t.Fatalf("first revoke changed=%v err=%v", changed, err)
	}
	if changed, err := perms.SetDocumentPermission("doc", "dev", false); err != nil || changed {
		t.Fatalf("repeat revoke changed=%v err=%v", changed, err)
	}
	if changed, err := perms.SetDocumentPermission("doc", "dev", true); err != nil || !changed {
		t.Fatalf("re-grant changed=%v err=%v", changed, err)
	}
}

// Failure branch: an unregistered device yields ErrDeviceNotFound and writes
// nothing.
func TestServiceStandaloneUnknownDevice(t *testing.T) {
	_, perms := openKernelPerms(t)
	if _, err := perms.SetDocumentPermission("doc", "ghost", false); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("revoke unknown device err = %v, want ErrDeviceNotFound", err)
	}
}

// AuthorizedTx is the in-transaction verdict shared by the other services:
// unregistered -> 404, revoked -> 403, otherwise nil.
func TestServiceStandaloneAuthorizedTx(t *testing.T) {
	kernel, perms := openKernelPerms(t)
	db := kernel.DB()

	// Unregistered device.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := perms.AuthorizedTx(tx, "doc", "ghost"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("ghost verdict = %v, want ErrDeviceNotFound", err)
	}
	_ = tx.Rollback()

	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	// Registered, default authorized.
	tx, _ = db.Begin()
	if err := perms.AuthorizedTx(tx, "doc", "dev"); err != nil {
		t.Fatalf("default verdict = %v, want nil", err)
	}
	_ = tx.Commit()

	if _, err := perms.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	tx, _ = db.Begin()
	if err := perms.AuthorizedTx(tx, "doc", "dev"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked verdict = %v, want ErrPermissionDenied", err)
	}
	_ = tx.Rollback()
}

// A committed revoke fans out exactly once to each registered sink; a grant
// notifies nothing.
func TestServiceStandaloneRevokeSinks(t *testing.T) {
	kernel, perms := openKernelPerms(t)
	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	type call struct{ doc, device string }
	got := make(chan call, 4)
	perms.AddRevokeSink(revokeSinkFunc(func(doc, device string) { got <- call{doc, device} }))

	if _, err := perms.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-got:
		if c.doc != "doc" || c.device != "dev" {
			t.Fatalf("sink call = %+v", c)
		}
	default:
		t.Fatal("sink not notified after revoke")
	}

	// A repeat revoke and a grant do not notify.
	if _, err := perms.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	if _, err := perms.SetDocumentPermission("doc", "dev", true); err != nil {
		t.Fatal(err)
	}
	select {
	case c := <-got:
		t.Fatalf("unexpected sink call %+v after repeat revoke/grant", c)
	default:
	}
}

type revokeSinkFunc func(documentID, deviceID string)

func (f revokeSinkFunc) PermissionRevoked(documentID, deviceID string) { f(documentID, deviceID) }
