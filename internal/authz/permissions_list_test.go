package authz_test

import (
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// idsOf renders a ledger page as deviceId/authorized pairs, in order.
func idsOf(entries []authz.PermissionEntry) []string {
	pairs := make([]string, 0, len(entries))
	for _, e := range entries {
		pairs = append(pairs, e.DeviceID+":"+boolStr(e.Authorized))
	}
	return pairs
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// The service ledger lists every registered device once in ascending device-id
// order; untouched devices are authorized by default and a revoked device is
// not.
func TestListDocumentPermissionsService(t *testing.T) {
	kernel, perms := openKernelPerms(t)
	for _, id := range []string{"dev-3", "dev-1", "dev-2", "dev-10"} {
		if _, err := kernel.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := perms.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	// A grant on the default writes nothing and must not duplicate a row.
	if _, err := perms.SetDocumentPermission("doc", "dev-1", true); err != nil {
		t.Fatal(err)
	}
	// Another document's revoke never leaks in.
	if _, err := perms.SetDocumentPermission("other", "dev-1", false); err != nil {
		t.Fatal(err)
	}

	entries, err := perms.ListDocumentPermissions("doc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(entries); !equal(got, []string{
		"dev-1:true", "dev-10:true", "dev-2:false", "dev-3:true",
	}) {
		t.Fatalf("ledger = %v", got)
	}

	// An unknown document: every device keeps the default.
	entries, err = perms.ListDocumentPermissions("ghost", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Authorized {
			t.Fatalf("unknown document ledger has revoked entry: %+v", e)
		}
	}
	if len(entries) != 4 {
		t.Fatalf("unknown document ledger len = %d, want 4", len(entries))
	}

	// Paging slices the one lexicographic order.
	page, err := perms.ListDocumentPermissions("doc", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(page); !equal(got, []string{"dev-10:true", "dev-2:false"}) {
		t.Fatalf("page = %v", got)
	}
}

// A deregistered device carries no entry (its ledger rows are cleared by the
// deregistration cascade), and re-registering the id starts with the default.
func TestListDocumentPermissionsDeregisteredDevice(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	for _, id := range []string{"dev-1", "dev-2"} {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if changed, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil || !changed {
		t.Fatalf("revoke = changed:%v err:%v", changed, err)
	}
	if err := s.DeregisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	entries, err := s.ListDocumentPermissions("doc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := idsOf(entries); !equal(got, []string{"dev-2:true"}) {
		t.Fatalf("ledger after deregister = %v", got)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	entries, _ = s.ListDocumentPermissions("doc", 100, 0)
	if got := idsOf(entries); !equal(got, []string{"dev-1:true", "dev-2:true"}) {
		t.Fatalf("ledger after re-register = %v", got)
	}
	// No state is inherited: the first revoke after re-registration is a real
	// change.
	if changed, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil || !changed {
		t.Fatalf("revoke after re-register = changed:%v err:%v", changed, err)
	}
}

// With no registered devices the ledger is an empty (non-nil) slice, including
// for an unknown document.
func TestListDocumentPermissionsEmpty(t *testing.T) {
	_, perms := openKernelPerms(t)
	entries, err := perms.ListDocumentPermissions("anything", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("empty ledger = %v, want no entries", entries)
	}
}

// The ledger and its order survive a restart byte-for-byte at the service
// level.
func TestListDocumentPermissionsPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"dev-2", "dev-1"} {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListDocumentPermissions("doc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	after, err := s2.ListDocumentPermissions("doc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !equal(idsOf(before), idsOf(after)) || !equal(idsOf(before), []string{"dev-1:false", "dev-2:true"}) {
		t.Fatalf("before=%v after=%v", idsOf(before), idsOf(after))
	}
}

// The read is read-only: it does not register devices, does not create ledger
// rows (a subsequent first revoke is still a real change) and does not touch
// the registration table.
func TestListDocumentPermissionsIsReadOnly(t *testing.T) {
	kernel, perms := openKernelPerms(t)
	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := perms.ListDocumentPermissions("doc", 100, 0); err != nil {
			t.Fatal(err)
		}
	}
	exists, err := store.DeviceExistsTx(kernel.DB(), "ghost")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("the ledger read registered a device")
	}
	changed, err := perms.SetDocumentPermission("doc", "dev", false)
	if err != nil || !changed {
		t.Fatalf("first revoke after reads = changed:%v err:%v", changed, err)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
