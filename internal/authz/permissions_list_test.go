package authz_test

import (
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
)

// ListDocumentPermissions is the ledger read: every registered device once,
// in device-id order, with its current authorization; the default is
// authorized and a committed revoke/grant flips the bool.
func TestListDocumentPermissionsLifecycle(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	// No registered devices: a non-null empty page for any document, known or
	// not.
	got, err := s.ListDocumentPermissions("doc", 100, 0)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty ledger = %+v err = %v", got, err)
	}

	for _, id := range []string{"dev-b", "dev-a", "dev-c"} {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}

	// Registered out of order, the ledger is still lexicographic and every
	// device starts authorized — even though no permission row exists.
	got, err = s.ListDocumentPermissions("doc", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertLedger(t, got, map[string]bool{"dev-a": true, "dev-b": true, "dev-c": true})

	// A revoke flips exactly that pair.
	if _, err := s.SetDocumentPermission("doc", "dev-b", false); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListDocumentPermissions("doc", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": true, "dev-b": false, "dev-c": true})

	// An idempotent repeat revoke and a grant change the ledger only through
	// the grant.
	if _, err := s.SetDocumentPermission("doc", "dev-b", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-b", true); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListDocumentPermissions("doc", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": true, "dev-b": true, "dev-c": true})

	// The ledger is per document: a revoke on another document is not visible
	// here and an unknown document shows the same default-authorized roster.
	if _, err := s.SetDocumentPermission("other", "dev-a", false); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListDocumentPermissions("doc", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": true, "dev-b": true, "dev-c": true})
	got, _ = s.ListDocumentPermissions("ghost", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": true, "dev-b": true, "dev-c": true})
	got, _ = s.ListDocumentPermissions("other", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": false, "dev-b": true, "dev-c": true})
}

// limit/offset slice one stable lexicographic order: walking pages reproduces
// the full ledger with no duplicate and no gap.
func TestListDocumentPermissionsPagination(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	ids := []string{"d0", "d1", "d2", "d3", "d4"}
	for _, id := range ids {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SetDocumentPermission("doc", "d1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "d4", false); err != nil {
		t.Fatal(err)
	}

	var walked []string
	authByID := map[string]bool{}
	for offset := int64(0); offset < 5; offset += 2 {
		page, err := s.ListDocumentPermissions("doc", 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page {
			walked = append(walked, e.DeviceID)
			authByID[e.DeviceID] = e.Authorized
		}
	}
	if len(walked) != 5 {
		t.Fatalf("paged walk = %v", walked)
	}
	for i, id := range ids {
		if walked[i] != id {
			t.Fatalf("paged order = %v, want %v", walked, ids)
		}
	}
	want := map[string]bool{"d0": true, "d1": false, "d2": true, "d3": true, "d4": false}
	for id, authorized := range want {
		if authByID[id] != authorized {
			t.Fatalf("device %s authorized = %v, want %v", id, authByID[id], authorized)
		}
	}

	// A partial last page and a page past the end.
	page, err := s.ListDocumentPermissions("doc", 4, 3)
	if err != nil || len(page) != 2 || page[0].DeviceID != "d3" || page[1].DeviceID != "d4" {
		t.Fatalf("last partial page = %+v err = %v", page, err)
	}
	page, err = s.ListDocumentPermissions("doc", 100, 5)
	if err != nil || len(page) != 0 {
		t.Fatalf("page past end = %+v err = %v", page, err)
	}
}

// A deregistered device leaves the ledger altogether — its row and the
// default-authorized fallback both disappear — and the same id re-registers
// as a brand-new device with no inherited state.
func TestListDocumentPermissionsDeregistration(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	for _, id := range []string{"dev-a", "dev-b", "dev-c"} {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SetDocumentPermission("doc", "dev-a", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-b", false); err != nil {
		t.Fatal(err)
	}

	if err := s.DeregisterDevice("dev-b"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ListDocumentPermissions("doc", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": false, "dev-c": true})

	// Re-registration starts authorized: the earlier revoke is not inherited.
	if _, err := s.RegisterDevice("dev-b"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListDocumentPermissions("doc", 100, 0)
	assertLedger(t, got, map[string]bool{"dev-a": false, "dev-b": true, "dev-c": true})
	if changed, err := s.SetDocumentPermission("doc", "dev-b", false); err != nil || !changed {
		t.Fatalf("revoke after re-register = changed:%v err:%v, want a fresh first revoke", changed, err)
	}
}

// The ledger content and order are durable state: the same paged request reads
// back byte-compatible entries after a process restart.
func TestListDocumentPermissionsPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"dev-a", "dev-b", "dev-c"} {
		if _, err := s.RegisterDevice(id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SetDocumentPermission("doc", "dev-a", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-c", false); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListDocumentPermissions("doc", 2, 1)
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

	after, err := s2.ListDocumentPermissions("doc", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("after restart = %+v, want %+v", after, before)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("page entry %d after restart = %+v, want %+v", i, after[i], before[i])
		}
	}
}

// assertLedger checks both the ascending device-id order and each device's
// authorized flag.
func assertLedger(t *testing.T, got []authz.PermissionEntry, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ledger len = %d, want %d: %+v", len(got), len(want), got)
	}
	prev := ""
	for _, e := range got {
		if e.DeviceID <= prev && prev != "" {
			t.Fatalf("ledger not ascending at %q after %q: %+v", e.DeviceID, prev, got)
		}
		if e.DeviceID == "" {
			t.Fatalf("empty device id in ledger: %+v", got)
		}
		authorized, ok := want[e.DeviceID]
		if !ok {
			t.Fatalf("unexpected device %q in ledger: %+v", e.DeviceID, got)
		}
		if e.Authorized != authorized {
			t.Fatalf("device %q authorized = %v, want %v", e.DeviceID, e.Authorized, authorized)
		}
		prev = e.DeviceID
	}
	// Also detect duplicates an order-only walk would miss.
	seen := map[string]struct{}{}
	for _, e := range got {
		if _, dup := seen[e.DeviceID]; dup {
			t.Fatalf("device %q listed twice: %+v", e.DeviceID, got)
		}
		seen[e.DeviceID] = struct{}{}
	}
}
