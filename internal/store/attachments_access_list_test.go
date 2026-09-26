package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestListAttachmentAccess(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	mustRegisterDevice(t, s, "dev-3")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}

	// Unknown attachment and non-creator caller: independent errors, no list.
	if _, err := s.ListAttachmentAccess("dev-1", "nope", 100, 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.ListAttachmentAccess("dev-2", "att-1", 100, 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator = %v, want ErrAttachmentForbidden", err)
	}

	// No grants yet: an empty, non-nil list.
	devices, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || devices == nil || len(devices) != 0 {
		t.Fatalf("empty list = %v %v", devices, err)
	}

	// Grants list in the order they took effect; a revoke removes the device;
	// a re-grant lists it once at its new position.
	grant := func(target string) {
		t.Helper()
		if _, err := s.SetAttachmentAccess("dev-1", "att-1", target, true); err != nil {
			t.Fatal(err)
		}
	}
	revoke := func(target string) {
		t.Helper()
		if _, err := s.SetAttachmentAccess("dev-1", "att-1", target, false); err != nil {
			t.Fatal(err)
		}
	}
	grant("dev-3")
	grant("dev-2")
	devices, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || !equalIDLists(devices, []string{"dev-3", "dev-2"}) {
		t.Fatalf("list = %v %v", devices, err)
	}
	revoke("dev-3")
	grant("dev-3")
	devices, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || !equalIDLists(devices, []string{"dev-2", "dev-3"}) {
		t.Fatalf("list after re-grant = %v %v", devices, err)
	}

	// limit/offset page the same stable order.
	devices, err = s.ListAttachmentAccess("dev-1", "att-1", 1, 1)
	if err != nil || !equalIDLists(devices, []string{"dev-3"}) {
		t.Fatalf("page = %v %v", devices, err)
	}
	devices, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 2)
	if err != nil || devices == nil || len(devices) != 0 {
		t.Fatalf("offset past end = %v %v", devices, err)
	}
}

// A database written before the grant-order column existed is migrated on
// open: the relative order of the rows it held is preserved and new grants
// take strictly later positions.
func TestListAttachmentAccessMigratesGrantOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	mustRegisterDevice(t, s, "dev-3")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-3", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a pre-migration database by dropping the column.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE attachment_access DROP COLUMN granted_seq`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// The still-authorized device keeps listing; a re-grant of the revoked
	// device lands after it, at the position of its most recent grant.
	devices, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || !equalIDLists(devices, []string{"dev-3"}) {
		t.Fatalf("list after migration = %v %v", devices, err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	devices, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || !equalIDLists(devices, []string{"dev-3", "dev-2"}) {
		t.Fatalf("list after migrated re-grant = %v %v", devices, err)
	}
}
