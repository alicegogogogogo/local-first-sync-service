package store

import (
	"errors"
	"reflect"
	"testing"
)

// ListDeviceSessions returns the device's live session ids, each once, in
// ascending lexicographic order, sliced by limit/offset; an unregistered
// device is ErrDeviceNotFound, and the read writes nothing.
func TestListDeviceSessions(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unregistered device: ErrDeviceNotFound, no listing.
	if _, err := s.ListDeviceSessions("ghost", 100, 0); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	// Insert deliberately out of lexicographic order.
	for _, id := range []string{"s-3", "s-1", "s-10", "s-2"} {
		if _, err := s.CreateSession("dev-1", id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.CreateSession("dev-2", "s-other"); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListDeviceSessions("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"s-1", "s-10", "s-2", "s-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("list = %v, want %v", got, want)
	}

	// Pagination over the fixed order: pages neither repeat nor skip.
	var paged []string
	for offset := int64(0); offset < 4; offset += 2 {
		page, err := s.ListDeviceSessions("dev-1", 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) != 2 {
			t.Fatalf("page at offset %d has %d ids, want 2", offset, len(page))
		}
		paged = append(paged, page...)
	}
	if want := []string{"s-1", "s-10", "s-2", "s-3"}; !reflect.DeepEqual(paged, want) {
		t.Fatalf("paged walk = %v, want %v", paged, want)
	}

	// A page past the end is an empty (non-nil) slice.
	tail, err := s.ListDeviceSessions("dev-1", 100, 4)
	if err != nil || len(tail) != 0 || tail == nil {
		t.Fatalf("tail page = %v (nil=%v) err=%v, want empty non-nil slice", tail, tail == nil, err)
	}

	// The other device is scoped independently.
	got2, err := s.ListDeviceSessions("dev-2", 100, 0)
	if err != nil || !reflect.DeepEqual(got2, []string{"s-other"}) {
		t.Fatalf("dev-2 list = %v err=%v", got2, err)
	}

	// A fresh device with no sessions lists empty.
	if _, err := s.RegisterDevice("dev-3"); err != nil {
		t.Fatal(err)
	}
	empty, err := s.ListDeviceSessions("dev-3", 100, 0)
	if err != nil || len(empty) != 0 || empty == nil {
		t.Fatalf("empty list = %v (nil=%v) err=%v", empty, empty == nil, err)
	}
}

// Deleted sessions stop appearing and deregistration removes every session
// the device owned from the listing.
func TestListDeviceSessionsDeletion(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"s-1", "s-2"} {
		if _, err := s.CreateSession("dev-1", id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.CreateSession("dev-2", "s-3"); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteSession("dev-1", "s-1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.ListDeviceSessions("dev-1", 100, 0); !reflect.DeepEqual(got, []string{"s-2"}) {
		t.Fatalf("after delete = %v", got)
	}

	// Deregister dev-1: its sessions are gone with the device; the listing now
	// reports the device unknown, and dev-2 is unaffected.
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := DeleteDeviceTx(tx, "dev-1"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListDeviceSessions("dev-1", 100, 0); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("after deregister err = %v, want ErrDeviceNotFound", err)
	}
	if exists, _ := s.SessionExists("s-2"); exists {
		t.Fatal("s-2 survived device deregistration")
	}
	if got, _ := s.ListDeviceSessions("dev-2", 100, 0); !reflect.DeepEqual(got, []string{"s-3"}) {
		t.Fatalf("dev-2 changed after dev-1 deregistration: %v", got)
	}
}
