package store

import (
	"errors"
	"path/filepath"
	"testing"
)

// A device's sessions list in lexicographic id order, independent of creation
// order; another device's sessions never appear.
func TestListSessionsOrderAndOwnership(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	// Create out of lexicographic order to prove the listing sorts rather
	// than echoing insertion order.
	for _, id := range []string{"sess-c", "sess-a", "sess-b", "sess-a2"} {
		if _, err := s.CreateSession("dev-1", id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.CreateSession("dev-2", "sess-foreign"); err != nil {
		t.Fatal(err)
	}

	got, err := s.ListSessions("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"sess-a", "sess-a2", "sess-b", "sess-c"}
	if len(got) != len(want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("list = %v, want %v", got, want)
		}
	}

	// The other device sees only its own session.
	got, err = s.ListSessions("dev-2", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "sess-foreign" {
		t.Fatalf("dev-2 list = %v, want [sess-foreign]", got)
	}

	// A registered device with no sessions gets an empty, non-nil page.
	if _, err := s.RegisterDevice("dev-3"); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListSessions("dev-3", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty list = %v, want a non-nil empty slice", got)
	}

	// A never-registered device misses with ErrDeviceNotFound and no list.
	if _, err := s.ListSessions("ghost", 100, 0); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}
}

// limit/offset slice the fixed lexicographic order without repeats or gaps;
// an offset past the end is an empty page.
func TestListSessionsPagination(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	ids := []string{"s0", "s1", "s2", "s3", "s4"}
	for _, id := range ids {
		if _, err := s.CreateSession("dev", id); err != nil {
			t.Fatal(err)
		}
	}

	// Default-sized page holds everything.
	got, err := s.ListSessions("dev", 100, 0)
	if err != nil || len(got) != 5 {
		t.Fatalf("full page = %v, err %v", got, err)
	}

	// Walk the order in pages of two: no duplicates, no gaps.
	var seen []string
	for offset := int64(0); offset < 5; offset += 2 {
		page, err := s.ListSessions("dev", 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, page...)
	}
	if len(seen) != 5 {
		t.Fatalf("paged walk = %v, want all five once", seen)
	}
	for i, id := range ids {
		if seen[i] != id {
			t.Fatalf("paged walk = %v, want %v", seen, ids)
		}
	}

	// Partial last page and a page past the end.
	page, err := s.ListSessions("dev", 4, 3)
	if err != nil || len(page) != 2 || page[0] != "s3" || page[1] != "s4" {
		t.Fatalf("last partial page = %v, err %v", page, err)
	}
	page, err = s.ListSessions("dev", 100, 5)
	if err != nil || len(page) != 0 {
		t.Fatalf("page past end = %v, err %v", page, err)
	}
}

// A deleted session leaves the listing at once; a session id re-created after
// deletion sorts by its id like any live session.
func TestListSessionsReflectsDeleteAndRecreate(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sess-a", "sess-b"} {
		if _, err := s.CreateSession("dev", id); err != nil {
			t.Fatal(err)
		}
	}

	if err := s.DeleteSession("dev", "sess-a"); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListSessions("dev", 100, 0)
	if err != nil || len(got) != 1 || got[0] != "sess-b" {
		t.Fatalf("list after delete = %v, err %v", got, err)
	}

	// Re-create the deleted id: it is a live session again and sorts first.
	if _, err := s.CreateSession("dev", "sess-a"); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListSessions("dev", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "sess-a" || got[1] != "sess-b" {
		t.Fatalf("list after recreate = %v, want [sess-a sess-b]", got)
	}
}

// Deregistering a device removes every session it owned, so its listing
// misses; another device's sessions are untouched.
func TestListSessionsAfterDeviceDeregister(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-2", "sess-b"); err != nil {
		t.Fatal(err)
	}

	if err := deleteDevice(t, s, "dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListSessions("dev-1", 100, 0); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("deregistered device list err = %v, want ErrDeviceNotFound", err)
	}
	if exists, _ := s.SessionExists("sess-a"); exists {
		t.Fatal("cascade left the deregistered device's session behind")
	}
	got, err := s.ListSessions("dev-2", 100, 0)
	if err != nil || len(got) != 1 || got[0] != "sess-b" {
		t.Fatalf("other device list = %v, err %v", got, err)
	}
}

// The listing is read-only and durable: the same page is byte-for-byte the
// same content after a process restart.
func TestListSessionsSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"sess-c", "sess-a", "sess-b"} {
		if _, err := s.CreateSession("dev", id); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.ListSessions("dev", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	after, err := s2.ListSessions("dev", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("page after restart = %v, want %v", after, before)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("page after restart = %v, want %v", after, before)
		}
	}
}
