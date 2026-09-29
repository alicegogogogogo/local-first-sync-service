package events_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// An unknown document answers with all four numbers zero: empty state is an
// answer, not an error, and the read creates neither a change nor a snapshot.
func TestGetChangesStatusUnknownDocument(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetChangesStatus("never-heard-of-it", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{}) {
		t.Fatalf("unknown document status = %+v, want all zero", got)
	}

	// The miss left nothing behind: the document still reads as unknown
	// through its change log and carries no snapshot boundary.
	if known, err := s.DocumentExists("never-heard-of-it"); err != nil || known {
		t.Fatalf("status read created state: known=%v err=%v", known, err)
	}
}

// With only online changes the numbers count the online log, and a snapshot
// without a trim raises the boundary alone.
func TestGetChangesStatusOnlineAndSnapshotBoundary(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-2", "b", `{"v":2}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "c", `{"v":3}`); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 3, Boundary: 0, MaxCursor: 3, TrimmedCount: 0}) {
		t.Fatalf("online-only status = %+v", got)
	}

	// A snapshot pins the boundary to its cursor but trims nothing by itself.
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 3, Boundary: 2, MaxCursor: 3, TrimmedCount: 0}) {
		t.Fatalf("status after snapshot = %+v, want boundary 2 only", got)
	}
}

// After compaction the online count and max cursor cover only the surviving
// tail, the boundary is the saved-snapshot cursor and trimmed counts exactly
// the moved ids; a repeat compaction moves nothing more.
func TestGetChangesStatusAfterCompaction(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"a", "b", "c", "d"} {
		if err := post(s, "doc", "dev-1", id, `{"v":`+strconv.Itoa(i+1)+`}`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	if boundary, removed, err := s.CompactChanges("doc", "dev-1"); err != nil || boundary != 2 || removed != 2 {
		t.Fatalf("compact = boundary %d removed %d err %v", boundary, removed, err)
	}

	got, err := s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 2, Boundary: 2, MaxCursor: 4, TrimmedCount: 2}) {
		t.Fatalf("post-compaction status = %+v, want 2 online, boundary 2, max 4, 2 trimmed", got)
	}

	// A repeat compaction trims nothing new and the four numbers are stable.
	if _, removed, err := s.CompactChanges("doc", "dev-1"); err != nil || removed != 0 {
		t.Fatalf("repeat compact removed = %d err %v", removed, err)
	}
	got, err = s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 2, Boundary: 2, MaxCursor: 4, TrimmedCount: 2}) {
		t.Fatalf("status after repeat compact = %+v", got)
	}

	// New commits past the boundary extend the online log without disturbing
	// the trimmed count.
	if err := post(s, "doc", "dev-1", "e", `{"v":5}`); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 3, Boundary: 2, MaxCursor: 5, TrimmedCount: 2}) {
		t.Fatalf("status after new commit = %+v", got)
	}
}

// A document trimmed down to its boundary reports zero online rows, a zero
// max online cursor and the full trimmed count.
func TestGetChangesStatusFullyTrimmed(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetChangesStatus("doc", "dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if got != (events.ChangeStatus{OnlineCount: 0, Boundary: 1, MaxCursor: 0, TrimmedCount: 1}) {
		t.Fatalf("fully trimmed status = %+v, want 0 online, boundary 1, max 0, 1 trimmed", got)
	}
}

// Registration then permission are enforced before any content is observed.
func TestGetChangesStatusGate(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}

	if _, err := s.GetChangesStatus("doc", "ghost"); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetChangesStatus("doc", "dev-1"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked = %v, want ErrPermissionDenied", err)
	}
}

// The status read is repeatable: it consumes no cursor, so a following commit
// takes the next cursor exactly as without the reads.
func TestGetChangesStatusMovesNoCursor(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.GetChangesStatus("doc", "dev-1"); err != nil {
			t.Fatal(err)
		}
	}
	results, err := s.PostChanges("doc", []events.Change{
		{ID: "b", DeviceID: "dev-1", Payload: json.RawMessage(`{"v":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Cursor != 2 {
		t.Fatalf("post after status reads = %+v, want cursor 2", results)
	}
}
