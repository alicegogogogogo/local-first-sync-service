package events_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// pendingSpec is one expected statistics row in test comparisons.
type pendingSpec struct {
	documentID string
	count      int64
	maxCursor  int64
}

func pendingSpecs(stats []events.PendingChangeStat) []pendingSpec {
	out := make([]pendingSpec, len(stats))
	for i, st := range stats {
		out[i] = pendingSpec{st.DocumentID, st.Count, st.MaxCursor}
	}
	return out
}

// Each document the device authored appears once, sorted by id; count and
// max cursor cover only the device's own online rows even when another
// device's changes interleave on the same document.
func TestListDevicePendingChangesBasic(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	// doc-a, interleaved: dev-1 cursor 1, dev-2 cursor 2, dev-1 cursor 3.
	if err := post(s, "doc-a", "dev-1", "a1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-a", "dev-2", "a2", `{"n":2}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-a", "dev-1", "a3", `{"n":3}`); err != nil {
		t.Fatal(err)
	}
	// doc-c: one dev-1 change; doc-b: two dev-1 changes committed out of
	// id order so sorting has something to do.
	if err := post(s, "doc-c", "dev-1", "c1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-b", "dev-1", "b2", `{"n":2}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-b", "dev-1", "b1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}
	// doc-foreign belongs to dev-2 alone and must never appear for dev-1.
	if err := post(s, "doc-foreign", "dev-2", "f1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}

	stats, err := s.ListDevicePendingChanges("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []pendingSpec{
		{"doc-a", 2, 3},
		{"doc-b", 2, 2},
		{"doc-c", 1, 1},
	}
	if got := pendingSpecs(stats); !equalPendingSpecs(got, want) {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}

	// dev-2 sees only its own rows, on the documents it authored.
	stats, err = s.ListDevicePendingChanges("dev-2", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	want = []pendingSpec{
		{"doc-a", 1, 2},
		{"doc-foreign", 1, 1},
	}
	if got := pendingSpecs(stats); !equalPendingSpecs(got, want) {
		t.Fatalf("dev-2 stats = %+v, want %+v", got, want)
	}

	// A registered device that never wrote gets an empty (non-nil) page.
	if _, err := s.RegisterDevice("dev-3"); err != nil {
		t.Fatal(err)
	}
	stats, err = s.ListDevicePendingChanges("dev-3", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 0 {
		t.Fatalf("empty stats = %+v", stats)
	}
}

func equalPendingSpecs(got, want []pendingSpec) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// limit/offset slice the fixed document-id order without repeats or gaps, and
// an offset past the end yields an empty page.
func TestListDevicePendingChangesPagination(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	for i, doc := range []string{"d0", "d1", "d2", "d3", "d4"} {
		if err := post(s, doc, "dev", "c-"+doc, fmt.Sprintf(`{"n":%d}`, i+1)); err != nil {
			t.Fatal(err)
		}
	}

	// Walk the stats in pages of two: no duplicates, no gaps.
	var seen []pendingSpec
	for offset := int64(0); offset < 5; offset += 2 {
		page, err := s.ListDevicePendingChanges("dev", 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, pendingSpecs(page)...)
	}
	want := []pendingSpec{
		{"d0", 1, 1},
		{"d1", 1, 1},
		{"d2", 1, 1},
		{"d3", 1, 1},
		{"d4", 1, 1},
	}
	if !equalPendingSpecs(seen, want) {
		t.Fatalf("paged walk = %+v, want %+v", seen, want)
	}

	// A partial last page and a page past the end.
	page, err := s.ListDevicePendingChanges("dev", 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := pendingSpecs(page); !equalPendingSpecs(got, []pendingSpec{{"d3", 1, 1}, {"d4", 1, 1}}) {
		t.Fatalf("last partial page = %+v", got)
	}
	past, err := s.ListDevicePendingChanges("dev", 100, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(past) != 0 {
		t.Fatalf("page past end = %+v", past)
	}
}

// An unregistered device is ErrDeviceNotFound, before any statistics are
// observed.
func TestListDevicePendingChangesUnknownDevice(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.ListDevicePendingChanges("ghost", 100, 0); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeregisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListDevicePendingChanges("dev", 100, 0); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("deregistered device err = %v, want ErrDeviceNotFound", err)
	}
}

// Compacting a device's changes out of the online log removes them from the
// count and maximum cursor; once every one of the device's online changes is
// trimmed, its document no longer appears at all. A whole-document deletion
// removes it likewise.
func TestListDevicePendingChangesReflectsCompactionAndDelete(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc", changes("c1", "c2", "c3")); err != nil {
		t.Fatal(err)
	}
	if created, err := s.PutSnapshot("doc", 2, json.RawMessage(`{"s":1}`)); err != nil || !created {
		t.Fatalf("snapshot: created=%v err=%v", created, err)
	}
	if _, _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// c1 and c2 left the online log; only c3 at cursor 3 remains.
	stats, err := s.ListDevicePendingChanges("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := pendingSpecs(stats); !equalPendingSpecs(got, []pendingSpec{{"doc", 1, 3}}) {
		t.Fatalf("stats after compaction = %+v", got)
	}

	// Pin another snapshot and compact again: the last online row is trimmed,
	// so the document disappears even though its retained summaries survive.
	if created, err := s.PutSnapshot("doc", 3, json.RawMessage(`{"s":2}`)); err != nil || !created {
		t.Fatalf("second snapshot: created=%v err=%v", created, err)
	}
	if _, _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	stats, err = s.ListDevicePendingChanges("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 0 {
		t.Fatalf("stats after full compaction = %+v, want empty", stats)
	}
	var summaries int
	if err := s.DB().QueryRow(
		`SELECT COUNT(*) FROM change_identities WHERE document_id = 'doc'`,
	).Scan(&summaries); err != nil {
		t.Fatal(err)
	}
	if summaries == 0 {
		t.Fatal("retained summaries should survive compaction")
	}

	// A new change brings the document back with the fresh cursor alone.
	if err := post(s, "doc", "dev-1", "c4", `{"n":4}`); err != nil {
		t.Fatal(err)
	}
	stats, err = s.ListDevicePendingChanges("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := pendingSpecs(stats); !equalPendingSpecs(got, []pendingSpec{{"doc", 1, 4}}) {
		t.Fatalf("stats after new change = %+v", got)
	}

	// Deleting the whole document removes every row the stats derive from.
	if err := s.DeleteDocument("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	stats, err = s.ListDevicePendingChanges("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 0 {
		t.Fatalf("stats after document delete = %+v, want empty", stats)
	}
}

// Repeated reads and reads across a process restart return the same stats.
func TestListDevicePendingChangesDeterministicAndDurable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) (*app.App, func()) {
		t.Helper()
		s, err := app.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return s, func() { _ = s.Close() }
	}

	s, close1 := open(t)
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-c", "dev", "c1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-a", "dev", "a1", `{"n":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc-a", "dev", "a2", `{"n":2}`); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		stats, err := s.ListDevicePendingChanges("dev", 100, 0)
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%+v", pendingSpecs(stats))
	}
	before := snapshot()
	if again := snapshot(); again != before {
		t.Fatalf("repeat read changed: %q vs %q", again, before)
	}
	close1()

	s, close2 := open(t)
	defer close2()
	if after := snapshot(); after != before {
		t.Fatalf("stats after restart = %q, want %q", after, before)
	}
}
