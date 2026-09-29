package events_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// Pending statistics count only the device's own online changes per document,
// each document at most once, sorted by document id with the count and the
// maximum cursor among the surviving rows.
func TestListDevicePendingBasic(t *testing.T) {
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

	// dev-1: two changes on doc-c, one each on doc-a/doc-b; dev-2 appends a
	// third change to doc-c and one on doc-z.
	if _, err := s.PostChanges("doc-c", changes("c1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc-a", changes("c2")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc-b", changes("c3")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc-c", changes("c4")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc-z", []events.Change{
		{ID: "z1", DeviceID: "dev-2", Payload: []byte(`{"n":9}`)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PostChanges("doc-c", []events.Change{
		{ID: "c5", DeviceID: "dev-2", Payload: []byte(`{"n":9}`)},
	}); err != nil {
		t.Fatal(err)
	}

	stats, err := s.Events().ListDevicePending("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []events.PendingStat{
		{DocumentID: "doc-a", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "doc-b", ChangeCount: 1, MaxCursor: 1},
		{DocumentID: "doc-c", ChangeCount: 2, MaxCursor: 2},
	}
	if len(stats) != len(want) {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}
	for i := range want {
		if stats[i] != want[i] {
			t.Fatalf("stat %d = %+v, want %+v", i, stats[i], want[i])
		}
	}

	// dev-2's row of doc-c is counted on its own, with the shared cursor.
	stats, err = s.Events().ListDevicePending("dev-2", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 ||
		stats[0] != (events.PendingStat{DocumentID: "doc-c", ChangeCount: 1, MaxCursor: 3}) ||
		stats[1] != (events.PendingStat{DocumentID: "doc-z", ChangeCount: 1, MaxCursor: 1}) {
		t.Fatalf("dev-2 stats = %+v", stats)
	}
}

// limit/offset slice the fixed sorted order; a page past the end is an empty
// non-nil slice.
func TestListDevicePendingPaging(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	for _, doc := range []string{"d2", "d0", "d1"} {
		if _, err := s.PostChanges(doc, []events.Change{
			{ID: "c-" + doc, DeviceID: "dev", Payload: []byte(`{}`)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	page, err := s.Events().ListDevicePending("dev", 2, 0)
	if err != nil || len(page) != 2 || page[0].DocumentID != "d0" || page[1].DocumentID != "d1" {
		t.Fatalf("first page = %+v err=%v", page, err)
	}
	page, err = s.Events().ListDevicePending("dev", 2, 2)
	if err != nil || len(page) != 1 || page[0].DocumentID != "d2" {
		t.Fatalf("second page = %+v err=%v", page, err)
	}
	page, err = s.Events().ListDevicePending("dev", 2, 9)
	if err != nil || len(page) != 0 || page == nil {
		t.Fatalf("page past end = %+v (nil=%v) err=%v", page, page == nil, err)
	}
}

// An unregistered device is ErrDeviceNotFound and yields no statistics.
func TestListDevicePendingUnknownDevice(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	stats, err := s.Events().ListDevicePending("ghost", 100, 0)
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}
	if stats != nil {
		t.Fatalf("stats = %+v, want nil", stats)
	}
}

// Compaction removes rows from the statistics exactly the way it removes them
// from the listing: partial trimming lowers the count and moves the maximum
// onto the surviving tail; trimming every row drops the document.
func TestListDevicePendingReflectsCompaction(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	mk := func(id string) events.Change {
		return events.Change{ID: id, DeviceID: "dev", Payload: []byte(`{"n":1}`)}
	}
	if _, err := s.PostChanges("doc", []events.Change{mk("c1"), mk("c2"), mk("c3")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompactChanges("doc", "dev"); err != nil {
		t.Fatal(err)
	}

	stats, err := s.Events().ListDevicePending("dev", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 1 || stats[0] != (events.PendingStat{DocumentID: "doc", ChangeCount: 2, MaxCursor: 3}) {
		t.Fatalf("after partial compaction stats = %+v", stats)
	}

	if _, err := s.PutSnapshot("doc", 3, []byte(`{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompactChanges("doc", "dev"); err != nil {
		t.Fatal(err)
	}
	stats, err = s.Events().ListDevicePending("dev", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 0 {
		t.Fatalf("after full compaction stats = %+v, want empty", stats)
	}
}
