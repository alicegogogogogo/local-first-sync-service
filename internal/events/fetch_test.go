package events_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The gated by-id fetch answers one entry per requested id in request order:
// online hits carry device, verbatim payload and cursor; unknown ids are
// missing.
func TestFetchChangesOnlineAndMissing(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	compactSeed(t, s) // dev-1 registered, c1..c3 committed, no compaction yet

	got, err := s.FetchChanges("doc", "dev-1", []string{"c3", "ghost", "c1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	if got[0].Status != events.FetchStatusOnline || got[0].Cursor != 3 || got[0].DeviceID != "dev-1" {
		t.Fatalf("c3 = %+v", got[0])
	}
	if string(got[0].Payload) != `{"n":3}` {
		t.Fatalf("c3 payload = %s, want verbatim stored JSON", got[0].Payload)
	}
	if got[1].Status != events.FetchStatusMissing || got[1].Cursor != 0 ||
		len(got[1].Payload) != 0 || got[1].DeviceID != "" {
		t.Fatalf("ghost = %+v, want a bare missing entry", got[1])
	}
	if got[2].Status != events.FetchStatusOnline || got[2].Cursor != 1 {
		t.Fatalf("c1 = %+v", got[2])
	}
}

// An id compaction trimmed is answered from the retained summary alone:
// compacted marker and first cursor, no device or payload; it never returns to
// the online results.
func TestFetchChangesTrimmedReportsSummaryOnly(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	compactSeed(t, s)
	if _, _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	got, err := s.FetchChanges("doc", "dev-1", []string{"c2", "never", "c1", "c3"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != events.FetchStatusCompacted || got[0].Cursor != 2 ||
		len(got[0].Payload) != 0 || got[0].DeviceID != "" {
		t.Fatalf("c2 = %+v, want cursor-only compacted entry", got[0])
	}
	if got[1].Status != events.FetchStatusMissing {
		t.Fatalf("never = %+v, want missing", got[1])
	}
	if got[2].Status != events.FetchStatusCompacted || got[2].Cursor != 1 {
		t.Fatalf("c1 = %+v", got[2])
	}
	if got[3].Status != events.FetchStatusOnline || got[3].Cursor != 3 {
		t.Fatalf("c3 = %+v", got[3])
	}
}

// The gate is taken before any content is read: an unregistered device gets
// ErrDeviceNotFound, a revoked one ErrPermissionDenied, and neither leaves a
// trace on an unknown document.
func TestFetchChangesGateBeforeContent(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	compactSeed(t, s)

	if _, err := s.FetchChanges("doc", "ghost", []string{"c1"}); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered device err = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.FetchChanges("doc", "dev-2", []string{"c1"}); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked device err = %v, want ErrPermissionDenied", err)
	}
}

// A fetch is a pure read: it creates no change and moves no cursor, so the
// next commit lands on the cursor it would have without the fetch.
func TestFetchChangesAllocatesNoCursor(t *testing.T) {
	s, err := app.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	compactSeed(t, s) // current cursor is 3

	if _, err := s.FetchChanges("doc", "dev-1", []string{"c1", "ghost"}); err != nil {
		t.Fatal(err)
	}
	if rows, next, err := s.ListChanges("doc", 3, 10); err != nil || len(rows) != 0 || next != 3 {
		t.Fatalf("fetch moved the cursor: rows=%v next=%d err=%v", rows, next, err)
	}
}
