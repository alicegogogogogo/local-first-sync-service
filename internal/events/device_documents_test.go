package events_test

import (
	"path/filepath"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// newEventsService opens a kernel and constructs the change event service
// over it, the shape used by the standalone service tests.
func newEventsService(t *testing.T) (*store.Store, *events.Service) {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	svc, err := events.New(kernel, nil)
	if err != nil {
		t.Fatal(err)
	}
	return kernel, svc
}

func mustPost(t *testing.T, svc *events.Service, doc, device string, ids ...string) {
	t.Helper()
	changes := make([]events.Change, 0, len(ids))
	for _, id := range ids {
		changes = append(changes, events.Change{ID: id, DeviceID: device, Payload: []byte(`{"k":1}`)})
	}
	if _, err := svc.PostChanges(doc, changes); err != nil {
		t.Fatalf("post %s: %v", doc, err)
	}
}

// The device listing is the distinct set of documents with an online change
// stamped with the device, sorted and paged, regardless of write order or
// repeated writes; another device's documents never appear.
func TestListDeviceDocumentsTxOrderDedupPaging(t *testing.T) {
	kernel, svc := newEventsService(t)

	mustPost(t, svc, "doc-c", "dev-1", "c1")
	mustPost(t, svc, "doc-a", "dev-1", "c2")
	mustPost(t, svc, "doc-b", "dev-1", "c3")
	mustPost(t, svc, "doc-a", "dev-1", "c4") // same doc, must not duplicate
	mustPost(t, svc, "doc-x", "dev-2", "c5")

	tx, err := kernel.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	got, err := svc.ListDeviceDocumentsTx(tx, "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"doc-a", "doc-b", "doc-c"}
	if len(got) != len(want) {
		t.Fatalf("list = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("list = %v, want %v", got, want)
		}
	}

	// Paging over the fixed order neither repeats nor skips.
	var seen []string
	for off := int64(0); off < 3; off += 2 {
		page, err := svc.ListDeviceDocumentsTx(tx, "dev-1", 2, off)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, page...)
	}
	if len(seen) != 3 || seen[0] != "doc-a" || seen[1] != "doc-b" || seen[2] != "doc-c" {
		t.Fatalf("paged walk = %v", seen)
	}

	// A page past the end is an empty, non-nil slice.
	tail, err := svc.ListDeviceDocumentsTx(tx, "dev-1", 100, 3)
	if err != nil || tail == nil || len(tail) != 0 {
		t.Fatalf("tail = %v err %v", tail, err)
	}

	// The other device sees only its own document; a device with no changes
	// gets the same empty page.
	if got, _ := svc.ListDeviceDocumentsTx(tx, "dev-2", 100, 0); len(got) != 1 || got[0] != "doc-x" {
		t.Fatalf("dev-2 list = %v", got)
	}
	if got, _ := svc.ListDeviceDocumentsTx(tx, "dev-ghost", 100, 0); len(got) != 0 {
		t.Fatalf("ghost list = %v", got)
	}
}

// Fully clearing a document's online changes removes it from the listing; a
// document that keeps any online row stays. The set is derived from the
// online log alone, not the retained compaction summaries.
func TestListDeviceDocumentsTxReflectsClearedLog(t *testing.T) {
	kernel, svc := newEventsService(t)

	mustPost(t, svc, "doc-keep", "dev", "a1")
	mustPost(t, svc, "doc-cleared", "dev", "b1")

	// Move doc-cleared's only online row into the retained-identity table, the
	// way compaction does, then delete the online row without touching the
	// document boundary the listing ignores.
	tx, err := kernel.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(
		`INSERT INTO change_identities (document_id, id, device_id, digest, cursor)
		 VALUES ('doc-cleared', 'b1', 'dev', x'00', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`DELETE FROM changes WHERE document_id = 'doc-cleared'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx2, err := kernel.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx2.Rollback() }()
	got, err := svc.ListDeviceDocumentsTx(tx2, "dev", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "doc-keep" {
		t.Fatalf("list after clearing = %v, want [doc-keep]", got)
	}
}

// The read is durable: the same page renders identically after a restart.
func TestListDeviceDocumentsTxSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")

	kernel, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := events.New(kernel, nil)
	if err != nil {
		t.Fatal(err)
	}
	mustPost(t, svc, "doc-c", "dev", "c1")
	mustPost(t, svc, "doc-a", "dev", "a1")

	read := func(k *store.Store) []string {
		t.Helper()
		s2, err := events.New(k, nil)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := k.DB().Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		got, err := s2.ListDeviceDocumentsTx(tx, "dev", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := read(kernel)
	if err := kernel.Close(); err != nil {
		t.Fatal(err)
	}

	kernel2, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kernel2.Close() }()
	after := read(kernel2)
	if len(before) != len(after) {
		t.Fatalf("after restart = %v, want %v", after, before)
	}
	for i := range before {
		if after[i] != before[i] {
			t.Fatalf("after restart = %v, want %v", after, before)
		}
	}
}
