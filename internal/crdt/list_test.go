package crdt_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The listing browses the online operations in ascending operation-id order,
// paged by limit/offset: pages neither repeat nor skip an id, and each found
// entry carries its source device and the stored type-specific content.
func TestListOpsOrderingPaginationAndContent(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	// Counter ops inserted out of id order across two devices; the browse
	// order is the stable id order, not arrival or device order.
	submitCRDT(t, s, "c", crdt.TypeCounter, crdt.Op{ID: "c2", DeviceID: "dev-2", Value: rawInt(7)})
	submitCRDT(t, s, "c", crdt.TypeCounter, crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "c", crdt.TypeCounter, crdt.Op{ID: "c3", DeviceID: "dev-1", Value: rawInt(9)})

	// Gset elements are presented exactly as saved; the register entry carries
	// version plus the verbatim value.
	submitCRDT(t, s, "g", crdt.TypeGSet, crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"b", "a"}})
	submitCRDT(t, s, "r", crdt.TypeRegister, crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 2, Value: rawJSON(`null`)})
	// Orset logs add and remove alike, never compacted.
	submitCRDT(t, s, "o", crdt.TypeORSet, crdt.Op{ID: "o2", DeviceID: "dev-1", Action: crdt.ORSetRemove, Element: "apple"})
	submitCRDT(t, s, "o", crdt.TypeORSet, crdt.Op{ID: "o1", DeviceID: "dev-1", Action: crdt.ORSetAdd, Element: "apple"})

	page1, err := s.ListCRDTOps("c", "dev-1", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Ops) != 2 || page1.Ops[0].ID != "c1" || page1.Ops[1].ID != "c2" {
		t.Fatalf("page1 ids = %+v, want c1,c2", page1.Ops)
	}
	if page1.Compacted != 0 {
		t.Fatalf("compacted before compaction = %d, want 0", page1.Compacted)
	}
	if page1.Ops[0].DeviceID != "dev-1" || string(page1.Ops[0].Value) != "5" {
		t.Fatalf("c1 content = %+v", page1.Ops[0])
	}
	page2, err := s.ListCRDTOps("c", "dev-1", 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Ops) != 1 || page2.Ops[0].ID != "c3" {
		t.Fatalf("page2 ids = %+v, want c3", page2.Ops)
	}
	// An offset past the end is an empty, non-nil page; the trim count stays
	// independent of paging.
	page3, err := s.ListCRDTOps("c", "dev-1", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Ops) != 0 || page3.Ops == nil {
		t.Fatalf("past-end page = %+v, want empty non-nil", page3.Ops)
	}

	// Type-specific content shapes.
	g, err := s.ListCRDTOps("g", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if g.Ops[0].Type != crdt.TypeGSet || string(g.Ops[0].Elements) != `["b","a"]` {
		t.Fatalf("gset row = %+v", g.Ops[0])
	}
	r, err := s.ListCRDTOps("r", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Ops[0].Type != crdt.TypeRegister || r.Ops[0].Version != 2 || string(r.Ops[0].Value) != "null" {
		t.Fatalf("register row = %+v", r.Ops[0])
	}
	o, err := s.ListCRDTOps("o", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Ops) != 2 || o.Ops[0].ID != "o1" || o.Ops[0].Action != crdt.ORSetAdd ||
		o.Ops[1].ID != "o2" || o.Ops[1].Action != crdt.ORSetRemove ||
		o.Ops[0].Element != "apple" {
		t.Fatalf("orset rows = %+v", o.Ops)
	}
}

// After compaction the trimmed ids leave the page and the retained identities
// are reported as the trim total; the surviving online row stays browsable.
func TestListOpsCompactedCount(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	page, err := s.ListCRDTOps("doc", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Ops) != 1 || page.Ops[0].ID != "a2" || string(page.Ops[0].Value) != "8" {
		t.Fatalf("page after compaction = %+v, want only a2", page.Ops)
	}
	if page.Compacted != 1 {
		t.Fatalf("compacted = %d, want 1", page.Compacted)
	}
	// The trimmed id never reappears on a later page.
	next, err := s.ListCRDTOps("doc", "dev-1", 100, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(next.Ops) != 0 || next.Compacted != 1 {
		t.Fatalf("next page = %+v, want empty with compacted 1", next)
	}
}

// The fixed verdict order: device existence (404), permission (403), CRDT
// state existence (404), all before any content is observed; the listing is
// read-only and a repeat returns the same data.
func TestListOpsGateOrderAndReadOnly(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(1)})

	if _, err := s.ListCRDTOps("doc", "ghost", 100, 0); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered device = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListCRDTOps("doc", "dev-2", 100, 0); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked device = %v, want ErrPermissionDenied", err)
	}
	if _, err := s.ListCRDTOps("never-a-crdt-doc", "dev-1", 100, 0); !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("no-crdt document = %v, want crdt.ErrNotFound", err)
	}

	first, err := s.ListCRDTOps("doc", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.ListCRDTOps("doc", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Ops) != 1 || len(again.Ops) != 1 ||
		first.Ops[0].ID != again.Ops[0].ID || first.Compacted != again.Compacted {
		t.Fatalf("repeat listing changed: %+v vs %+v", first, again)
	}
	// A read changed no storage: the snapshot operation count is unchanged.
	snap, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if snap.Operations != 1 {
		t.Fatalf("listing wrote operations: snapshot = %+v", snap)
	}
}
