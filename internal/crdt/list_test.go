package crdt_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The listing answers online operations in ascending operation-id order with
// the stored source device, type and type-specific comparison content,
// paged by limit/offset; non-overlapping pages visit every online operation
// exactly once and nothing else.
func TestListOpsOrderingAndPagination(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	// Commit in deliberately non-sorted order so the listing proves its own
	// ordering rather than insertion order.
	submitCRDT(t, s, "doc", crdt.TypeCounter,
		crdt.Op{ID: "c3", DeviceID: "dev-2", Value: rawInt(9)},
	)
	submitCRDT(t, s, "doc", crdt.TypeCounter,
		crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(4)},
	)
	submitCRDT(t, s, "doc", crdt.TypeCounter,
		crdt.Op{ID: "c2", DeviceID: "dev-1", Value: rawInt(4)}, // equal contribution: still online
	)

	var gotIDs []string
	var gotValues []string
	for offset := int64(0); ; offset += 2 {
		page, err := s.ListCRDTOps("doc", "dev-1", 2, offset)
		if err != nil {
			t.Fatalf("page offset=%d: %v", offset, err)
		}
		if page.Compacted != 0 {
			t.Fatalf("compacted = %d, want 0 before compaction", page.Compacted)
		}
		for _, op := range page.Ops {
			if op.Type != crdt.TypeCounter {
				t.Fatalf("%s type = %q, want counter", op.ID, op.Type)
			}
			gotIDs = append(gotIDs, op.ID)
			gotValues = append(gotValues, string(op.Value))
		}
		if len(page.Ops) < 2 {
			break
		}
	}
	wantIDs := []string{"c1", "c2", "c3"}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("paged ids = %v, want %v", gotIDs, wantIDs)
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] {
			t.Fatalf("paged ids = %v, want ascending %v", gotIDs, wantIDs)
		}
	}
	if gotValues[0] != "4" || gotValues[1] != "4" || gotValues[2] != "9" {
		t.Fatalf("paged values = %v, want [4 4 9]", gotValues)
	}

	// An offset past the end is a normal empty page, not an error.
	page, err := s.ListCRDTOps("doc", "dev-1", 100, 99)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Ops) != 0 {
		t.Fatalf("past-end page = %d ops, want 0", len(page.Ops))
	}
}

// The listing renders the stored comparison content for every type exactly
// like a found by-id answer.
func TestListOpsContentPerType(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "g", crdt.TypeGSet, crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"b", "a"}})
	submitCRDT(t, s, "r", crdt.TypeRegister, crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 1, Value: rawJSON(`null`)})
	submitCRDT(t, s, "r", crdt.TypeRegister, crdt.Op{ID: "r2", DeviceID: "dev-1", Version: 2, Value: rawJSON(`{"x":1}`)})
	submitCRDT(t, s, "o", crdt.TypeORSet,
		crdt.Op{ID: "o2", DeviceID: "dev-1", Action: crdt.ORSetRemove, Element: "apple"},
		crdt.Op{ID: "o1", DeviceID: "dev-1", Action: crdt.ORSetAdd, Element: "apple"},
	)

	g, err := s.ListCRDTOps("g", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Ops) != 1 || g.Ops[0].ID != "g1" || string(g.Ops[0].Elements) != `["b","a"]` {
		t.Fatalf("gset page = %+v", g.Ops)
	}

	r, err := s.ListCRDTOps("r", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Ops) != 2 || r.Ops[0].ID != "r1" || r.Ops[0].Version != 1 || string(r.Ops[0].Value) != "null" ||
		r.Ops[1].ID != "r2" || r.Ops[1].Version != 2 || string(r.Ops[1].Value) != `{"x":1}` {
		t.Fatalf("register page = %+v", r.Ops)
	}

	o, err := s.ListCRDTOps("o", "dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Ops) != 2 ||
		o.Ops[0].ID != "o1" || o.Ops[0].Action != crdt.ORSetAdd || o.Ops[0].Element != "apple" ||
		o.Ops[1].ID != "o2" || o.Ops[1].Action != crdt.ORSetRemove || o.Ops[1].Element != "apple" {
		t.Fatalf("orset page = %+v", o.Ops)
	}
	if o.Compacted != 0 {
		t.Fatalf("orset compacted = %d, always 0", o.Compacted)
	}
}

// After compaction, trimmed ids leave the listing entirely — they are not
// restored — and compacted reports the total number trimmed on every page.
func TestListOpsExcludesTrimmedAndCountsThem(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "d1", DeviceID: "dev-2", Value: rawInt(1)})
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "d2", DeviceID: "dev-2", Value: rawInt(3)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	// Each device keeps its maximum (a2 for dev-1, d2 for dev-2); a1 and d1
	// were trimmed, so two identities were retained.
	first, err := s.ListCRDTOps("doc", "dev-1", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Ops) != 1 || first.Ops[0].ID != "a2" {
		t.Fatalf("first page = %+v, want only a2", first.Ops)
	}
	if first.Compacted != 2 {
		t.Fatalf("compacted = %d, want 2", first.Compacted)
	}

	// The trimmed total is page-independent, including on an empty page.
	empty, err := s.ListCRDTOps("doc", "dev-1", 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty.Ops) != 0 || empty.Compacted != 2 {
		t.Fatalf("past-end page = %+v, want 0 ops and compacted 2", empty)
	}
}

// The gate verdicts precede the CRDT existence verdict in the fixed order,
// and a document with no committed operation is ErrNotFound.
func TestListOpsGateOrderAndNotFound(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(1)})

	if _, err := s.ListCRDTOps("doc", "ghost", 100, 0); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered device err = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListCRDTOps("doc", "dev-2", 100, 0); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked device err = %v, want ErrPermissionDenied", err)
	}
	if _, err := s.ListCRDTOps("never", "dev-1", 100, 0); !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("no-crdt document err = %v, want crdt.ErrNotFound", err)
	}
}
