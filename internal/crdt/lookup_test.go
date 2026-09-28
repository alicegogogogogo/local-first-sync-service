package crdt_test

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// Found answers carry the originating device and the type-specific
// comparison content exactly as submitted, in request order, with unknown ids
// answered missing.
func TestServiceLookupFoundAndMissingAllTypes(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")

	submitCRDT(t, s, "c", crdt.TypeCounter,
		crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)},
		crdt.Op{ID: "b1", DeviceID: "dev-2", Value: rawInt(3)},
	)
	submitCRDT(t, s, "g", crdt.TypeGSet,
		crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"b", "a"}},
	)
	submitCRDT(t, s, "r", crdt.TypeRegister,
		crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 1, Value: rawJSON(`{"k":"v"}`)},
		crdt.Op{ID: "r2", DeviceID: "dev-1", Version: 2, Value: rawJSON(`null`)},
	)
	submitCRDT(t, s, "o", crdt.TypeORSet,
		crdt.Op{ID: "o1", DeviceID: "dev-1", Action: crdt.ORSetAdd, Element: "apple"},
		crdt.Op{ID: "o2", DeviceID: "dev-2", Action: crdt.ORSetRemove, Element: "apple"},
	)

	got, err := s.GetCRDTOpsByIDs("c", "dev-1", []string{"b1", "nope", "a1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("len = %d, want 3", len(got))
	}
	if got[0].Status != crdt.OpLookupFound || got[0].DeviceID != "dev-2" || string(got[0].Value) != "3" {
		t.Fatalf("b1 = %+v", got[0])
	}
	if got[1].Status != crdt.OpLookupMissing || got[1].DeviceID != "" || got[1].Value != nil {
		t.Fatalf("missing = %+v, want bare missing", got[1])
	}
	if got[2].Status != crdt.OpLookupFound || got[2].DeviceID != "dev-1" || string(got[2].Value) != "5" {
		t.Fatalf("a1 = %+v", got[2])
	}

	// A gset presents the element set in its saved array order.
	gots, err := s.GetCRDTOpsByIDs("g", "dev-1", []string{"g1"})
	if err != nil {
		t.Fatal(err)
	}
	if gots[0].Status != crdt.OpLookupFound || len(gots[0].Elements) != 2 ||
		gots[0].Elements[0] != "b" || gots[0].Elements[1] != "a" {
		t.Fatalf("g1 = %+v", gots[0])
	}

	// A register carries version and the verbatim stored value, null included.
	gotr, err := s.GetCRDTOpsByIDs("r", "dev-1", []string{"r2", "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if gotr[0].Status != crdt.OpLookupFound || gotr[0].Version != 2 || string(gotr[0].Value) != "null" {
		t.Fatalf("r2 = %+v", gotr[0])
	}
	if gotr[1].Version != 1 || string(gotr[1].Value) != `{"k":"v"}` {
		t.Fatalf("r1 = %+v", gotr[1])
	}

	// An orset carries action and element for adds and removes alike.
	oto, err := s.GetCRDTOpsByIDs("o", "dev-1", []string{"o1", "o2"})
	if err != nil {
		t.Fatal(err)
	}
	if oto[0].Action != crdt.ORSetAdd || oto[0].Element != "apple" || oto[0].DeviceID != "dev-1" {
		t.Fatalf("o1 = %+v", oto[0])
	}
	if oto[1].Action != crdt.ORSetRemove || oto[1].Element != "apple" || oto[1].DeviceID != "dev-2" {
		t.Fatalf("o2 = %+v", oto[1])
	}
}

// After compaction, trimmed ids answer "compacted" with no content while the
// retained rows still answer found; orset rows are never trimmed.
func TestServiceLookupCompactedAnswers(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// Counter: a1 is superseded by a2.
	submitCRDT(t, s, "c", crdt.TypeCounter,
		crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)},
		crdt.Op{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)},
	)
	// GSet: g2 is covered by g1.
	submitCRDT(t, s, "g", crdt.TypeGSet,
		crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"a", "b"}},
		crdt.Op{ID: "g2", DeviceID: "dev-1", Elements: []string{"a"}},
	)
	// Register: r1 is superseded by r2.
	submitCRDT(t, s, "r", crdt.TypeRegister,
		crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 1, Value: rawJSON(`"old"`)},
		crdt.Op{ID: "r2", DeviceID: "dev-1", Version: 2, Value: rawJSON(`"new"`)},
	)
	// ORSet: the dead add tag is trimmed but the operation log stays.
	submitCRDT(t, s, "o", crdt.TypeORSet,
		crdt.Op{ID: "o1", DeviceID: "dev-1", Action: crdt.ORSetAdd, Element: "apple"},
		crdt.Op{ID: "o2", DeviceID: "dev-1", Action: crdt.ORSetRemove, Element: "apple"},
	)
	for _, doc := range []string{"c", "g", "r", "o"} {
		if _, err := s.CompactCRDT(doc, "dev-1"); err != nil {
			t.Fatalf("compact %s: %v", doc, err)
		}
	}

	got, err := s.GetCRDTOpsByIDs("c", "dev-1", []string{"a1", "a2", "never"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != crdt.OpLookupCompacted || got[0].DeviceID != "" || got[0].Value != nil {
		t.Fatalf("trimmed a1 = %+v, want bare compacted", got[0])
	}
	if got[1].Status != crdt.OpLookupFound || string(got[1].Value) != "8" {
		t.Fatalf("retained a2 = %+v", got[1])
	}
	if got[2].Status != crdt.OpLookupMissing {
		t.Fatalf("never = %+v, want missing", got[2])
	}

	gotg, err := s.GetCRDTOpsByIDs("g", "dev-1", []string{"g2", "g1"})
	if err != nil {
		t.Fatal(err)
	}
	if gotg[0].Status != crdt.OpLookupCompacted || gotg[0].Elements != nil {
		t.Fatalf("trimmed g2 = %+v, want bare compacted", gotg[0])
	}
	if gotg[1].Status != crdt.OpLookupFound || len(gotg[1].Elements) != 2 {
		t.Fatalf("retained g1 = %+v", gotg[1])
	}

	gotr, err := s.GetCRDTOpsByIDs("r", "dev-1", []string{"r1", "r2"})
	if err != nil {
		t.Fatal(err)
	}
	if gotr[0].Status != crdt.OpLookupCompacted || gotr[0].Version != 0 || gotr[0].Value != nil {
		t.Fatalf("trimmed r1 = %+v, want bare compacted", gotr[0])
	}
	if gotr[1].Status != crdt.OpLookupFound || gotr[1].Version != 2 || string(gotr[1].Value) != `"new"` {
		t.Fatalf("retained r2 = %+v", gotr[1])
	}

	// ORSet compaction leaves the operation log intact: both ids are found.
	oto, err := s.GetCRDTOpsByIDs("o", "dev-1", []string{"o1", "o2"})
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range oto {
		if l.Status != crdt.OpLookupFound {
			t.Fatalf("orset op %d = %+v, want found (op log is never trimmed)", i, l)
		}
	}
}

// A document with no committed CRDT operation is the same not-found verdict
// the state and snapshot reads give, even when every queried id would
// otherwise be missing.
func TestServiceLookupUnknownDocument(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})
	_, err := svc.GetOpsByIDs("never", "dev", []string{"x"})
	if !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// The gate precedes every content observation: an unregistered device and a
// revoked device are rejected before the document's existence or any op id is
// observed.
func TestServiceLookupGateDenials(t *testing.T) {
	kernel, _ := openKernelCRDT(t, nil)
	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	// Seed a document through an allowing service.
	allowed, _ := crdt.New(kernel, allowGate{})
	if _, err := allowed.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawInt(1)},
	}); err != nil {
		t.Fatal(err)
	}

	deny404, _ := crdt.New(kernel, denyGate{err: store.ErrDeviceNotFound})
	if _, err := deny404.GetOpsByIDs("doc", "ghost", []string{"a1"}); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered err = %v, want ErrDeviceNotFound", err)
	}
	deny403, _ := crdt.New(kernel, denyGate{err: store.ErrPermissionDenied})
	if _, err := deny403.GetOpsByIDs("doc", "dev", []string{"a1"}); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked err = %v, want ErrPermissionDenied", err)
	}
	// The gate even precedes the unknown-document verdict.
	if _, err := deny403.GetOpsByIDs("never", "dev", []string{"a1"}); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("gate-before-existence err = %v, want ErrPermissionDenied", err)
	}
}

// The lookup is read-only: no row counts move and the merged state is the
// same before and after, repeatedly.
func TestServiceLookupIsReadOnly(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")
	submitCRDT(t, s, "doc", crdt.TypeCounter,
		crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)},
	)
	before, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.GetCRDTOpsByIDs("doc", "dev-1", []string{"a1", "x"}); err != nil {
			t.Fatal(err)
		}
	}
	after, err := s.GetCRDTSnapshot("doc")
	if err != nil {
		t.Fatal(err)
	}
	if after.Operations != before.Operations || after.Tombstones != before.Tombstones ||
		string(after.Value) != string(before.Value) {
		t.Fatalf("snapshot moved: before=%+v after=%+v", before, after)
	}
}

// Lookup verdicts derive solely from durable rows and are byte-stable across
// a restart, found, compacted and missing alike.
func TestServiceLookupPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crdt-lookup.db")
	s1, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registerDevices(t, s1, "dev-1")
	submitCRDT(t, s1, "doc", crdt.TypeCounter,
		crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)},
		crdt.Op{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)},
	)
	if _, err := s1.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}
	before, err := s1.GetCRDTOpsByIDs("doc", "dev-1", []string{"a1", "a2", "x"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := app.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	after, err := s2.GetCRDTOpsByIDs("doc", "dev-1", []string{"a1", "a2", "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("len changed: %d vs %d", len(after), len(before))
	}
	for i := range before {
		if !reflect.DeepEqual(after[i], before[i]) {
			t.Fatalf("answer %d changed across restart:\nbefore %+v\nafter  %+v", i, before[i], after[i])
		}
	}
}
