package crdt_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The lookup answers one entry per id in request order for every type: found
// rows carry their source device and the stored type-specific comparison
// content; never-known ids are missing.
func TestGetOpsByIDsFoundAndMissingPerType(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// Counter: contribution value.
	submitCRDT(t, s, "counter-doc", crdt.TypeCounter, crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(5)})
	// GSet: the element set stored as submitted.
	submitCRDT(t, s, "gset-doc", crdt.TypeGSet, crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"b", "a"}})
	// Register: version and value.
	submitCRDT(t, s, "register-doc", crdt.TypeRegister, crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 3, Value: rawJSON(`{"k":"v"}`)})
	// ORSet: add then remove.
	submitCRDT(t, s, "orset-doc", crdt.TypeORSet, crdt.Op{ID: "o1", DeviceID: "dev-1", Action: crdt.ORSetAdd, Element: "apple"})
	submitCRDT(t, s, "orset-doc", crdt.TypeORSet, crdt.Op{ID: "o2", DeviceID: "dev-1", Action: crdt.ORSetRemove, Element: "apple"})

	type want struct {
		id, status, device, kind string
		version                  int64
		value, elements          string
		action, element          string
	}
	cases := []struct {
		doc  string
		ids  []string
		want []want
	}{
		{"counter-doc", []string{"nope", "c1"}, []want{
			{id: "nope", status: crdt.LookupMissing},
			{id: "c1", status: crdt.LookupFound, device: "dev-1", kind: crdt.TypeCounter, value: "5"},
		}},
		{"gset-doc", []string{"g1", "nope"}, []want{
			{id: "g1", status: crdt.LookupFound, device: "dev-1", kind: crdt.TypeGSet, elements: `["b","a"]`},
			{id: "nope", status: crdt.LookupMissing},
		}},
		{"register-doc", []string{"r1", "nope"}, []want{
			{id: "r1", status: crdt.LookupFound, device: "dev-1", kind: crdt.TypeRegister, version: 3, value: `{"k":"v"}`},
			{id: "nope", status: crdt.LookupMissing},
		}},
		{"orset-doc", []string{"o1", "o2", "nope"}, []want{
			{id: "o1", status: crdt.LookupFound, device: "dev-1", kind: crdt.TypeORSet, action: crdt.ORSetAdd, element: "apple"},
			{id: "o2", status: crdt.LookupFound, device: "dev-1", kind: crdt.TypeORSet, action: crdt.ORSetRemove, element: "apple"},
			{id: "nope", status: crdt.LookupMissing},
		}},
	}

	for _, tc := range cases {
		got, err := s.GetCRDTOpsByIDs(tc.doc, "dev-1", tc.ids)
		if err != nil {
			t.Fatalf("%s lookup: %v", tc.doc, err)
		}
		if len(got) != len(tc.want) {
			t.Fatalf("%s got %d answers, want %d", tc.doc, len(got), len(tc.want))
		}
		for i, w := range tc.want {
			g := got[i]
			if g.ID != w.id || g.Status != w.status || g.DeviceID != w.device || g.Type != w.kind {
				t.Fatalf("%s[%d] = %+v, want id=%s status=%s device=%s type=%s", tc.doc, i, g, w.id, w.status, w.device, w.kind)
			}
			if w.status != crdt.LookupFound {
				if g.Value != nil || g.Elements != nil || g.Version != 0 || g.Action != "" || g.Element != "" {
					t.Fatalf("%s[%d] non-found answer carried content: %+v", tc.doc, i, g)
				}
				continue
			}
			if w.value != "" && string(g.Value) != w.value {
				t.Fatalf("%s[%d] value = %s, want %s", tc.doc, i, g.Value, w.value)
			}
			if w.elements != "" && string(g.Elements) != w.elements {
				t.Fatalf("%s[%d] elements = %s, want %s", tc.doc, i, g.Elements, w.elements)
			}
			if w.kind == crdt.TypeRegister && g.Version != w.version {
				t.Fatalf("%s[%d] version = %d, want %d", tc.doc, i, g.Version, w.version)
			}
			if g.Action != w.action || g.Element != w.element {
				t.Fatalf("%s[%d] action/element = %q/%q, want %q/%q", tc.doc, i, g.Action, g.Element, w.action, w.element)
			}
		}
	}
}

// A compacted counter id answers "compacted" with no device or content: the
// retained identity reports the trim alone, while the surviving maximum stays
// found with its full content.
func TestGetOpsByIDsCompactedCounter(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a1", DeviceID: "dev-1", Value: rawInt(5)})
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "a2", DeviceID: "dev-1", Value: rawInt(8)})
	if _, err := s.CompactCRDT("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetCRDTOpsByIDs("doc", "dev-1", []string{"a1", "a2", "never"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].ID != "a1" || got[0].Status != crdt.LookupCompacted || got[0].DeviceID != "" || got[0].Value != nil {
		t.Fatalf("trimmed answer = %+v, want compacted with no device or content", got[0])
	}
	if got[1].Status != crdt.LookupFound || got[1].DeviceID != "dev-1" || string(got[1].Value) != "8" {
		t.Fatalf("surviving answer = %+v, want found dev-1 value 8", got[1])
	}
	if got[2].Status != crdt.LookupMissing {
		t.Fatalf("unknown answer = %s, want missing", got[2].Status)
	}
}

// A compacted gset id and a compacted register id answer "compacted" with no
// content, while their retained identities keep the idempotency/conflict
// decisions working.
func TestGetOpsByIDsCompactedGSetAndRegister(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1")

	// Ascending-id prefix cover: g1 first contributes "a" (and "b") and is
	// kept; g2 adds nothing new and is trimmed.
	submitCRDT(t, s, "g", crdt.TypeGSet, crdt.Op{ID: "g1", DeviceID: "dev-1", Elements: []string{"a", "b"}})
	submitCRDT(t, s, "g", crdt.TypeGSet, crdt.Op{ID: "g2", DeviceID: "dev-1", Elements: []string{"a"}})
	if _, err := s.CompactCRDT("g", "dev-1"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCRDTOpsByIDs("g", "dev-1", []string{"g1", "g2"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != crdt.LookupFound || string(got[0].Elements) != `["a","b"]` {
		t.Fatalf("g1 = %+v, want found [a,b]", got[0])
	}
	if got[1].Status != crdt.LookupCompacted || got[1].Elements != nil {
		t.Fatalf("g2 = %+v, want compacted with no elements", got[1])
	}

	submitCRDT(t, s, "r", crdt.TypeRegister, crdt.Op{ID: "r1", DeviceID: "dev-1", Version: 1, Value: rawJSON(`"old"`)})
	submitCRDT(t, s, "r", crdt.TypeRegister, crdt.Op{ID: "r2", DeviceID: "dev-1", Version: 2, Value: rawJSON(`"new"`)})
	if _, err := s.CompactCRDT("r", "dev-1"); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetCRDTOpsByIDs("r", "dev-1", []string{"r1", "r2"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != crdt.LookupCompacted || got[0].Value != nil || got[0].Version != 0 {
		t.Fatalf("r1 = %+v, want compacted with no version or value", got[0])
	}
	if got[1].Status != crdt.LookupFound || got[1].Version != 2 || string(got[1].Value) != `"new"` {
		t.Fatalf("r2 = %+v, want found version 2 value new", got[1])
	}
}

// Gate and existence verdicts: an unregistered device is 404 (device), a
// revoked device 403, and a document with no committed CRDT operation 404
// (state), the same verdict the state read gives.
func TestGetOpsByIDsGateAndExistence(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	registerDevices(t, s, "dev-1", "dev-2")
	submitCRDT(t, s, "doc", crdt.TypeCounter, crdt.Op{ID: "c1", DeviceID: "dev-1", Value: rawInt(1)})

	if _, err := s.GetCRDTOpsByIDs("doc", "ghost", []string{"c1"}); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered device = %v, want ErrDeviceNotFound", err)
	}
	// A revoked device is rejected before any CRDT content is observed.
	if _, err := s.SetDocumentPermission("doc", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCRDTOpsByIDs("doc", "dev-2", []string{"c1"}); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked device = %v, want ErrPermissionDenied", err)
	}
	// A document the CRDT layer has never held an operation for is unknown to
	// it even if the device is registered and otherwise authorized.
	if _, err := s.GetCRDTOpsByIDs("never-a-crdt-doc", "dev-1", []string{"c1"}); !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("no-crdt document = %v, want crdt.ErrNotFound", err)
	}
}
