package crdt_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/crdt"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

type allowGate struct{}

func (allowGate) DeviceAuthorizedTx(_ *sql.Tx, _, _ string) error { return nil }

type denyGate struct{ err error }

func (g denyGate) DeviceAuthorizedTx(_ *sql.Tx, _, _ string) error { return g.err }

// openKernelCRDT constructs the CRDT service directly over a kernel handle,
// without the composition root, and registers the one device its ops use.
func openKernelCRDT(t *testing.T, gate crdt.Gate) (*store.Store, *crdt.Service) {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })
	svc, err := crdt.New(kernel, gate)
	if err != nil {
		t.Fatal(err)
	}
	if gate != nil {
		if _, err := kernel.RegisterDevice("dev"); err != nil {
			t.Fatal(err)
		}
	}
	return kernel, svc
}

func rawJSON(s string) json.RawMessage { return json.RawMessage(s) }

// Normal path: the first batch fixes the type; the counter merge is the sum of
// per-device maxima.
func TestServiceStandaloneCounterMerge(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})

	if _, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("5")},
	}); err != nil {
		t.Fatal(err)
	}
	// An equal contribution is an accepted no-op that does not change results.
	results, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a2", DeviceID: "dev", Value: rawJSON("5")},
	})
	if err != nil || !results[0].Created {
		t.Fatalf("equal contribution results=%v err=%v", results, err)
	}
	state, err := svc.GetState("doc")
	if err != nil || state.Type != crdt.TypeCounter || string(state.Value) != "5" {
		t.Fatalf("state = %+v err=%v", state, err)
	}

	// Boundary: an unknown document has no state.
	if _, err := svc.GetState("unknown"); !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("unknown state err = %v, want ErrNotFound", err)
	}
}

// Type fixation: after the first batch a mismatched declared type is a 409
// conflict that writes nothing.
func TestServiceStandaloneTypeFixing(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})
	if _, err := svc.SubmitOps("doc", crdt.TypeGSet, []crdt.Op{
		{ID: "g1", DeviceID: "dev", Elements: []string{"apple"}},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("1")},
	})
	var conflict *crdt.ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "" {
		t.Fatalf("type mismatch err = %v, want document-level ErrConflict", err)
	}
}

// Failure branches at the gate: 404 for an unregistered device and 403 for a
// revoked one, before any CRDT content is observed.
func TestServiceStandaloneGateDenials(t *testing.T) {
	kernel, _ := openKernelCRDT(t, nil)
	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	deny404, _ := crdt.New(kernel, denyGate{err: store.ErrDeviceNotFound})
	_, err := deny404.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "ghost", Value: rawJSON("1")},
	})
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered err = %v, want ErrDeviceNotFound", err)
	}
	if _, err := deny404.GetState("doc"); !errors.Is(err, crdt.ErrNotFound) {
		t.Fatalf("document should have no state after 404: %v", err)
	}

	deny403, _ := crdt.New(kernel, denyGate{err: store.ErrPermissionDenied})
	_, err = deny403.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("1")},
	})
	if !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked err = %v, want ErrPermissionDenied", err)
	}
	if _, err := deny403.Compact("doc", "dev"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("compact revoked err = %v, want ErrPermissionDenied", err)
	}
}

// Idempotency versus conflict: the same id with the same device and content is
// created=false; a different value is an operation-level 409.
func TestServiceStandaloneIdempotencyAndConflict(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})
	op := crdt.Op{ID: "a1", DeviceID: "dev", Value: rawJSON("3")}
	if _, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{op}); err != nil {
		t.Fatal(err)
	}
	results, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{op})
	if err != nil || results[0].Created {
		t.Fatalf("idempotent results=%v err=%v", results, err)
	}
	_, err = svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("4")},
	})
	var conflict *crdt.ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "a1" {
		t.Fatalf("conflicting replay err = %v, want ErrConflict{a1}", err)
	}
}

// A regressing counter contribution is rejected and the merge is unchanged.
func TestServiceStandaloneCounterRegression(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})
	if _, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("5")},
	}); err != nil {
		t.Fatal(err)
	}
	_, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a2", DeviceID: "dev", Value: rawJSON("4")},
	})
	var conflict *crdt.ErrConflict
	if !errors.As(err, &conflict) || conflict.ID != "a2" {
		t.Fatalf("regression err = %v, want ErrConflict{a2}", err)
	}
	state, _ := svc.GetState("doc")
	if string(state.Value) != "5" {
		t.Fatalf("merge after regression = %s, want 5", state.Value)
	}
}

// Compaction trims storage but preserves the merge, the counts and
// idempotency for a trimmed id.
func TestServiceStandaloneCompact(t *testing.T) {
	_, svc := openKernelCRDT(t, allowGate{})
	if _, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("1")},
		{ID: "a2", DeviceID: "dev", Value: rawJSON("2")},
	}); err != nil {
		t.Fatal(err)
	}
	snap, err := svc.Compact("doc", "dev")
	if err != nil {
		t.Fatal(err)
	}
	if string(snap.Value) != "2" || snap.Operations != 1 || snap.Tombstones != 0 {
		t.Fatalf("snapshot = %+v, want value 2 / 1 op / 0 tombstones", snap)
	}
	// The trimmed id replays idempotently even though its merge row is gone.
	results, err := svc.SubmitOps("doc", crdt.TypeCounter, []crdt.Op{
		{ID: "a1", DeviceID: "dev", Value: rawJSON("1")},
	})
	if err != nil || results[0].Created {
		t.Fatalf("trimmed id replay results=%v err=%v", results, err)
	}
}
