package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// The session merge's gated service entry shares MergeChange's judgment but
// takes the registration/permission verdict first, inside the same serialized
// transaction.
func TestMergeAuthorizedGateAndOutcomes(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("other"); err != nil {
		t.Fatal(err)
	}

	// First applied merge on an unknown document from a registered device.
	r, err := s.MergeSessionChange("doc", 0, events.Change{
		ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{"a":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "applied" || r.Cursor != 1 {
		t.Fatalf("applied = %+v", r)
	}

	// An unregistered device is rejected before any change content is read.
	_, err = s.MergeSessionChange("doc", 1, events.Change{
		ID: "ghost", DeviceID: "stranger", Payload: json.RawMessage(`{"z":1}`),
	})
	if !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered err = %v, want ErrDeviceNotFound", err)
	}

	// A revoked device is rejected with the shared permission verdict.
	if _, err := s.SetDocumentPermission("doc", "other", false); err != nil {
		t.Fatal(err)
	}
	_, err = s.MergeSessionChange("doc", 1, events.Change{
		ID: "c2", DeviceID: "other", Payload: json.RawMessage(`{"b":2}`),
	})
	if !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked err = %v, want ErrPermissionDenied", err)
	}

	// Advance the log to cursor 2 with an ordinary commit, then merge from a
	// stale base (1) with a key disjoint from the later payload.
	if _, err := s.PostChanges("doc", []events.Change{
		{ID: "p2", DeviceID: "dev", Payload: json.RawMessage(`{"n":2}`)},
	}); err != nil {
		t.Fatal(err)
	}
	r, err = s.MergeSessionChange("doc", 1, events.Change{
		ID: "c3", DeviceID: "dev", Payload: json.RawMessage(`{"c":3}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "merged" || r.Cursor != 3 {
		t.Fatalf("merged = %+v", r)
	}

	// An idempotent repeat reports the first cursor without writing again.
	r, err = s.MergeSessionChange("doc", 3, events.Change{
		ID: "c1", DeviceID: "dev", Payload: json.RawMessage(`{ "a": 1.0 }`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Outcome != "idempotent" || r.Cursor != 1 || r.Result == nil ||
		r.Result.Created || r.Result.Cursor != 1 {
		t.Fatalf("idempotent = %+v", r)
	}

	rows, next, err := s.ListChanges("doc", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || next != 3 {
		t.Fatalf("rejected/idempotent merges leaked writes: rows=%d next=%d", len(rows), next)
	}
}
