package events_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/app"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

func TestGetChangesByIDsBasic(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-2", "b", `{"v":2}`); err != nil {
		t.Fatal(err)
	}

	// Order follows the request, not the cursor; an unknown id is missing and
	// an unknown document reads as entirely missing.
	got, err := s.GetChangesByIDs("doc", "dev-1", []string{"b", "missing", "a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d answers, want 4", len(got))
	}
	want := []events.ChangeLookup{
		{ID: "b", Status: events.LookupFound, DeviceID: "dev-2", Payload: json.RawMessage(`{"v":2}`), Cursor: 2},
		{ID: "missing", Status: events.LookupMissing},
		{ID: "a", Status: events.LookupFound, DeviceID: "dev-1", Payload: json.RawMessage(`{"v":1}`), Cursor: 1},
		{ID: "b", Status: events.LookupFound, DeviceID: "dev-2", Payload: json.RawMessage(`{"v":2}`), Cursor: 2},
	}
	for i := range want {
		payloadOK := len(got[i].Payload) == 0 && len(want[i].Payload) == 0 ||
			store.JSONEqual(got[i].Payload, want[i].Payload)
		if got[i].ID != want[i].ID ||
			got[i].Status != want[i].Status ||
			got[i].DeviceID != want[i].DeviceID ||
			got[i].Cursor != want[i].Cursor ||
			!payloadOK {
			t.Fatalf("answer %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	unknown, err := s.GetChangesByIDs("nope", "dev-1", []string{"a"})
	if err != nil {
		t.Fatal(err)
	}
	if unknown[0].Status != events.LookupMissing {
		t.Fatalf("unknown doc answer = %+v", unknown[0])
	}
}

// A compacted-away id answers from its retained summary: trimmed status and
// first cursor only, with no device or payload; ids never seen stay missing.
func TestGetChangesByIDsCompacted(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "c1", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "c2", `{"v":2}`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutSnapshot("doc", 1, json.RawMessage(`{"s":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CompactChanges("doc", "dev-1"); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetChangesByIDs("doc", "dev-1", []string{"c1", "c2", "never"})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Status != events.LookupCompacted || got[0].Cursor != 1 || got[0].DeviceID != "" || len(got[0].Payload) != 0 {
		t.Fatalf("trimmed answer = %+v, want compacted cursor 1 with no content", got[0])
	}
	if got[1].Status != events.LookupFound || got[1].Cursor != 2 || string(got[1].Payload) != `{"v":2}` {
		t.Fatalf("surviving answer = %+v", got[1])
	}
	if got[2].Status != events.LookupMissing {
		t.Fatalf("unknown answer = %+v", got[2])
	}
}

// The gate is taken before any content is observed, in the same order as the
// other gated reads: unregistered device 404, revoked permission 403.
func TestGetChangesByIDsGate(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}

	if _, err := s.GetChangesByIDs("doc", "ghost", []string{"a"}); !errors.Is(err, store.ErrDeviceNotFound) {
		t.Fatalf("unregistered = %v, want ErrDeviceNotFound", err)
	}
	if _, err := s.SetDocumentPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetChangesByIDs("doc", "dev-1", []string{"a"}); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("revoked = %v, want ErrPermissionDenied", err)
	}
}

// The lookup writes nothing: it leaves the cursor where it was, so the
// following commit takes exactly the next cursor and no answer depends on
// querying first.
func TestGetChangesByIDsMovesNoCursor(t *testing.T) {
	s, _ := app.Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if err := post(s, "doc", "dev-1", "a", `{"v":1}`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetChangesByIDs("doc", "dev-1", []string{"a", "x"}); err != nil {
		t.Fatal(err)
	}
	results, err := s.PostChanges("doc", []events.Change{
		{ID: "b", DeviceID: "dev-1", Payload: json.RawMessage(`{"v":2}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || !results[0].Created || results[0].Cursor != 2 {
		t.Fatalf("post after queries = %+v, want created cursor 2", results)
	}
}

// post commits one change as device with the given payload.
func post(s *app.App, doc, device, id, payload string) error {
	_, err := s.PostChanges(doc, []events.Change{
		{ID: id, DeviceID: device, Payload: json.RawMessage(payload)},
	})
	return err
}
