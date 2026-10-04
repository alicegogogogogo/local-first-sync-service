package checkpoint_test

import (
	"errors"
	"testing"

	"github.com/alicegogogogogo/local-first-sync-service/internal/authz"
	"github.com/alicegogogogogo/local-first-sync-service/internal/checkpoint"
	"github.com/alicegogogogogo/local-first-sync-service/internal/events"
	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// fixture wires the kernel, permission service, change event service and
// checkpoint service over one in-memory database in the composition order the
// app uses, registering one device with one session.
type fixture struct {
	kernel *store.Store
	authz  *authz.Service
	events *events.Service
	svc    *checkpoint.Service
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	kernel, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kernel.Close() })

	permissions, err := authz.New(kernel)
	if err != nil {
		t.Fatal(err)
	}
	eventSvc, err := events.New(kernel, permissions)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := checkpoint.New(kernel, permissions, eventSvc)
	if err != nil {
		t.Fatal(err)
	}
	permissions.AddRevokeTxHook(checkpoint.NewRevokeHook(svc))

	if _, err := kernel.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}
	if _, err := kernel.CreateSession("dev", "sess"); err != nil {
		t.Fatal(err)
	}
	return fixture{kernel, permissions, eventSvc, svc}
}

// A first confirmation advances, a repeat is idempotent, and a read echoes the
// standing boundary and high-water mark.
func TestPutGetLifecycle(t *testing.T) {
	f := newFixture(t)
	changes := []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{}`)},
		{ID: "c2", DeviceID: "dev", Payload: []byte(`{}`)},
	}
	if _, err := f.events.PostChanges("doc", changes); err != nil {
		t.Fatal(err)
	}

	got, err := f.svc.Get("sess", "doc")
	if err != nil {
		t.Fatal(err)
	}
	if got != (checkpoint.GetResult{Cursor: 0, Recorded: false, Boundary: 0, MaxCursor: 2}) {
		t.Fatalf("unconfirmed get = %+v", got)
	}

	put, err := f.svc.Put("sess", "doc", 2)
	if err != nil || put != (checkpoint.PutResult{Cursor: 2, Advanced: true}) {
		t.Fatalf("put 2 = %+v, %v", put, err)
	}
	put, err = f.svc.Put("sess", "doc", 2)
	if err != nil || put != (checkpoint.PutResult{Cursor: 2, Advanced: false}) {
		t.Fatalf("repeat put = %+v, %v", put, err)
	}
}

func TestConflictErrors(t *testing.T) {
	f := newFixture(t)
	changes := []events.Change{{ID: "c1", DeviceID: "dev", Payload: []byte(`{}`)}}
	if _, err := f.events.PostChanges("doc", changes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Put("sess", "doc", 1); err != nil {
		t.Fatal(err)
	}

	// Regression names the standing cursor.
	_, err := f.svc.Put("sess", "doc", 0)
	var regress *checkpoint.ErrRegress
	if !errors.As(err, &regress) || regress.Current != 1 {
		t.Fatalf("regress = %v, want *ErrRegress current 1", err)
	}

	// Above the high-water mark names it.
	_, err = f.svc.Put("sess", "doc", 2)
	var above *checkpoint.ErrAboveMax
	if !errors.As(err, &above) || above.Max != 1 {
		t.Fatalf("above max = %v, want *ErrAboveMax max 1", err)
	}
}

// A first confirmation below the stored compaction boundary is rejected with
// the boundary, equality allowed, on a separate never-confirmed session.
func TestFirstConfirmationBoundary(t *testing.T) {
	f := newFixture(t)
	changes := []events.Change{
		{ID: "c1", DeviceID: "dev", Payload: []byte(`{}`)},
		{ID: "c2", DeviceID: "dev", Payload: []byte(`{}`)},
	}
	if _, err := f.events.PostChanges("doc", changes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.events.PutSnapshot("doc", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.events.CompactChanges("doc", "dev"); err != nil {
		t.Fatal(err)
	}

	if _, err := f.kernel.CreateSession("dev", "late"); err != nil {
		t.Fatal(err)
	}
	_, err := f.svc.Put("late", "doc", 0)
	var below *checkpoint.ErrBelowBoundary
	if !errors.As(err, &below) || below.Boundary != 1 {
		t.Fatalf("below boundary = %v, want *ErrBelowBoundary boundary 1", err)
	}
	if _, err := f.svc.Put("late", "doc", 1); err != nil {
		t.Fatalf("equality with boundary = %v", err)
	}
}

// A missing session is a 404-shaped error before any permission or cursor
// judgment.
func TestPutUnknownSession(t *testing.T) {
	f := newFixture(t)
	if _, err := f.svc.Put("ghost", "doc", 0); !errors.Is(err, store.ErrSessionNotFound) {
		t.Fatalf("unknown session = %v, want ErrSessionNotFound", err)
	}
	if _, err := f.svc.Get("ghost", "doc"); !errors.Is(err, store.ErrSessionNotFound) {
		t.Fatalf("unknown session get = %v, want ErrSessionNotFound", err)
	}
}

// A genuine revoke clears the revoked device's session checkpoints on the
// document inside the revoke transaction; a subsequent grant leaves them
// unconfirmed.
func TestRevokeClearsCheckpoint(t *testing.T) {
	f := newFixture(t)
	changes := []events.Change{{ID: "c1", DeviceID: "dev", Payload: []byte(`{}`)}}
	if _, err := f.events.PostChanges("doc", changes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.Put("sess", "doc", 1); err != nil {
		t.Fatal(err)
	}

	if _, err := f.authz.SetDocumentPermission("doc", "dev", false); err != nil {
		t.Fatal(err)
	}
	// While revoked the checkpoint cannot even be read (403-shaped).
	if _, err := f.svc.Get("sess", "doc"); !errors.Is(err, store.ErrPermissionDenied) {
		t.Fatalf("get while revoked = %v, want ErrPermissionDenied", err)
	}
	if _, err := f.authz.SetDocumentPermission("doc", "dev", true); err != nil {
		t.Fatal(err)
	}
	got, err := f.svc.Get("sess", "doc")
	if err != nil {
		t.Fatal(err)
	}
	if got.Recorded || got.Cursor != 0 {
		t.Fatalf("checkpoint survived revoke/regrant = %+v", got)
	}
}
