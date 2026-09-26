package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// TestAttachmentAccessGrantRevoke covers the access ledger directly: every
// (attachment, device) pair starts unauthorized, only the creator may change
// the grant, grant/revoke are idempotent, and a grant widens only the read
// side — chunk writes and the finish stay creator-only.
func TestAttachmentAccessGrantRevoke(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("hello world")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}

	// Default: a non-creator has no read access.
	if _, _, err := s.GetAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("default meta read = %v, want ErrAttachmentForbidden", err)
	}
	if _, err := s.GetAttachmentChunk("dev-2", "att-1", 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("default chunk read = %v, want ErrAttachmentForbidden", err)
	}

	// Only the creator may change access; a non-creator caller is forbidden
	// and the failed call changes nothing.
	if _, err := s.SetAttachmentAccess("dev-2", "att-1", "dev-2", true); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator grant = %v, want ErrAttachmentForbidden", err)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("read after failed grant = %v, want ErrAttachmentForbidden", err)
	}

	// Unknown attachment and unregistered target: nothing is written.
	if _, err := s.SetAttachmentAccess("dev-1", "nope", "dev-2", true); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("grant on unknown attachment = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "ghost", true); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("grant to unknown device = %v, want ErrDeviceNotFound", err)
	}

	// The first grant is the first write; repeating it is idempotent.
	changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true)
	if err != nil || !changed {
		t.Fatalf("first grant = %v %v, want changed", changed, err)
	}
	changed, err = s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true)
	if err != nil || changed {
		t.Fatalf("repeat grant = %v %v, want idempotent", changed, err)
	}

	// The granted device reads the same metadata and bytes as the creator.
	a, indices, err := s.GetAttachment("dev-2", "att-1")
	if err != nil {
		t.Fatalf("granted meta read = %v", err)
	}
	if a.ID != "att-1" || a.TotalBytes != 11 || a.ChunkSize != 4 || len(indices) != 1 || indices[0] != 0 {
		t.Fatalf("granted meta = %+v %v", a, indices)
	}
	data, err := s.GetAttachmentChunk("dev-2", "att-1", 0)
	if err != nil || string(data) != "hell" {
		t.Fatalf("granted chunk read = %q %v", data, err)
	}

	// The grant is read-only: writes and the finish stay creator-only.
	if _, err := s.PutChunk("dev-2", "att-1", 1, []byte("o wo")); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("granted device chunk write = %v, want ErrAttachmentForbidden", err)
	}
	if _, err := s.CompleteAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("granted device complete = %v, want ErrAttachmentForbidden", err)
	}

	// Revoke is idempotent too and returns the read side to 403 at once.
	changed, err = s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false)
	if err != nil || !changed {
		t.Fatalf("first revoke = %v %v, want changed", changed, err)
	}
	changed, err = s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false)
	if err != nil || changed {
		t.Fatalf("repeat revoke = %v %v, want idempotent", changed, err)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("meta read after revoke = %v, want ErrAttachmentForbidden", err)
	}
	if _, err := s.GetAttachmentChunk("dev-2", "att-1", 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("chunk read after revoke = %v, want ErrAttachmentForbidden", err)
	}
}

// TestConcurrentSetAttachmentAccess hammers the access ledger from many
// goroutines: each change commits completely inside its serialized
// transaction, so concurrent identical grants have exactly one changed
// reporter and a concurrent grant/revoke pair settles into a coherent state.
func TestConcurrentSetAttachmentAccess(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	content := []byte("hello world")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}

	// Concurrent identical grants: exactly one reports a change.
	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	changedCount := 0
	errCh := make(chan error, n+2)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true)
			if err != nil {
				errCh <- err
				return
			}
			if changed {
				mu.Lock()
				changedCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if changedCount != 1 {
		t.Fatalf("changed reporters = %d, want 1", changedCount)
	}
	if _, err := s.GetAttachmentChunk("dev-2", "att-1", 0); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("granted read of missing chunk = %v, want ErrChunkNotFound", err)
	}

	// Concurrent grant vs. revoke: each commits completely, so the settled
	// state is coherent — a repeat of the observed state is a no-op.
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg2.Done()
		if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false); err != nil {
			errCh <- err
		}
	}()
	wg2.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	_, readErr := s.GetAttachmentChunk("dev-2", "att-1", 0)
	settled := !errors.Is(readErr, ErrAttachmentForbidden)
	changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", settled)
	if err != nil || changed {
		t.Fatalf("repeat of settled state = changed:%v err:%v, want no-op", changed, err)
	}
}

// TestAttachmentAccessSurvivesReopen persists a grant, reopens the database
// and checks that the read verdict and the idempotency judgment are
// unchanged — for both the granted and the revoked state.
func TestAttachmentAccessSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sync.db")
	open := func(t *testing.T) *Store {
		t.Helper()
		s, err := Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	s := open(t)
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	content := []byte("hello world")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil || !changed {
		t.Fatalf("grant = %v %v", changed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// After a restart the grant still holds and re-granting is idempotent.
	s = open(t)
	data, err := s.GetAttachmentChunk("dev-2", "att-1", 0)
	if err != nil || string(data) != "hell" {
		t.Fatalf("granted read after reopen = %q %v", data, err)
	}
	if changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil || changed {
		t.Fatalf("re-grant after reopen = %v %v, want idempotent", changed, err)
	}
	if changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false); err != nil || !changed {
		t.Fatalf("revoke = %v %v", changed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// The revoked state survives a restart as well.
	s = open(t)
	defer func() { _ = s.Close() }()
	if _, err := s.GetAttachmentChunk("dev-2", "att-1", 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("read after reopen = %v, want ErrAttachmentForbidden", err)
	}
	if changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false); err != nil || changed {
		t.Fatalf("re-revoke after reopen = %v %v, want idempotent", changed, err)
	}
}
