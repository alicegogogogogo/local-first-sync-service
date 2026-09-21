package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestSetDocumentPermissionLifecycle(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown device -> ErrDeviceNotFound, nothing written.
	if _, err := s.SetDocumentPermission("doc", "ghost", false); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	// Devices start authorized: no row needed.
	if ok, err := s.DocumentAuthorized("doc", "dev-1"); err != nil || !ok {
		t.Fatalf("default authorized = %v err = %v", ok, err)
	}

	// Grant on the default state is a no-op.
	changed, err := s.SetDocumentPermission("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("grant on default = changed:%v err:%v", changed, err)
	}

	// First revoke flips the state.
	changed, err = s.SetDocumentPermission("doc", "dev-1", false)
	if err != nil || !changed {
		t.Fatalf("first revoke = changed:%v err:%v", changed, err)
	}
	if ok, err := s.DocumentAuthorized("doc", "dev-1"); err != nil || ok {
		t.Fatalf("after revoke authorized = %v err = %v", ok, err)
	}

	// Repeating the revoke is idempotent.
	changed, err = s.SetDocumentPermission("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("repeat revoke = changed:%v err:%v", changed, err)
	}

	// Permissions are per (document, device): neighbours are untouched.
	if ok, _ := s.DocumentAuthorized("other-doc", "dev-1"); !ok {
		t.Fatal("revoke leaked to another document")
	}
	if ok, _ := s.DocumentAuthorized("doc", "dev-2"); !ok {
		t.Fatal("revoke leaked to another device")
	}

	// Re-grant flips back; repeating the grant is idempotent again.
	changed, err = s.SetDocumentPermission("doc", "dev-1", true)
	if err != nil || !changed {
		t.Fatalf("re-grant = changed:%v err:%v", changed, err)
	}
	changed, err = s.SetDocumentPermission("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("repeat grant = changed:%v err:%v", changed, err)
	}
	if ok, _ := s.DocumentAuthorized("doc", "dev-1"); !ok {
		t.Fatal("device should be authorized after re-grant")
	}
}

func TestSetDocumentPermissionPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	changed, err := s.SetDocumentPermission("doc", "dev-1", false)
	if err != nil || !changed {
		t.Fatalf("revoke = changed:%v err:%v", changed, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// The revoked state and the idempotency of replaying it survive restart.
	if ok, err := s2.DocumentAuthorized("doc", "dev-1"); err != nil || ok {
		t.Fatalf("authorized after restart = %v err = %v", ok, err)
	}
	changed, err = s2.SetDocumentPermission("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("revoke replay after restart = changed:%v err:%v", changed, err)
	}
	changed, err = s2.SetDocumentPermission("doc", "dev-1", true)
	if err != nil || !changed {
		t.Fatalf("grant after restart = changed:%v err:%v", changed, err)
	}
}

func TestConcurrentSetDocumentPermission(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	// Concurrent identical revokes: exactly one reports a change.
	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	changedCount := 0
	errCh := make(chan error, n+2)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := s.SetDocumentPermission("doc", "dev", false)
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
	if ok, _ := s.DocumentAuthorized("doc", "dev"); ok {
		t.Fatal("device should be revoked after concurrent revokes")
	}

	// Concurrent grant vs. revoke: each commits completely, so the settled
	// state is coherent — a repeat of the observed state is a no-op.
	var wg2 sync.WaitGroup
	wg2.Add(2)
	go func() {
		defer wg2.Done()
		if _, err := s.SetDocumentPermission("doc", "dev", true); err != nil {
			errCh <- err
		}
	}()
	go func() {
		defer wg2.Done()
		if _, err := s.SetDocumentPermission("doc", "dev", false); err != nil {
			errCh <- err
		}
	}()
	wg2.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	final, err := s.DocumentAuthorized("doc", "dev")
	if err != nil {
		t.Fatal(err)
	}
	changed, err := s.SetDocumentPermission("doc", "dev", final)
	if err != nil || changed {
		t.Fatalf("repeat of settled state = changed:%v err:%v, want no-op", changed, err)
	}
}
