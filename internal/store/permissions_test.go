package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestSetPermissionOutcomes(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown device -> ErrDeviceNotFound, nothing written.
	if _, err := s.SetPermission("doc", "ghost", false); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	// Default is authorized with no stored row.
	if ok, err := s.IsAuthorized("doc", "dev-1"); err != nil || !ok {
		t.Fatalf("default permission = %v, %v; want authorized", ok, err)
	}

	// Grant on the default is a no-op.
	changed, err := s.SetPermission("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("grant on default = changed:%v err:%v, want no change", changed, err)
	}

	// First revoke flips the state.
	changed, err = s.SetPermission("doc", "dev-1", false)
	if err != nil || !changed {
		t.Fatalf("first revoke = changed:%v err:%v, want changed", changed, err)
	}
	if ok, err := s.IsAuthorized("doc", "dev-1"); err != nil || ok {
		t.Fatalf("after revoke = %v, %v; want revoked", ok, err)
	}

	// A repeat revoke is idempotent.
	changed, err = s.SetPermission("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("repeat revoke = changed:%v err:%v, want no change", changed, err)
	}

	// Grant restores access; a repeat grant is idempotent.
	changed, err = s.SetPermission("doc", "dev-1", true)
	if err != nil || !changed {
		t.Fatalf("grant after revoke = changed:%v err:%v, want changed", changed, err)
	}
	changed, err = s.SetPermission("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("repeat grant = changed:%v err:%v, want no change", changed, err)
	}

	// Permissions are per (document, device): other pairs keep the default.
	if ok, err := s.IsAuthorized("other-doc", "dev-1"); err != nil || !ok {
		t.Fatalf("other document = %v, %v; want authorized", ok, err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.IsAuthorized("doc", "dev-2"); err != nil || !ok {
		t.Fatalf("other device = %v, %v; want authorized", ok, err)
	}
}

func TestPermissionPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perm.db")

	s, _ := Open(path)
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetPermission("doc", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	if ok, err := s2.IsAuthorized("doc", "dev-1"); err != nil || ok {
		t.Fatalf("after restart = %v, %v; want revoked", ok, err)
	}
	// Idempotency survives the restart too: the revoke is not news.
	changed, err := s2.SetPermission("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("revoke after restart = changed:%v err:%v, want no change", changed, err)
	}
}

func TestConcurrentPermissionChangesSerialize(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}

	// Concurrent identical revokes: exactly one commits a change.
	const n = 16
	changedCount := make(chan bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changed, err := s.SetPermission("doc", "dev-1", false)
			if err != nil {
				t.Errorf("SetPermission: %v", err)
				return
			}
			changedCount <- changed
		}()
	}
	wg.Wait()
	close(changedCount)

	trueCount := 0
	for c := range changedCount {
		if c {
			trueCount++
		}
	}
	if trueCount != 1 {
		t.Fatalf("changed=true count = %d, want exactly 1", trueCount)
	}
	if ok, err := s.IsAuthorized("doc", "dev-1"); err != nil || ok {
		t.Fatalf("final state = %v, %v; want revoked", ok, err)
	}
}
