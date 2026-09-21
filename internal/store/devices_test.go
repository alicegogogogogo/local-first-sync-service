package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestRegisterDeviceIdempotent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	created, err := s.RegisterDevice("dev-1")
	if err != nil || !created {
		t.Fatalf("first register = created:%v err:%v", created, err)
	}
	created, err = s.RegisterDevice("dev-1")
	if err != nil || created {
		t.Fatalf("retry register = created:%v err:%v, want created=false", created, err)
	}

	exists, err := s.DeviceExists("dev-1")
	if err != nil || !exists {
		t.Fatalf("DeviceExists = %v, %v", exists, err)
	}
	exists, err = s.DeviceExists("ghost")
	if err != nil || exists {
		t.Fatalf("DeviceExists ghost = %v, %v", exists, err)
	}
}

func TestCreateSessionOutcomes(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown device -> ErrDeviceNotFound and zero write.
	if _, err := s.CreateSession("ghost", "sess-1"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}
	if exists, _ := s.SessionExists("sess-1"); exists {
		t.Fatal("zero-write violated: session exists after failed create")
	}

	if _, err := s.RegisterDevice("dev-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-B"); err != nil {
		t.Fatal(err)
	}

	created, err := s.CreateSession("dev-A", "sess-1")
	if err != nil || !created {
		t.Fatalf("first create = created:%v err:%v", created, err)
	}

	// Same device retries: idempotent.
	created, err = s.CreateSession("dev-A", "sess-1")
	if err != nil || created {
		t.Fatalf("retry create = created:%v err:%v, want created=false", created, err)
	}

	// Another device claims the same session id: conflict, zero write.
	_, err = s.CreateSession("dev-B", "sess-1")
	var conflict *ErrSessionConflict
	if !errors.As(err, &conflict) || conflict.ID != "sess-1" {
		t.Fatalf("cross-device create err = %v, want *ErrSessionConflict{sess-1}", err)
	}

	// Independent session ids per device are fine.
	created, err = s.CreateSession("dev-B", "sess-2")
	if err != nil || !created {
		t.Fatalf("dev-B own session = created:%v err:%v", created, err)
	}
}

func TestDeleteSessionRules(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-A"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-B"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-A", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-B", "sess-2"); err != nil {
		t.Fatal(err)
	}

	// Ownership mismatch: dev-B cannot delete dev-A's session.
	if err := s.DeleteSession("dev-B", "sess-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("wrong-owner delete err = %v, want ErrSessionNotFound", err)
	}
	if exists, _ := s.SessionExists("sess-1"); !exists {
		t.Fatal("zero-write violated: wrong-owner delete removed the session")
	}

	// Unknown session id, unknown device.
	if err := s.DeleteSession("dev-A", "nope"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown session err = %v, want ErrSessionNotFound", err)
	}
	if err := s.DeleteSession("ghost", "sess-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("unknown device err = %v, want ErrSessionNotFound", err)
	}

	// Happy path.
	if err := s.DeleteSession("dev-A", "sess-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if exists, _ := s.SessionExists("sess-1"); exists {
		t.Fatal("session still exists after delete")
	}

	// Re-delete is 404-class.
	if err := s.DeleteSession("dev-A", "sess-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("re-delete err = %v, want ErrSessionNotFound", err)
	}

	// The id is free again: another device may now claim it.
	created, err := s.CreateSession("dev-B", "sess-1")
	if err != nil || !created {
		t.Fatalf("recreate after delete = created:%v err:%v", created, err)
	}
	if exists, _ := s.SessionExists("sess-1"); !exists {
		t.Fatal("recreated session missing")
	}

	// The untouched session survives.
	if exists, _ := s.SessionExists("sess-2"); !exists {
		t.Fatal("unrelated session disappeared")
	}
}

func TestDevicesSessionsPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess-1"); err != nil {
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

	if exists, _ := s2.DeviceExists("dev-1"); !exists {
		t.Fatal("device missing after reopen")
	}
	if exists, _ := s2.SessionExists("sess-1"); !exists {
		t.Fatal("session missing after reopen")
	}

	// Idempotency and ownership decisions survive the restart.
	created, err := s2.RegisterDevice("dev-1")
	if err != nil || created {
		t.Fatalf("device retry after reopen = created:%v err:%v", created, err)
	}
	created, err = s2.CreateSession("dev-1", "sess-1")
	if err != nil || created {
		t.Fatalf("session retry after reopen = created:%v err:%v", created, err)
	}
	if _, err := s2.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.CreateSession("dev-2", "sess-1"); !errors.As(err, new(*ErrSessionConflict)) {
		t.Fatalf("conflict after reopen err = %v", err)
	}

	// Deletion persists too.
	if err := s2.DeleteSession("dev-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	if exists, _ := s3.SessionExists("sess-1"); exists {
		t.Fatal("deleted session resurrected after reopen")
	}
	if err := s3.DeleteSession("dev-1", "sess-1"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("re-delete after reopen err = %v", err)
	}
}

func TestConcurrentDeviceRegistration(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators, errs := 0, make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := s.RegisterDevice("same-device")
			if err != nil {
				errs <- err
				return
			}
			if created {
				mu.Lock()
				creators++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if creators != 1 {
		t.Fatalf("creators = %d, want 1", creators)
	}
}

func TestConcurrentSessionCreation(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	const n = 32
	for i := 0; i < 4; i++ {
		if _, err := s.RegisterDevice(fmt.Sprintf("dev-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Same device racing on one session id: exactly one creator.
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators := 0
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := s.CreateSession("dev-0", "shared-same-owner")
			if err != nil {
				errs <- err
				return
			}
			if created {
				mu.Lock()
				creators++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if creators != 1 {
		t.Fatalf("same-owner creators = %d, want 1", creators)
	}

	// Different devices racing on one new session id: one creator, every other
	// attempt is a conflict; nothing is duplicated.
	wg.Add(4)
	conflicts := 0
	createdAny := false
	var cmu sync.Mutex
	for i := 0; i < 4; i++ {
		go func(i int) {
			defer wg.Done()
			created, err := s.CreateSession(fmt.Sprintf("dev-%d", i), "contested")
			switch {
			case err == nil:
				cmu.Lock()
				createdAny = created
				cmu.Unlock()
			case errors.As(err, new(*ErrSessionConflict)):
				cmu.Lock()
				conflicts++
				cmu.Unlock()
			default:
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if !createdAny || conflicts != 3 {
		t.Fatalf("contested create: createdAny=%v conflicts=%d, want true/3", createdAny, conflicts)
	}

	// Distinct ids created concurrently all exist once.
	const m = 40
	var wg2 sync.WaitGroup
	moreErrs := make(chan error, m)
	for i := 0; i < m; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			if _, err := s.CreateSession(fmt.Sprintf("dev-%d", i%4), fmt.Sprintf("sess-%02d", i)); err != nil {
				moreErrs <- err
			}
		}(i)
	}
	wg2.Wait()
	close(moreErrs)
	for err := range moreErrs {
		t.Fatal(err)
	}
	for i := 0; i < m; i++ {
		if exists, _ := s.SessionExists(fmt.Sprintf("sess-%02d", i)); !exists {
			t.Fatalf("session sess-%02d missing", i)
		}
	}
}

func TestConcurrentCreateAndDelete(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	const rounds = 20
	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("flip-%d", i)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = s.CreateSession("dev", id)
		}()
		go func() {
			defer wg.Done()
			_ = s.DeleteSession("dev", id)
		}()
		wg.Wait()
	}

	// No invariant is broken: every existing session is retrievable, and
	// deleting then recreating leaves exactly one live binding.
	for i := 0; i < rounds; i++ {
		id := fmt.Sprintf("flip-%d", i)
		exists, err := s.SessionExists(id)
		if err != nil {
			t.Fatal(err)
		}
		// Whatever the race outcome, a final create must be idempotent if it
		// survived or create it if the delete won.
		created, err := s.CreateSession("dev", id)
		if err != nil {
			t.Fatalf("stabilize %s: %v", id, err)
		}
		if created == exists {
			t.Fatalf("round %d: exists=%v but create created=%v", i, exists, created)
		}
	}
}
