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
		t.Fatalf("repeat register = created:%v err:%v", created, err)
	}

	// A different id registers independently.
	created, err = s.RegisterDevice("dev-2")
	if err != nil || !created {
		t.Fatalf("second device = created:%v err:%v", created, err)
	}
}

func TestCreateSessionOutcomes(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown device -> ErrDeviceNotFound, nothing written.
	if _, err := s.CreateSession("ghost", "sess"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}
	if exists, err := s.SessionExists("sess"); err != nil || exists {
		t.Fatalf("session should not exist after rejected create: exists=%v err=%v", exists, err)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}

	// First create for the owner.
	created, err := s.CreateSession("dev-1", "sess")
	if err != nil || !created {
		t.Fatalf("first create = created:%v err:%v", created, err)
	}
	if exists, err := s.SessionExists("sess"); err != nil || !exists {
		t.Fatalf("session should exist: exists=%v err=%v", exists, err)
	}

	// Same owner repeat -> idempotent.
	created, err = s.CreateSession("dev-1", "sess")
	if err != nil || created {
		t.Fatalf("repeat create = created:%v err:%v", created, err)
	}

	// Another device reusing the id -> conflict, owner unchanged.
	_, err = s.CreateSession("dev-2", "sess")
	var conflict *ErrSessionConflict
	if !errors.As(err, &conflict) || conflict.ID != "sess" {
		t.Fatalf("cross-device create err = %v, want *ErrSessionConflict{sess}", err)
	}
	created, err = s.CreateSession("dev-1", "sess")
	if err != nil || created {
		t.Fatalf("owner after conflict = created:%v err:%v", created, err)
	}
}

func TestDeleteSessionOwnerScoped(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess"); err != nil {
		t.Fatal(err)
	}

	// Another device cannot delete it: not found, session stays live.
	if err := s.DeleteSession("dev-2", "sess"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("other-device delete err = %v, want ErrSessionNotFound", err)
	}
	if exists, _ := s.SessionExists("sess"); !exists {
		t.Fatal("session was deleted by a non-owner")
	}

	// Deleting a never-created session misses as well.
	if err := s.DeleteSession("dev-1", "ghost"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing delete err = %v, want ErrSessionNotFound", err)
	}

	// Owner delete succeeds.
	if err := s.DeleteSession("dev-1", "sess"); err != nil {
		t.Fatalf("owner delete: %v", err)
	}
	if exists, _ := s.SessionExists("sess"); exists {
		t.Fatal("session still exists after owner delete")
	}

	// Repeat delete, and a delete by another device, both miss.
	if err := s.DeleteSession("dev-1", "sess"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("repeat delete err = %v, want ErrSessionNotFound", err)
	}
	if err := s.DeleteSession("dev-2", "sess"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("other-device repeat delete err = %v, want ErrSessionNotFound", err)
	}

	// The id is free again: a fresh create succeeds and the original owner's
	// stale delete misses.
	created, err := s.CreateSession("dev-2", "sess")
	if err != nil || !created {
		t.Fatalf("recreate after delete = created:%v err:%v", created, err)
	}
	if err := s.DeleteSession("dev-1", "sess"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("old owner deleted recreated session: %v", err)
	}
	if exists, _ := s.SessionExists("sess"); !exists {
		t.Fatal("recreated session must survive a non-owner delete")
	}
}

func TestConcurrentCreateSameSession(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	if _, err := s.RegisterDevice("dev"); err != nil {
		t.Fatal(err)
	}

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators, conflicts := 0, 0
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			created, err := s.CreateSession("dev", "sess")
			if err != nil {
				errCh <- err
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
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if creators != 1 {
		t.Fatalf("creators = %d, want 1 (conflicts=%d)", creators, conflicts)
	}
}

func TestConcurrentCreateAcrossDevices(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	const n = 30
	for i := 0; i < n; i++ {
		if _, err := s.RegisterDevice(fmt.Sprintf("dev-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Every device races for the same session id: exactly one creates, all
	// others get a conflict and zero rows beyond the single session.
	var wg sync.WaitGroup
	var mu sync.Mutex
	creators, conflicts := 0, 0
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			created, err := s.CreateSession(fmt.Sprintf("dev-%d", i), "shared")
			switch {
			case err == nil:
				if created {
					mu.Lock()
					creators++
					mu.Unlock()
				}
			case errors.As(err, new(*ErrSessionConflict)):
				mu.Lock()
				conflicts++
				mu.Unlock()
			default:
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	if creators != 1 || conflicts != n-1 {
		t.Fatalf("creators=%d conflicts=%d, want 1 and %d", creators, conflicts, n-1)
	}
	if exists, _ := s.SessionExists("shared"); !exists {
		t.Fatal("shared session missing after the race")
	}
}

// deleteDeviceTx runs DeleteDeviceTx in its own transaction, mirroring how
// the composition root drives it.
func deleteDeviceTx(t *testing.T, s *Store, deviceID string) error {
	t.Helper()
	tx, err := s.DB().Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := DeleteDeviceTx(tx, deviceID); err != nil {
		return err
	}
	return tx.Commit()
}

func TestDeleteDeviceTxCascade(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	// Unknown device misses and writes nothing.
	if err := deleteDeviceTx(t, s, "ghost"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess"); err != nil {
		t.Fatal(err)
	}

	// dev-1 owns a completed attachment whose digest dev-2's completed
	// attachment reuses, plus an incomplete upload; dev-2 grants dev-1 read
	// access to its own attachment.
	content := []byte("shared content")
	digest := digestOf(content)
	mk := func(device, id string) {
		t.Helper()
		created, err := s.CreateAttachment(device, Attachment{
			ID: id, TotalBytes: int64(len(content)), ChunkSize: 7, SHA256: digest,
		})
		if err != nil || !created {
			t.Fatalf("create %s = created:%v err:%v", id, created, err)
		}
	}
	mk("dev-1", "att-owned")
	if _, err := s.PutChunk("dev-1", "att-owned", 0, content[:7]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-owned", 1, content[7:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev-1", "att-owned"); err != nil {
		t.Fatal(err)
	}
	mk("dev-2", "att-shared")
	if _, err := s.PutChunk("dev-2", "att-shared", 0, content[:7]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-2", "att-shared", 1, content[7:]); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteAttachment("dev-2", "att-shared")
	if err != nil || !res.Reused {
		t.Fatalf("dev-2 complete = %+v err:%v, want reused", res, err)
	}
	mk("dev-1", "att-open")
	if _, err := s.PutChunk("dev-1", "att-open", 0, content[:7]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-2", "att-shared", "dev-1", true); err != nil {
		t.Fatal(err)
	}

	if err := deleteDeviceTx(t, s, "dev-1"); err != nil {
		t.Fatalf("delete dev-1: %v", err)
	}

	// Sessions and attachments are gone; the device row is gone.
	if exists, _ := s.SessionExists("sess"); exists {
		t.Fatal("session survived device deletion")
	}
	if _, _, err := s.GetAttachment("dev-2", "att-owned"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("owned attachment err = %v, want ErrAttachmentNotFound", err)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-open"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("incomplete attachment err = %v, want ErrAttachmentNotFound", err)
	}
	if exists, _ := DeviceExistsTx(s.DB(), "dev-1"); exists {
		t.Fatal("device row survived deletion")
	}

	// The grant dev-2 extended to dev-1 lapsed: the roster is empty and the
	// re-registered device of the same name has no access.
	roster, err := s.ListAttachmentAccess("dev-2", "att-shared", 100, 0)
	if err != nil || len(roster) != 0 {
		t.Fatalf("roster = %v err:%v, want empty", roster, err)
	}

	// The shared content bytes survive: dev-2's completed attachment still
	// reads its chunks.
	if _, err := s.GetAttachmentChunk("dev-2", "att-shared", 0); err != nil {
		t.Fatalf("shared content reclaimed too early: %v", err)
	}

	// Repeat delete misses; the id registers again as a brand-new device.
	if err := deleteDeviceTx(t, s, "dev-1"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("repeat delete err = %v, want ErrDeviceNotFound", err)
	}
	created, err := s.RegisterDevice("dev-1")
	if err != nil || !created {
		t.Fatalf("re-register = created:%v err:%v", created, err)
	}
	if _, _, err := s.GetAttachment("dev-1", "att-shared"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("re-registered device access err = %v, want ErrAttachmentForbidden", err)
	}
	// The old attachment id is free again.
	created, err = s.CreateAttachment("dev-1", Attachment{
		ID: "att-owned", TotalBytes: 1, ChunkSize: 1, SHA256: digestOf([]byte("x")),
	})
	if err != nil || !created {
		t.Fatalf("attachment recreate = created:%v err:%v", created, err)
	}
}

func TestDeleteDeviceTxReclaimsUnsharedContent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	content := []byte("sole owner content")
	digest := digestOf(content)
	if _, err := s.CreateAttachment("dev-1", Attachment{
		ID: "att", TotalBytes: int64(len(content)), ChunkSize: int64(len(content)), SHA256: digest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att", 0, content); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev-1", "att"); err != nil {
		t.Fatal(err)
	}

	if err := deleteDeviceTx(t, s, "dev-1"); err != nil {
		t.Fatal(err)
	}

	// The last completed reference is gone, so the content bytes are
	// reclaimed: a fresh upload of the same digest does not reuse them.
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttachment("dev-2", Attachment{
		ID: "att", TotalBytes: int64(len(content)), ChunkSize: int64(len(content)), SHA256: digest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-2", "att", 0, content); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteAttachment("dev-2", "att")
	if err != nil {
		t.Fatal(err)
	}
	if res.Reused {
		t.Fatal("content bytes were not reclaimed with the last reference")
	}
}

func TestDeleteDeviceTxPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RegisterDevice("dev-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess"); err != nil {
		t.Fatal(err)
	}
	if err := deleteDeviceTx(t, s, "dev-1"); err != nil {
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

	// The deletion is durable: the repeat misses and nothing comes back.
	if err := deleteDeviceTx(t, s2, "dev-1"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("repeat delete after reopen err = %v, want ErrDeviceNotFound", err)
	}
	if exists, _ := s2.SessionExists("sess"); exists {
		t.Fatal("session resurrected after reopen")
	}
	if created, err := s2.RegisterDevice("dev-1"); err != nil || !created {
		t.Fatalf("re-register after reopen = created:%v err:%v", created, err)
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
	if _, err := s.RegisterDevice("dev-2"); err != nil {
		t.Fatal(err)
	}
	if created, err := s.CreateSession("dev-1", "sess"); err != nil || !created {
		t.Fatalf("create = created:%v err:%v", created, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()

	// Registration and session creation stay idempotent after restart.
	if created, err := s2.RegisterDevice("dev-1"); err != nil || created {
		t.Fatalf("device replay = created:%v err:%v", created, err)
	}
	if created, err := s2.CreateSession("dev-1", "sess"); err != nil || created {
		t.Fatalf("session replay = created:%v err:%v", created, err)
	}
	// Ownership survives: the other device still gets a 409-class conflict.
	if _, err := s2.CreateSession("dev-2", "sess"); !errors.As(err, new(*ErrSessionConflict)) {
		t.Fatalf("conflict after restart err = %v", err)
	}
	if exists, _ := s2.SessionExists("sess"); !exists {
		t.Fatal("session missing after restart")
	}

	// A deletion also persists: delete, reopen, and confirm the repeat misses.
	if err := s2.DeleteSession("dev-1", "sess"); err != nil {
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
	if exists, _ := s3.SessionExists("sess"); exists {
		t.Fatal("deleted session resurrected after restart")
	}
	if err := s3.DeleteSession("dev-1", "sess"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("repeat delete after restart err = %v", err)
	}
	if created, err := s3.CreateSession("dev-1", "sess"); err != nil || !created {
		t.Fatalf("create after delete+restart = created:%v err:%v", created, err)
	}
}
