package store

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

// deviceExists reports whether the device row is present.
func deviceExists(t *testing.T, s *Store, deviceID string) bool {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	exists, err := DeviceExistsTx(tx, deviceID)
	if err != nil {
		t.Fatal(err)
	}
	return exists
}

// deleteDevice runs DeleteDeviceTx in its own committed transaction, the way a
// store-level caller would.
func deleteDevice(t *testing.T, s *Store, deviceID string) error {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := DeleteDeviceTx(tx, deviceID); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func TestDeleteDeviceUnknownAndRepeatMiss(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// Never registered: 404-shaped miss, zero writes.
	if err := deleteDevice(t, s, "ghost"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device = %v, want ErrDeviceNotFound", err)
	}

	mustRegisterDevice(t, s, "dev-1")
	if err := deleteDevice(t, s, "dev-1"); err != nil {
		t.Fatalf("deregister = %v", err)
	}
	// A repeat deregistration misses exactly like a never-registered id.
	if err := deleteDevice(t, s, "dev-1"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("repeat deregistration = %v, want ErrDeviceNotFound", err)
	}

	// The id is free again and registers as a brand-new device.
	created, err := s.RegisterDevice("dev-1")
	if err != nil || !created {
		t.Fatalf("re-register after deregistration = created:%v err:%v", created, err)
	}
}

func TestDeleteDeviceCascadesSessionsAndAttachments(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	if _, err := s.CreateSession("dev-1", "sess-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-1", "sess-2"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateSession("dev-2", "sess-other"); err != nil {
		t.Fatal(err)
	}

	// dev-1 owns a completed attachment and an unfinished one.
	done := []byte("finished content")
	if _, err := s.CreateAttachment("dev-1", toAttachment("done", int64(len(done)), 8, done)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "done", 0, done[:8]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "done", 1, done[8:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev-1", "done"); err != nil {
		t.Fatal(err)
	}
	half := []byte("half uploaded")
	if _, err := s.CreateAttachment("dev-1", toAttachment("half", 32, 8, half)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "half", 0, half[:8]); err != nil {
		t.Fatal(err)
	}

	// dev-2 owns an attachment dev-1 was granted read access to, and dev-1
	// granted dev-2 read access to its completed attachment.
	if _, err := s.CreateAttachment("dev-2", toAttachment("foreign", 4, 4, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.SetAttachmentAccess("dev-2", "foreign", "dev-1", true); err != nil || !changed {
		t.Fatalf("grant dev-1 on foreign = changed:%v err:%v", changed, err)
	}
	if changed, err := s.SetAttachmentAccess("dev-1", "done", "dev-2", true); err != nil || !changed {
		t.Fatalf("grant dev-2 on done = changed:%v err:%v", changed, err)
	}

	if err := deleteDevice(t, s, "dev-1"); err != nil {
		t.Fatalf("deregister = %v", err)
	}

	// Device row and both sessions are hard-deleted; dev-2's session survives.
	if exists := deviceExists(t, s, "dev-1"); exists {
		t.Fatal("device row survived deregistration")
	}
	if exists, _ := s.SessionExists("sess-1"); exists {
		t.Fatal("sess-1 survived deregistration")
	}
	if exists, _ := s.SessionExists("sess-2"); exists {
		t.Fatal("sess-2 survived deregistration")
	}
	if exists, _ := s.SessionExists("sess-other"); !exists {
		t.Fatal("other device's session was removed")
	}

	// Both owned attachments are gone with every trace; reads miss 404.
	for _, id := range []string{"done", "half"} {
		if _, _, err := s.GetAttachment("dev-2", id); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("attachment %q after deregistration = %v, want not found", id, err)
		}
	}
	// dev-1's grant on dev-2's attachment vanished with the device: a
	// re-registered device of the same id does not inherit read access.
	mustRegisterDevice(t, s, "dev-1")
	if _, _, err := s.GetAttachment("dev-1", "foreign"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("re-registered device inherited a read grant: %v", err)
	}
	// ...and dev-2's roster on "foreign" no longer names dev-1.
	roster, err := s.ListAttachmentAccess("dev-2", "foreign", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range roster {
		if d == "dev-1" {
			t.Fatal("deregistered device still on the access roster")
		}
	}

	// The new dev-1 inherits no sessions or attachments.
	if exists, _ := s.SessionExists("sess-1"); exists {
		t.Fatal("re-registered device inherited a session")
	}
	items, err := s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("re-registered device inherited attachments: %+v", items)
	}
}

func TestDeleteDeviceSharedContentReclamation(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("same bytes here")
	complete := func(device, id string) {
		t.Helper()
		if _, err := s.CreateAttachment(device, toAttachment(id, int64(len(content)), 8, content)); err != nil {
			t.Fatal(err)
		}
		for i, off := 0, 0; off < len(content); i, off = i+1, off+8 {
			end := off + 8
			if end > len(content) {
				end = len(content)
			}
			if _, err := s.PutChunk(device, id, int64(i), content[off:end]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.CompleteAttachment(device, id); err != nil {
			t.Fatal(err)
		}
	}
	complete("dev-1", "a1")
	complete("dev-2", "a2") // reused bytes

	// Deregistering dev-1 alone must not reclaim the shared bytes: dev-2's
	// attachment still reads them.
	if err := deleteDevice(t, s, "dev-1"); err != nil {
		t.Fatal(err)
	}
	if data, err := s.GetAttachmentChunk("dev-2", "a2", 0); err != nil || string(data) != string(content[:8]) {
		t.Fatalf("shared content after deregistration = %q %v", data, err)
	}

	// The bytes are reclaimed only once the last reference disappears.
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM attachment_contents`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("content rows = %d, want the shared row retained", n)
	}
	if err := s.DeleteAttachment("dev-2", "a2"); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM attachment_contents`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("content rows after last reference = %d, want 0", n)
	}
}

func TestDeleteDeviceConcurrentAtMostOnce(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "hot")

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded, missed := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := deleteDevice(t, s, "hot")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				succeeded++
			case errors.Is(err, ErrDeviceNotFound):
				missed++
			default:
				t.Errorf("concurrent deregistration err = %v", err)
			}
		}()
	}
	wg.Wait()
	if succeeded != 1 || missed != n-1 {
		t.Fatalf("succeeded = %d, missed = %d, want 1 and %d", succeeded, missed, n-1)
	}
	if exists := deviceExists(t, s, "hot"); exists {
		t.Fatal("device exists after concurrent deregistration")
	}
}

func TestDeleteDevicePersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deregister.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")
	if _, err := s.CreateSession("dev-1", "sess"); err != nil {
		t.Fatal(err)
	}
	if err := deleteDevice(t, s, "dev-1"); err != nil {
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

	if exists := deviceExists(t, s2, "dev-1"); exists {
		t.Fatal("device reappeared after restart")
	}
	if exists, _ := s2.SessionExists("sess"); exists {
		t.Fatal("session reappeared after restart")
	}
	// Repeat deregistration after restart still misses with zero writes.
	tx, err := s2.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := DeleteDeviceTx(tx, "dev-1"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("repeat after restart = %v, want ErrDeviceNotFound", err)
	}
	_ = tx.Rollback()

	created, err := s2.RegisterDevice("dev-1")
	if err != nil || !created {
		t.Fatalf("re-register after restart = created:%v err:%v", created, err)
	}
}
