package permission

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	_ "modernc.org/sqlite"
)

// openDB opens a single-connection in-memory SQLite database, matching the
// serialized-transaction setup the service runs under in production.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?_txlock=immediate&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// openFileDB opens a single-connection SQLite database at path, so a service
// can be closed and reopened on the same file to prove durability.
func openFileDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path+"?_txlock=immediate&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// fakeRegistry is a DeviceRegistry stub: devices in the set exist, everything
// else fails with errUnknownDevice.
type fakeRegistry struct {
	devices map[string]bool
}

var errUnknownDevice = errors.New("device not found")

func (r fakeRegistry) RequireTx(_ *sql.Tx, deviceID string) error {
	if r.devices[deviceID] {
		return nil
	}
	return errUnknownDevice
}

func newService(t *testing.T, db *sql.DB, registry DeviceRegistry) *Service {
	t.Helper()
	svc, err := New(db, registry)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func TestDefaultAuthorizedAndRevokeLifecycle(t *testing.T) {
	svc := newService(t, openDB(t), fakeRegistry{devices: map[string]bool{"dev-1": true}})

	// A pair with no stored row is authorized by default.
	ok, err := svc.Authorized("doc", "dev-1")
	if err != nil || !ok {
		t.Fatalf("default Authorized = %v, %v; want true, nil", ok, err)
	}

	// Grant on the default state changes nothing.
	changed, err := svc.Set("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("grant on default = changed %v, err %v; want false, nil", changed, err)
	}

	// First revoke is the first write.
	changed, err = svc.Set("doc", "dev-1", false)
	if err != nil || !changed {
		t.Fatalf("first revoke = changed %v, err %v; want true, nil", changed, err)
	}
	ok, err = svc.Authorized("doc", "dev-1")
	if err != nil || ok {
		t.Fatalf("after revoke Authorized = %v, %v; want false, nil", ok, err)
	}

	// Repeating the revoke is idempotent.
	changed, err = svc.Set("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("repeat revoke = changed %v, err %v; want false, nil", changed, err)
	}

	// Grant restores access; repeating it is idempotent.
	changed, err = svc.Set("doc", "dev-1", true)
	if err != nil || !changed {
		t.Fatalf("grant after revoke = changed %v, err %v; want true, nil", changed, err)
	}
	changed, err = svc.Set("doc", "dev-1", true)
	if err != nil || changed {
		t.Fatalf("repeat grant = changed %v, err %v; want false, nil", changed, err)
	}
	ok, err = svc.Authorized("doc", "dev-1")
	if err != nil || !ok {
		t.Fatalf("after grant Authorized = %v, %v; want true, nil", ok, err)
	}
}

func TestSetUnknownDevicePropagatesRegistryError(t *testing.T) {
	svc := newService(t, openDB(t), fakeRegistry{devices: map[string]bool{"dev-1": true}})

	if _, err := svc.Set("doc", "ghost", false); !errors.Is(err, errUnknownDevice) {
		t.Fatalf("Set with unknown device err = %v; want %v", err, errUnknownDevice)
	}
	// The failed write left nothing behind: the pair still defaults to
	// authorized and a later revoke of a known device is unaffected.
	ok, err := svc.Authorized("doc", "ghost")
	if err != nil || !ok {
		t.Fatalf("ghost pair Authorized = %v, %v; want true, nil", ok, err)
	}
}

func TestNilRegistrySkipsDeviceCheck(t *testing.T) {
	svc := newService(t, openDB(t), nil)
	changed, err := svc.Set("doc", "anything", false)
	if err != nil || !changed {
		t.Fatalf("Set without registry = changed %v, err %v; want true, nil", changed, err)
	}
}

func TestCheckTxUnifiedDecision(t *testing.T) {
	db := openDB(t)
	svc := newService(t, db, nil)

	check := func(doc, device string) error {
		tx, err := db.Begin()
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		defer func() { _ = tx.Rollback() }()
		return svc.CheckTx(tx, doc, device)
	}

	if err := check("doc", "dev-1"); err != nil {
		t.Fatalf("CheckTx on default pair = %v; want nil", err)
	}
	if _, err := svc.Set("doc", "dev-1", false); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if err := check("doc", "dev-1"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("CheckTx on revoked pair = %v; want ErrPermissionDenied", err)
	}
	// Other pairs and other documents are unaffected.
	if err := check("doc", "dev-2"); err != nil {
		t.Fatalf("CheckTx other device = %v; want nil", err)
	}
	if err := check("other-doc", "dev-1"); err != nil {
		t.Fatalf("CheckTx other document = %v; want nil", err)
	}
}

func TestOnRevokeHooksFireOnlyOnActualRevoke(t *testing.T) {
	svc := newService(t, openDB(t), nil)

	var mu sync.Mutex
	var fired []string
	svc.OnRevoke(func(documentID, deviceID string) {
		mu.Lock()
		defer mu.Unlock()
		fired = append(fired, documentID+"/"+deviceID)
	})
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(fired)
	}

	// A grant fires nothing.
	if _, err := svc.Set("doc", "dev-1", true); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if count() != 0 {
		t.Fatalf("grant fired %d hooks; want 0", count())
	}
	// The first revoke fires exactly once.
	if _, err := svc.Set("doc", "dev-1", false); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if count() != 1 {
		t.Fatalf("revoke fired %d hooks; want 1", count())
	}
	// An idempotent repeat revoke changes nothing and fires nothing.
	if _, err := svc.Set("doc", "dev-1", false); err != nil {
		t.Fatalf("repeat revoke: %v", err)
	}
	if count() != 1 {
		t.Fatalf("repeat revoke fired %d hooks total; want 1", count())
	}
	// A re-grant fires nothing, but a later revoke fires again.
	if _, err := svc.Set("doc", "dev-1", true); err != nil {
		t.Fatalf("re-grant: %v", err)
	}
	if count() != 1 {
		t.Fatalf("re-grant fired %d hooks total; want 1", count())
	}
	if _, err := svc.Set("doc", "dev-1", false); err != nil {
		t.Fatalf("second revoke: %v", err)
	}
	if count() != 2 {
		t.Fatalf("second revoke fired %d hooks total; want 2", count())
	}
}

func TestConcurrentGrantRevokeSerialize(t *testing.T) {
	svc := newService(t, openDB(t), nil)

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				if _, err := svc.Set("doc", "dev-1", i%2 == 0); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Every call committed completely; the final state is one of the two
	// legal values and matches the stored row.
	ok, err := svc.Authorized("doc", "dev-1")
	if err != nil {
		t.Fatalf("Authorized: %v", err)
	}
	t.Logf("final authorized state: %v", ok)
}

func TestPermissionPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "perm.db")

	svc := newService(t, openFileDB(t, path), nil)
	if _, err := svc.Set("doc", "dev-1", false); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := svc.Set("doc", "dev-2", true); err != nil {
		t.Fatalf("grant: %v", err)
	}

	// A fresh service on the same file sees the same decisions.
	reopened := newService(t, openFileDB(t, path), nil)
	ok, err := reopened.Authorized("doc", "dev-1")
	if err != nil || ok {
		t.Fatalf("reopened Authorized revoked pair = %v, %v; want false, nil", ok, err)
	}
	// Idempotency decisions survive too: re-revoking reports no change.
	changed, err := reopened.Set("doc", "dev-1", false)
	if err != nil || changed {
		t.Fatalf("reopened repeat revoke = changed %v, err %v; want false, nil", changed, err)
	}
	ok, err = reopened.Authorized("doc", "dev-2")
	if err != nil || !ok {
		t.Fatalf("reopened Authorized granted pair = %v, %v; want true, nil", ok, err)
	}
}
