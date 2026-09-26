package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestListAttachmentAccessOrderingAndPagination(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	mustRegisterDevice(t, s, "dev-3")
	mustRegisterDevice(t, s, "dev-4")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}

	// No grants yet: an empty (non-nil) roster.
	got, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil || len(got) != 0 || got == nil {
		t.Fatalf("empty roster = %v %v, want empty non-nil slice", got, err)
	}

	// Grants list in the order they took effect.
	for _, target := range []string{"dev-2", "dev-3", "dev-4"} {
		if _, err := s.SetAttachmentAccess("dev-1", "att-1", target, true); err != nil {
			t.Fatal(err)
		}
	}
	got, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !equalIDLists(got, []string{"dev-2", "dev-3", "dev-4"}) {
		t.Fatalf("roster = %v, want [dev-2 dev-3 dev-4]", got)
	}

	// A revoked device disappears at once; the others keep their positions.
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-3", false); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if !equalIDLists(got, []string{"dev-2", "dev-4"}) {
		t.Fatalf("roster after revoke = %v, want [dev-2 dev-4]", got)
	}

	// A re-grant moves the device to the position of its latest grant and
	// still appears at most once.
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-3", true); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if !equalIDLists(got, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("roster after regrant = %v, want [dev-2 dev-4 dev-3]", got)
	}

	// Repeating the current state never moves a device or duplicates it.
	if changed, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-4", true); err != nil || changed {
		t.Fatalf("idempotent grant = %v %v", changed, err)
	}
	got, _ = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if !equalIDLists(got, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("roster after idempotent grant = %v", got)
	}

	// Pages over a stable ledger neither repeat nor skip an entry.
	var seen []string
	page, err := s.ListAttachmentAccess("dev-1", "att-1", 2, 0)
	if err != nil || len(page) != 2 {
		t.Fatalf("page 0 = %v %v, want 2 entries", page, err)
	}
	seen = append(seen, page...)
	page, err = s.ListAttachmentAccess("dev-1", "att-1", 2, 2)
	if err != nil || len(page) != 1 {
		t.Fatalf("page 2 = %v %v, want 1 entry", page, err)
	}
	seen = append(seen, page...)
	if !equalIDLists(seen, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("paged walk = %v", seen)
	}
	page, err = s.ListAttachmentAccess("dev-1", "att-1", 100, 3)
	if err != nil || len(page) != 0 || page == nil {
		t.Fatalf("page past end = %v %v", page, err)
	}
}

func TestListAttachmentAccessErrorsAndDelete(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}

	// Unknown attachment: not found, independently of the caller.
	if _, err := s.ListAttachmentAccess("dev-1", "nope", 100, 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment as creator = %v, want not found", err)
	}
	if _, err := s.ListAttachmentAccess("dev-2", "nope", 100, 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment as other = %v, want not found", err)
	}

	// A non-creator — even one currently granted read access — is forbidden.
	if _, err := s.ListAttachmentAccess("dev-2", "att-1", 100, 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("granted reader roster = %v, want forbidden", err)
	}

	// The failed reads wrote nothing: the grant still reads.
	if ok, err := s.attachmentAccessGranted("att-1", "dev-2"); err != nil || !ok {
		t.Fatalf("grant after rejected roster reads = %v %v", ok, err)
	}

	// After deletion the roster misses like a never-created attachment, and a
	// roster lookup leaves nothing behind.
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("roster after delete = %v, want not found", err)
	}
}

// attachmentAccessGranted is a test-only view over the tx helper.
func (s *Store) attachmentAccessGranted(attachmentID, deviceID string) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	granted, err := attachmentAccessGrantedTx(tx, attachmentID, deviceID)
	if err != nil {
		return false, err
	}
	return granted, tx.Commit()
}

// The roster and its grant order survive a restart byte-for-byte in effect.
func TestListAttachmentAccessSurvivesRestart(t *testing.T) {
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
	mustRegisterDevice(t, s, "dev-3")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-3", true); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = open(t)
	defer func() { _ = s.Close() }()
	got, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !equalIDLists(got, []string{"dev-3", "dev-2"}) {
		t.Fatalf("roster after restart = %v, want [dev-3 dev-2]", got)
	}
}

// Databases written before the grant-order column existed are migrated on
// open: surviving grants keep their insertion order, revoked rows stay
// revoked, and grants after the migration take strictly larger positions.
func TestListAttachmentAccessMigratesLegacySchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	db, err := sql.Open("sqlite", dbPath+"?_txlock=immediate&_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`CREATE TABLE devices (id TEXT NOT NULL PRIMARY KEY)`,
		`CREATE TABLE attachments (
			id TEXT NOT NULL PRIMARY KEY,
			device_id TEXT NOT NULL,
			total_bytes INTEGER NOT NULL,
			chunk_size INTEGER NOT NULL,
			sha256 TEXT NOT NULL,
			complete INTEGER NOT NULL DEFAULT 0,
			reused INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE attachment_access (
			attachment_id TEXT NOT NULL,
			device_id TEXT NOT NULL,
			authorized INTEGER NOT NULL,
			PRIMARY KEY (attachment_id, device_id)
		)`,
		`INSERT INTO devices (id) VALUES ('dev-1'), ('dev-2'), ('dev-3'), ('dev-4')`,
		`INSERT INTO attachments (id, device_id, total_bytes, chunk_size, sha256)
			VALUES ('att-1', 'dev-1', 4, 2, '` + digestOf([]byte("data")) + `')`,
		`INSERT INTO attachment_access (attachment_id, device_id, authorized) VALUES ('att-1', 'dev-2', 1)`,
		`INSERT INTO attachment_access (attachment_id, device_id, authorized) VALUES ('att-1', 'dev-3', 0)`,
		`INSERT INTO attachment_access (attachment_id, device_id, authorized) VALUES ('att-1', 'dev-4', 1)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("legacy setup: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	got, err := s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !equalIDLists(got, []string{"dev-2", "dev-4"}) {
		t.Fatalf("migrated roster = %v, want [dev-2 dev-4]", got)
	}

	// A post-migration grant of a previously revoked device lists last, never
	// reusing the backfilled position.
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-3", true); err != nil {
		t.Fatal(err)
	}
	got, _ = s.ListAttachmentAccess("dev-1", "att-1", 100, 0)
	if !equalIDLists(got, []string{"dev-2", "dev-4", "dev-3"}) {
		t.Fatalf("roster after post-migration grant = %v, want [dev-2 dev-4 dev-3]", got)
	}
}
