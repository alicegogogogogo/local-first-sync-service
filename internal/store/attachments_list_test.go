package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func listedIDs(items []ListedAttachment) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.ID)
	}
	return ids
}

func equalIDLists(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestListAttachments(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	// Unregistered device: ErrDeviceNotFound, no listing.
	if _, err := s.ListAttachments("ghost", 100, 0); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("list on unknown device = %v, want ErrDeviceNotFound", err)
	}

	// dev-1 owns att-1 and att-3; dev-2 owns att-2 and grants dev-1 access.
	for _, tc := range []struct {
		device, id string
	}{
		{"dev-1", "att-1"},
		{"dev-2", "att-2"},
		{"dev-1", "att-3"},
	} {
		if _, err := s.CreateAttachment(tc.device, toAttachment(tc.id, 4, 2, []byte("data"))); err != nil {
			t.Fatalf("create %s: %v", tc.id, err)
		}
	}
	if _, err := s.PutChunk("dev-1", "att-1", 1, []byte("ta")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("da")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetAttachmentAccess("dev-2", "att-2", "dev-1", true); err != nil {
		t.Fatal(err)
	}
	// A grant to the creator itself must not duplicate the entry.
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-1", true); err != nil {
		t.Fatal(err)
	}

	items, err := s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-1", "att-2", "att-3"}) {
		t.Fatalf("list = %v, want [att-1 att-2 att-3]", got)
	}
	if !items[0].Owned || items[1].Owned || !items[2].Owned {
		t.Fatalf("owned flags = %v %v %v", items[0].Owned, items[1].Owned, items[2].Owned)
	}
	if got := items[0].ReceivedChunks; len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("received chunks = %v, want [0 1]", got)
	}
	if len(items[1].ReceivedChunks) != 0 || items[1].ReceivedChunks == nil {
		t.Fatalf("received chunks = %v, want empty non-nil", items[1].ReceivedChunks)
	}

	// Pagination walks the same order without repeats or gaps.
	var seen []string
	for offset := int64(0); offset < 3; offset += 2 {
		page, err := s.ListAttachments("dev-1", 2, offset)
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, listedIDs(page)...)
	}
	if !equalIDLists(seen, []string{"att-1", "att-2", "att-3"}) {
		t.Fatalf("paged walk = %v", seen)
	}
	page, err := s.ListAttachments("dev-1", 100, 3)
	if err != nil || len(page) != 0 {
		t.Fatalf("page past end = %v %v", page, err)
	}

	// A revoke removes the entry from the granted device's listing only.
	if _, err := s.SetAttachmentAccess("dev-2", "att-2", "dev-1", false); err != nil {
		t.Fatal(err)
	}
	items, err = s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-1", "att-3"}) {
		t.Fatalf("list after revoke = %v", got)
	}
	items, err = s.ListAttachments("dev-2", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-2"}) {
		t.Fatalf("creator list after revoke = %v", got)
	}

	// A delete removes the entry everywhere; re-creating the id lists a new
	// record at the end of the creation order.
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	items, err = s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-3", "att-1"}) {
		t.Fatalf("list after recreate = %v, want [att-3 att-1]", got)
	}
	if len(items[1].ReceivedChunks) != 0 {
		t.Fatalf("recreated progress = %v, want none", items[1].ReceivedChunks)
	}
}

// Databases written before the creation-order column existed are migrated on
// open: existing rows keep their insertion order and rows created afterwards
// take strictly larger sequence values.
func TestListAttachmentsMigratesLegacySchema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")

	// Write a database with the pre-migration attachments schema.
	db, err := sql.Open("sqlite", dbPath+"?_txlock=immediate&_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	stmts := []string{
		`CREATE TABLE devices (id TEXT NOT NULL PRIMARY KEY)`,
		`CREATE TABLE attachments (
			id          TEXT NOT NULL PRIMARY KEY,
			device_id   TEXT NOT NULL REFERENCES devices(id),
			total_bytes INTEGER NOT NULL,
			chunk_size  INTEGER NOT NULL,
			sha256      TEXT NOT NULL,
			complete    INTEGER NOT NULL DEFAULT 0,
			reused      INTEGER NOT NULL DEFAULT 0
		)`,
		`INSERT INTO devices (id) VALUES ('dev-1')`,
		// Insert out of id order so the backfill must follow insertion order.
		`INSERT INTO attachments (id, device_id, total_bytes, chunk_size, sha256)
			VALUES ('att-z', 'dev-1', 4, 2, '` + digestOf([]byte("data")) + `')`,
		`INSERT INTO attachments (id, device_id, total_bytes, chunk_size, sha256)
			VALUES ('att-a', 'dev-1', 4, 2, '` + digestOf([]byte("data")) + `')`,
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

	items, err := s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-z", "att-a"}) {
		t.Fatalf("migrated list = %v, want insertion order [att-z att-a]", got)
	}

	// New rows take strictly larger sequence values: they list after every
	// migrated row, and deleting the newest row never frees a position.
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-m", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAttachment("dev-1", "att-m"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-n", 4, 2, []byte("data"))); err != nil {
		t.Fatal(err)
	}
	items, err = s.ListAttachments("dev-1", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := listedIDs(items); !equalIDLists(got, []string{"att-z", "att-a", "att-n"}) {
		t.Fatalf("list after recreate = %v, want [att-z att-a att-n]", got)
	}
}
