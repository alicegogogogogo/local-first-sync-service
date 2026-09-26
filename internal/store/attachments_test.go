package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
)

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func mustRegisterDevice(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, err := s.RegisterDevice(id); err != nil {
		t.Fatalf("register %q: %v", id, err)
	}
}

func TestAttachmentLifecycle(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("hello world")
	meta := toAttachment("att-1", 11, 4, content)

	// Unknown device cannot create.
	if _, err := s.CreateAttachment("ghost", meta); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("create on unknown device = %v, want ErrDeviceNotFound", err)
	}

	created, err := s.CreateAttachment("dev-1", meta)
	if err != nil || !created {
		t.Fatalf("create = %v %v, want created", created, err)
	}
	// Identical retry is idempotent.
	created, err = s.CreateAttachment("dev-1", meta)
	if err != nil || created {
		t.Fatalf("retry = %v %v, want idempotent", created, err)
	}
	// Other device, identical metadata: conflict, original unchanged.
	if _, err := s.CreateAttachment("dev-2", meta); err == nil {
		t.Fatal("cross-device create succeeded, want conflict")
	} else {
		var conflict *ErrAttachmentConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("cross-device create = %v, want ErrAttachmentConflict", err)
		}
	}
	// Same device, different metadata: conflict.
	other := meta
	other.ChunkSize = 2
	if _, err := s.CreateAttachment("dev-1", other); err == nil {
		t.Fatal("different-metadata create succeeded, want conflict")
	}

	// Chunks arrive out of order; identical resubmission is idempotent.
	if created, err := s.PutChunk("dev-1", "att-1", 1, []byte("o wo")); err != nil || !created {
		t.Fatalf("put chunk 1 = %v %v", created, err)
	}
	if created, err := s.PutChunk("dev-1", "att-1", 1, []byte("o wo")); err != nil || created {
		t.Fatalf("re-put chunk 1 = %v %v, want idempotent", created, err)
	}
	// Different bytes at the same index conflict and keep the first content.
	if _, err := s.PutChunk("dev-1", "att-1", 1, []byte("O WO")); err == nil {
		t.Fatal("conflicting chunk accepted")
	} else {
		var conflict *ErrChunkConflict
		if !errors.As(err, &conflict) {
			t.Fatalf("chunk conflict = %v, want ErrChunkConflict", err)
		}
	}
	// Shape violations write nothing.
	if _, err := s.PutChunk("dev-1", "att-1", 3, []byte("xxxx")); err == nil {
		t.Fatal("out-of-range index accepted")
	} else {
		var invalid *ErrChunkInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("out-of-range = %v, want ErrChunkInvalid", err)
		}
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell!")); err == nil {
		t.Fatal("wrong-length non-final chunk accepted")
	}
	if _, err := s.PutChunk("dev-1", "att-1", 2, []byte("rld!")); err == nil {
		t.Fatal("oversized final chunk accepted")
	}
	// Ownership is enforced on chunks.
	if _, err := s.PutChunk("dev-2", "att-1", 0, []byte("hell")); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator chunk = %v, want ErrAttachmentForbidden", err)
	}
	if _, err := s.PutChunk("dev-1", "nope", 0, []byte("hell")); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment chunk = %v, want ErrAttachmentNotFound", err)
	}

	// Finishing before all chunks arrive is incomplete and stays resumable.
	if _, err := s.CompleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentIncomplete) {
		t.Fatalf("incomplete finish = %v, want ErrAttachmentIncomplete", err)
	}
	if _, err := s.CompleteAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator finish = %v, want ErrAttachmentForbidden", err)
	}

	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 2, []byte("rld")); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteAttachment("dev-1", "att-1")
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if res.ID != "att-1" || res.Size != 11 || res.SHA256 != digestOf(content) || !res.Complete || res.Reused {
		t.Fatalf("complete result = %+v", res)
	}
	// A repeat finish returns the same recorded result.
	res2, err := s.CompleteAttachment("dev-1", "att-1")
	if err != nil || res2 != res {
		t.Fatalf("repeat complete = %+v %v, want %+v", res2, err, res)
	}

	// Metadata readback: indices and completion status.
	a, indices, err := s.GetAttachment("dev-1", "att-1")
	if err != nil {
		t.Fatal(err)
	}
	if !a.Complete || len(indices) != 3 || indices[0] != 0 || indices[2] != 2 {
		t.Fatalf("get attachment = %+v %v", a, indices)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator read = %v, want ErrAttachmentForbidden", err)
	}

	// Chunk readback and misses.
	data, err := s.GetAttachmentChunk("dev-1", "att-1", 2)
	if err != nil || string(data) != "rld" {
		t.Fatalf("get chunk = %q %v", data, err)
	}
	// An out-of-range index is an invalid request (HTTP 400), not a missing
	// chunk; an in-range index with no chunk is a not-found.
	var invalid *ErrChunkInvalid
	if _, err := s.GetAttachmentChunk("dev-1", "att-1", 5); !errors.As(err, &invalid) {
		t.Fatalf("out-of-range chunk = %v, want *ErrChunkInvalid", err)
	}
}

func TestAttachmentDigestMismatchAndReuse(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	// Declared digest does not match the uploaded content: 422 at the store
	// layer, and the upload stays incomplete.
	bad := toAttachment("bad", 3, 3, []byte("xyz"))
	if _, err := s.CreateAttachment("dev-1", bad); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "bad", 0, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev-1", "bad"); !errors.Is(err, ErrAttachmentDigestMismatch) {
		t.Fatalf("digest mismatch = %v, want ErrAttachmentDigestMismatch", err)
	}
	a, _, err := s.GetAttachment("dev-1", "bad")
	if err != nil || a.Complete {
		t.Fatalf("mismatched upload complete = %v %v", a.Complete, err)
	}

	// Two attachments with the same content: the second reuses the bytes.
	content := []byte("shared content")
	for _, tc := range []struct {
		device, id string
		wantReused bool
	}{
		{"dev-1", "first", false},
		{"dev-2", "second", true},
	} {
		meta := toAttachment(tc.id, int64(len(content)), 8, content)
		if _, err := s.CreateAttachment(tc.device, meta); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(tc.device, tc.id, 0, content[:8]); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(tc.device, tc.id, 1, content[8:]); err != nil {
			t.Fatal(err)
		}
		res, err := s.CompleteAttachment(tc.device, tc.id)
		if err != nil {
			t.Fatalf("complete %q: %v", tc.id, err)
		}
		if res.Reused != tc.wantReused {
			t.Fatalf("complete %q reused = %v, want %v", tc.id, res.Reused, tc.wantReused)
		}
	}
}

// TestSealedAttachmentRejectsChunks covers post-seal immutability: once a
// finish has committed, every further chunk write — identical bytes, different
// bytes, even an out-of-range index — is ErrAttachmentSealed and the recorded
// state and content are unchanged. The rejection and the original bytes
// survive a restart.
func TestSealedAttachmentRejectsChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")

	content := []byte("hello world") // 11 bytes, chunks of 4: [hell][o wo][rld]
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}
	for i, chunk := range [][]byte{[]byte("hell"), []byte("o wo"), []byte("rld")} {
		if _, err := s.PutChunk("dev-1", "att-1", int64(i), chunk); err != nil {
			t.Fatal(err)
		}
	}
	res, err := s.CompleteAttachment("dev-1", "att-1")
	if err != nil || !res.Complete {
		t.Fatalf("complete = %+v %v", res, err)
	}

	assertSealed := func(t *testing.T, s *Store) {
		t.Helper()
		// Byte-identical resubmission is no longer idempotent: it is rejected.
		if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); !errors.Is(err, ErrAttachmentSealed) {
			t.Fatalf("identical chunk after seal = %v, want ErrAttachmentSealed", err)
		}
		// Different bytes at an existing index are rejected too.
		if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("HELL")); !errors.Is(err, ErrAttachmentSealed) {
			t.Fatalf("conflicting chunk after seal = %v, want ErrAttachmentSealed", err)
		}
		// Even an out-of-range write hits the seal before any shape check.
		if _, err := s.PutChunk("dev-1", "att-1", 7, []byte("xxxx")); !errors.Is(err, ErrAttachmentSealed) {
			t.Fatalf("out-of-range chunk after seal = %v, want ErrAttachmentSealed", err)
		}
		// The recorded state and content are unchanged.
		a, indices, err := s.GetAttachment("dev-1", "att-1")
		if err != nil || !a.Complete || len(indices) != 3 {
			t.Fatalf("sealed attachment = %+v %v %v", a, indices, err)
		}
		data, err := s.GetAttachmentChunk("dev-1", "att-1", 0)
		if err != nil || string(data) != "hell" {
			t.Fatalf("chunk 0 after rejected writes = %q %v", data, err)
		}
		// A repeat finish still returns the first recorded result.
		again, err := s.CompleteAttachment("dev-1", "att-1")
		if err != nil || again != res {
			t.Fatalf("repeat complete = %+v %v, want %+v", again, err, res)
		}
	}
	assertSealed(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// After a restart the seal still holds and the original bytes are intact.
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	assertSealed(t, s)
}

func TestAttachmentsPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")
	content := []byte("hello world")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// Idempotency decisions survive the restart.
	created, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content))
	if err != nil || created {
		t.Fatalf("create after reopen = %v %v, want idempotent", created, err)
	}
	created, err = s.PutChunk("dev-1", "att-1", 0, []byte("hell"))
	if err != nil || created {
		t.Fatalf("chunk after reopen = %v %v, want idempotent", created, err)
	}
	// The upload resumes and finishes.
	if _, err := s.PutChunk("dev-1", "att-1", 1, []byte("o wo")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 2, []byte("rld")); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteAttachment("dev-1", "att-1")
	if err != nil || !res.Complete || res.Reused {
		t.Fatalf("complete after reopen = %+v %v", res, err)
	}
}

// toAttachment builds the create-time metadata for content with the given
// chunking.
func toAttachment(id string, total, chunk int64, content []byte) Attachment {
	return Attachment{
		ID:         id,
		TotalBytes: total,
		ChunkSize:  chunk,
		SHA256:     digestOf(content),
	}
}

// TestReuseContentDecision covers the digest/size reuse matrix directly: no
// content -> not reused; same digest and size -> reused; same digest but a
// different size -> ErrAttachmentDigestConflict. The last state cannot arise
// from honest uploads (it would require a SHA-256 collision), but the decision
// must never reuse bytes whose length differs from the declared total.
func TestReuseContentDecision(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	const d = "0000000000000000000000000000000000000000000000000000000000000000"

	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	reused, err := reuseContentTx(tx, d, 4)
	if err != nil || reused {
		t.Fatalf("absent content: reused=%v err=%v, want false/nil", reused, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := s.db.Exec(
		`INSERT INTO attachment_contents (sha256, size, data) VALUES (?, ?, ?)`,
		d, int64(4), []byte("data"),
	); err != nil {
		t.Fatal(err)
	}

	tx, err = s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if reused, err := reuseContentTx(tx, d, 4); err != nil || !reused {
		t.Fatalf("same digest/size: reused=%v err=%v, want true/nil", reused, err)
	}
	if _, err := reuseContentTx(tx, d, 3); !errors.Is(err, ErrAttachmentDigestConflict) {
		t.Fatalf("same digest different size = %v, want ErrAttachmentDigestConflict", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestGetAttachmentChunkRange rejects an out-of-range index as invalid (400 at
// the HTTP layer) even when the attachment is otherwise known, while an
// in-range index without a chunk stays a not-found.
func TestGetAttachmentChunkRange(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")

	content := []byte("hello world") // 11 bytes / 4 -> 3 chunks (0..2)
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}

	var invalid *ErrChunkInvalid
	if _, err := s.GetAttachmentChunk("dev-1", "att-1", 3); !errors.As(err, &invalid) {
		t.Fatalf("out-of-range read = %v, want *ErrChunkInvalid", err)
	}
	if _, err := s.GetAttachmentChunk("dev-1", "att-1", 0); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("in-range missing chunk = %v, want ErrChunkNotFound", err)
	}
	if _, err := s.GetAttachmentChunk("dev-1", "nope", 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment = %v, want ErrAttachmentNotFound", err)
	}
}

// TestDeleteAttachment covers the owner-scoped hard delete: unknown ids and
// non-creators miss without writing, a delete removes the record with its
// chunks and grants, a repeat delete misses again, and the id is free to be
// created again as a brand-new upload.
func TestDeleteAttachment(t *testing.T) {
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
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}

	// Unknown id and non-creator miss without writing.
	if err := s.DeleteAttachment("dev-1", "nope"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("delete unknown = %v, want ErrAttachmentNotFound", err)
	}
	if err := s.DeleteAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("delete non-creator = %v, want ErrAttachmentForbidden", err)
	}
	// The forbidden delete wrote nothing: record, chunk and grant are intact.
	if _, indices, err := s.GetAttachment("dev-1", "att-1"); err != nil || len(indices) != 1 {
		t.Fatalf("attachment after forbidden delete = %v %v", indices, err)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-1"); err != nil {
		t.Fatalf("granted read after forbidden delete = %v", err)
	}

	// The creator deletes; the record, its chunks and its grants are gone.
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, _, err := s.GetAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("read after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, _, err := s.GetAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("granted read after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.GetAttachmentChunk("dev-1", "att-1", 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("chunk after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("chunk write after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.CompleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("complete after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("access after delete = %v, want ErrAttachmentNotFound", err)
	}

	// A repeat delete misses the same way and changes nothing.
	if err := s.DeleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("repeat delete = %v, want ErrAttachmentNotFound", err)
	}

	// The id is free again: re-creating starts a brand-new upload, even with
	// different metadata, and no old chunk leaks into it.
	created, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 4, []byte("data")))
	if err != nil || !created {
		t.Fatalf("re-create after delete = %v %v, want created", created, err)
	}
	_, indices, err := s.GetAttachment("dev-1", "att-1")
	if err != nil || len(indices) != 0 {
		t.Fatalf("recreated upload indices = %v %v, want empty", indices, err)
	}
}

// TestDeleteAttachmentContentReclamation verifies that digest-addressed
// content is reclaimed only with its last completed referrer: deleting one of
// two identical finished attachments keeps the bytes for the other and for
// later reuses, while deleting the last one drops them, so the next identical
// upload stores the content fresh.
func TestDeleteAttachmentContentReclamation(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("shared content")
	complete := func(device, id string) CompleteResult {
		t.Helper()
		if _, err := s.CreateAttachment(device, toAttachment(id, int64(len(content)), 8, content)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(device, id, 0, content[:8]); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(device, id, 1, content[8:]); err != nil {
			t.Fatal(err)
		}
		res, err := s.CompleteAttachment(device, id)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	if res := complete("dev-1", "att-1"); res.Reused {
		t.Fatalf("first complete reused = %+v", res)
	}
	if res := complete("dev-2", "att-2"); !res.Reused {
		t.Fatalf("second complete reused = %+v", res)
	}

	// Deleting the first referrer leaves the content for the second
	// attachment and for later identical uploads.
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatal(err)
	}
	if data, err := s.GetAttachmentChunk("dev-2", "att-2", 0); err != nil || string(data) != string(content[:8]) {
		t.Fatalf("surviving chunk = %q %v", data, err)
	}
	if res := complete("dev-1", "att-3"); !res.Reused {
		t.Fatalf("complete while still referenced = %+v, want reused", res)
	}

	// Deleting the last two referrers reclaims the bytes: the next identical
	// upload stores them again.
	if err := s.DeleteAttachment("dev-2", "att-2"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAttachment("dev-1", "att-3"); err != nil {
		t.Fatal(err)
	}
	if res := complete("dev-1", "att-4"); res.Reused {
		t.Fatalf("complete after reclamation = %+v, want not reused", res)
	}
}

// TestDeleteAttachmentPersistsAcrossReopen deletes a finished and an
// in-progress upload, reopens the database and verifies both are still gone
// while a surviving duplicate keeps its content.
func TestDeleteAttachmentPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("shared content")
	finish := func(device, id string) {
		t.Helper()
		if _, err := s.CreateAttachment(device, toAttachment(id, int64(len(content)), 8, content)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(device, id, 0, content[:8]); err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutChunk(device, id, 1, content[8:]); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CompleteAttachment(device, id); err != nil {
			t.Fatal(err)
		}
	}
	finish("dev-1", "att-1")
	finish("dev-2", "att-2")
	// An in-progress upload that is deleted before finishing.
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-3", 11, 4, []byte("hello world"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-3", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAttachment("dev-1", "att-3"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()

	// Both deletes survived the restart; the surviving duplicate still reads
	// its content.
	if _, _, err := s.GetAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("finished delete after reopen = %v, want ErrAttachmentNotFound", err)
	}
	if _, _, err := s.GetAttachment("dev-1", "att-3"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("in-progress delete after reopen = %v, want ErrAttachmentNotFound", err)
	}
	if err := s.DeleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("repeat delete after reopen = %v, want ErrAttachmentNotFound", err)
	}
	if data, err := s.GetAttachmentChunk("dev-2", "att-2", 1); err != nil || string(data) != string(content[8:]) {
		t.Fatalf("surviving chunk after reopen = %q %v", data, err)
	}
}
