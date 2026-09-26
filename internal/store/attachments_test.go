package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
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

// seedCompletedAttachment creates an attachment for device, uploads content
// in one chunk and seals it, failing the test on any error.
func seedCompletedAttachment(t *testing.T, s *Store, device, id string, content []byte) {
	t.Helper()
	if _, err := s.CreateAttachment(device, toAttachment(id, int64(len(content)), int64(len(content)), content)); err != nil {
		t.Fatalf("create %q: %v", id, err)
	}
	if _, err := s.PutChunk(device, id, 0, content); err != nil {
		t.Fatalf("put chunk %q: %v", id, err)
	}
	if _, err := s.CompleteAttachment(device, id); err != nil {
		t.Fatalf("complete %q: %v", id, err)
	}
}

func contentRowExists(t *testing.T, s *Store, digest string) bool {
	t.Helper()
	var n int
	if err := s.db.QueryRow(
		`SELECT COUNT(*) FROM attachment_contents WHERE sha256 = ?`, digest,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// TestDeleteAttachmentSealed covers the hard removal of a finished upload:
// only the creator deletes it, the metadata, chunks, seal and grants all go,
// the digest bytes are reclaimed, every later operation misses and the id is
// free for a brand-new upload.
func TestDeleteAttachmentSealed(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("hello world")
	seedCompletedAttachment(t, s, "dev-1", "att-1", content)
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}

	// Neither a granted reader nor a stranger may delete it.
	if err := s.DeleteAttachment("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("granted-reader delete = %v, want ErrAttachmentForbidden", err)
	}
	// The forbidden delete changes nothing: the grant and content survive.
	if _, _, err := s.GetAttachment("dev-2", "att-1"); err != nil {
		t.Fatalf("attachment changed by forbidden delete: %v", err)
	}
	if err := s.DeleteAttachment("dev-1", "nope"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown delete = %v, want ErrAttachmentNotFound", err)
	}

	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("creator delete: %v", err)
	}

	// The record, chunks and grants are gone: everyone now gets not-found.
	for _, dev := range []string{"dev-1", "dev-2"} {
		if _, _, err := s.GetAttachment(dev, "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("get after delete as %s = %v, want ErrAttachmentNotFound", dev, err)
		}
		if _, err := s.GetAttachmentChunk(dev, "att-1", 0); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("get chunk after delete as %s = %v, want ErrAttachmentNotFound", dev, err)
		}
	}
	// Further writes, finishes and access changes all miss with a 404 and zero
	// writes.
	if _, err := s.PutChunk("dev-1", "att-1", 0, content); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("chunk after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.CompleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("complete after delete = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("grant after delete = %v, want ErrAttachmentNotFound", err)
	}
	// A repeat delete misses and changes nothing.
	if err := s.DeleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("repeat delete = %v, want ErrAttachmentNotFound", err)
	}
	// The last reference is gone, so the digest-addressed bytes were reclaimed.
	if contentRowExists(t, s, digestOf(content)) {
		t.Fatal("sealed content row survived the last referencing delete")
	}

	// The id starts a brand-new upload: created=true, no chunks carried over.
	created, err := s.CreateAttachment("dev-1", toAttachment("att-1", int64(len(content)), int64(len(content)), content))
	if err != nil || !created {
		t.Fatalf("recreate = %v %v, want created=true", created, err)
	}
	if _, indices, err := s.GetAttachment("dev-1", "att-1"); err != nil || len(indices) != 0 {
		t.Fatalf("recreated attachment carries old state: %v %v", indices, err)
	}
}

// TestDeleteAttachmentIncomplete removes a partial upload: its received
// chunks and progress disappear even though it never sealed content.
func TestDeleteAttachmentIncomplete(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")

	content := []byte("hello world") // 11 bytes, chunks of 4
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 2, []byte("rld")); err != nil {
		t.Fatal(err)
	}

	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("delete incomplete: %v", err)
	}
	if _, _, err := s.GetAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("get after delete = %v, want ErrAttachmentNotFound", err)
	}
	// No content row ever existed for an unfinished upload.
	if contentRowExists(t, s, digestOf(content)) {
		t.Fatal("unfinished upload left a content row")
	}

	// A fresh upload under the same id starts from zero and the chunk range is
	// free again.
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	created, err := s.PutChunk("dev-1", "att-1", 0, []byte("hell"))
	if err != nil || !created {
		t.Fatalf("chunk after recreate = %v %v, want created=true", created, err)
	}
}

// TestDeleteAttachmentContentReferenceCounting verifies that the digest bytes
// survive while any completed attachment references them and are reclaimed
// only when the last reference is deleted; a reusing attachment's removal
// leaves the first attachment's bytes readable.
func TestDeleteAttachmentContentReferenceCounting(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("shared content")
	digest := digestOf(content)
	seedCompletedAttachment(t, s, "dev-1", "att-1", content)
	seedCompletedAttachment(t, s, "dev-2", "att-2", content) // reuses the bytes
	if !contentRowExists(t, s, digest) {
		t.Fatal("content row missing after two completed attachments")
	}

	// Deleting the reusing attachment must not reclaim the bytes: the first
	// attachment still reads its chunks and a new identical upload reuses.
	if err := s.DeleteAttachment("dev-2", "att-2"); err != nil {
		t.Fatalf("delete reuser: %v", err)
	}
	if !contentRowExists(t, s, digest) {
		t.Fatal("content reclaimed while another attachment still references it")
	}
	data, err := s.GetAttachmentChunk("dev-1", "att-1", 0)
	if err != nil || string(data) != string(content) {
		t.Fatalf("remaining attachment chunk = %q %v", data, err)
	}
	seedCompletedAttachment(t, s, "dev-2", "att-3", content)
	res, err := s.CompleteAttachment("dev-2", "att-3")
	if err != nil || !res.Reused {
		t.Fatalf("complete after one reference deleted = %+v %v, want reused", res, err)
	}

	// Delete the two remaining references; only the last delete reclaims.
	if err := s.DeleteAttachment("dev-2", "att-3"); err != nil {
		t.Fatalf("delete att-3: %v", err)
	}
	if !contentRowExists(t, s, digest) {
		t.Fatal("content reclaimed before the last referencing attachment was deleted")
	}
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("delete att-1: %v", err)
	}
	if contentRowExists(t, s, digest) {
		t.Fatal("content row survived the last referencing delete")
	}
	// A fresh identical upload now stores the bytes again: not reused.
	seedCompletedAttachment(t, s, "dev-1", "att-4", content)
	if res, err := s.CompleteAttachment("dev-1", "att-4"); err != nil || res.Reused {
		t.Fatalf("complete after GC = %+v %v, want reused=false", res, err)
	}
}

// TestDeleteAttachmentPersistsAcrossRestart verifies the removal is durable:
// after a reopen the attachment, its chunks and grants stay gone and a repeat
// delete still misses.
func TestDeleteAttachmentPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")
	content := []byte("hello world")
	seedCompletedAttachment(t, s, "dev-1", "att-1", content)
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	for _, dev := range []string{"dev-1", "dev-2"} {
		if _, _, err := s.GetAttachment(dev, "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("get after restart as %s = %v, want ErrAttachmentNotFound", dev, err)
		}
	}
	if contentRowExists(t, s, digestOf(content)) {
		t.Fatal("content row survived a restart after deletion")
	}
	if err := s.DeleteAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("repeat delete after restart = %v, want ErrAttachmentNotFound", err)
	}
	// The id is free for a brand-new upload after the restart.
	if created, err := s.CreateAttachment("dev-1", toAttachment("att-1", 11, 4, content)); err != nil || !created {
		t.Fatalf("recreate after restart = %v %v, want created=true", created, err)
	}
}

// TestDeleteAttachmentConcurrent verifies that concurrent deletes of the same
// attachment take effect at most once.
func TestDeleteAttachmentConcurrent(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	seedCompletedAttachment(t, s, "dev-1", "att-1", []byte("data"))

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.DeleteAttachment("dev-1", "att-1")
			switch {
			case err == nil:
				mu.Lock()
				succeeded++
				mu.Unlock()
			case errors.Is(err, ErrAttachmentNotFound):
			default:
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if succeeded != 1 {
		t.Fatalf("successful deletes = %d, want exactly 1", succeeded)
	}
	if _, _, err := s.GetAttachment("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("attachment after concurrent delete = %v, want ErrAttachmentNotFound", err)
	}
}
