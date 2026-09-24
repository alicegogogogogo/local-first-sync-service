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
	if _, err := s.GetAttachmentChunk("dev-1", "att-1", 5); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("missing chunk = %v, want ErrChunkNotFound", err)
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
