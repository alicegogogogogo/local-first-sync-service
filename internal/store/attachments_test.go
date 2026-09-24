package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"
)

func shaHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func mustRegisterAttachmentDevice(t *testing.T, s *Store, deviceID string) {
	t.Helper()
	if _, err := s.RegisterDevice(deviceID); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAttachmentValidationAndOwnership(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()

	digest := shaHex([]byte("hello"))

	// Unregistered device -> ErrDeviceNotFound, nothing written.
	if _, err := s.CreateAttachment("ghost", "a1", 5, 4, digest); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("unknown device err = %v, want ErrDeviceNotFound", err)
	}

	mustRegisterAttachmentDevice(t, s, "dev-1")
	mustRegisterAttachmentDevice(t, s, "dev-2")

	// First create.
	created, err := s.CreateAttachment("dev-1", "a1", 5, 4, digest)
	if err != nil || !created {
		t.Fatalf("create = created:%v err:%v", created, err)
	}

	// Identical metadata retry by the same owner is idempotent.
	created, err = s.CreateAttachment("dev-1", "a1", 5, 4, digest)
	if err != nil || created {
		t.Fatalf("retry = created:%v err:%v", created, err)
	}

	// Same id, other device: conflict even with identical metadata.
	_, err = s.CreateAttachment("dev-2", "a1", 5, 4, digest)
	var conflict *ErrAttachmentConflict
	if !errors.As(err, &conflict) || conflict.ID != "a1" {
		t.Fatalf("cross-device err = %v, want *ErrAttachmentConflict{a1}", err)
	}

	// Same owner, different metadata: also a conflict, first record unchanged.
	_, err = s.CreateAttachment("dev-1", "a1", 6, 4, digest)
	if !errors.As(err, &conflict) {
		t.Fatalf("changed-metadata err = %v, want conflict", err)
	}

	// Original metadata survives both conflicts.
	meta, err := s.GetAttachmentMeta("dev-1", "a1")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 5 || meta.ChunkSize != 4 || meta.SHA256 != digest || meta.Completed {
		t.Fatalf("metadata mutated after conflict: %+v", meta)
	}

	// A non-owner cannot even read metadata.
	if _, err := s.GetAttachmentMeta("dev-2", "a1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-owner meta err = %v, want ErrAttachmentForbidden", err)
	}
}

func TestChunkUploadOutOfOrderAndIdempotency(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")

	content := []byte("0123456789") // 10 bytes, chunk size 4 -> lengths 4,4,2
	digest := shaHex(content)
	if _, err := s.CreateAttachment("dev", "att", int64(len(content)), 4, digest); err != nil {
		t.Fatal(err)
	}

	// Unknown attachment -> not found; wrong owner -> forbidden.
	if err := s.PutChunk("dev", "ghost", 0, []byte("0123")); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment err = %v", err)
	}
	mustRegisterAttachmentDevice(t, s, "other")
	if err := s.PutChunk("other", "att", 0, []byte("0123")); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("other owner err = %v", err)
	}

	// Out-of-range index -> bad chunk.
	if err := s.PutChunk("dev", "att", 3, make([]byte, 1)); !errors.Is(err, ErrAttachmentBadChunk) {
		t.Fatalf("index 3 err = %v, want ErrAttachmentBadChunk", err)
	}

	// Wrong length for a non-final chunk and for the final chunk -> bad chunk.
	if err := s.PutChunk("dev", "att", 0, []byte("012")); !errors.Is(err, ErrAttachmentBadChunk) {
		t.Fatalf("short non-final err = %v", err)
	}
	if err := s.PutChunk("dev", "att", 2, []byte("89X")); !errors.Is(err, ErrAttachmentBadChunk) {
		t.Fatalf("long final err = %v", err)
	}

	// Upload out of order: 2, then 0, then 1.
	if err := s.PutChunk("dev", "att", 2, content[8:10]); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk("dev", "att", 0, content[0:4]); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk("dev", "att", 1, content[4:8]); err != nil {
		t.Fatal(err)
	}

	// Same index, same bytes: idempotent.
	if err := s.PutChunk("dev", "att", 0, content[0:4]); err != nil {
		t.Fatalf("identical re-post err = %v", err)
	}
	// Same index, different bytes: conflict, first bytes retained.
	err := s.PutChunk("dev", "att", 0, []byte("ZZZZ"))
	var chunkConflict *ErrChunkConflict
	if !errors.As(err, &chunkConflict) || chunkConflict.Index != 0 {
		t.Fatalf("different bytes err = %v, want *ErrChunkConflict{0}", err)
	}
	got, err := s.GetAttachmentChunk("dev", "att", 0)
	if err != nil || string(got) != "0123" {
		t.Fatalf("first bytes not retained: %q err=%v", got, err)
	}

	// Metadata reports sorted received indices and pending state.
	meta, err := s.GetAttachmentMeta("dev", "att")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Completed || len(meta.Received) != 3 ||
		meta.Received[0] != 0 || meta.Received[1] != 1 || meta.Received[2] != 2 {
		t.Fatalf("meta = %+v", meta)
	}
}

func TestCompleteAttachmentHappyPathAndRepeats(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")

	content := []byte("0123456789")
	digest := shaHex(content)
	if _, err := s.CreateAttachment("dev", "att", int64(len(content)), 4, digest); err != nil {
		t.Fatal(err)
	}

	// Completing with no chunks is incomplete and resumable.
	if err := s.uploadChunks("dev", "att", content, 4, 0); err == nil {
		t.Fatal("expected incomplete error")
	} else if !errors.Is(err, ErrAttachmentIncomplete) {
		t.Fatalf("empty complete err = %v", err)
	}

	// Upload the first two of three chunks: still incomplete, but resumable.
	if err := s.PutChunk("dev", "att", 0, content[0:4]); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk("dev", "att", 1, content[4:8]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev", "att"); !errors.Is(err, ErrAttachmentIncomplete) {
		t.Fatalf("missing chunk complete err = %v", err)
	}

	// Finish and complete.
	if err := s.PutChunk("dev", "att", 2, content[8:10]); err != nil {
		t.Fatal(err)
	}
	res, err := s.CompleteAttachment("dev", "att")
	if err != nil {
		t.Fatalf("complete err = %v", err)
	}
	if res.ID != "att" || res.Size != 10 || res.SHA256 != digest || !res.Completed || res.Reused {
		t.Fatalf("completion = %+v", res)
	}

	// Repeat completion returns the same result.
	res2, err := s.CompleteAttachment("dev", "att")
	if err != nil {
		t.Fatal(err)
	}
	if res2 != res {
		t.Fatalf("repeat completion = %+v, want %+v", res2, res)
	}

	// Only the creator may complete.
	if _, err := s.CompleteAttachment("other", "att"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-creator complete err = %v", err)
	}
}

// uploadChunks is a test helper that posts only the first limit chunks; it
// returns the completion error so callers can assert on incomplete state.
func (s *Store) uploadChunks(deviceID, id string, content []byte, chunkSize int64, limit int) error {
	for off, idx := int64(0), int64(0); off < int64(len(content)); off, idx = off+chunkSize, idx+1 {
		if int(idx) >= limit {
			break
		}
		end := off + chunkSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		if err := s.PutChunk(deviceID, id, idx, content[off:end]); err != nil {
			return err
		}
	}
	_, err := s.CompleteAttachment(deviceID, id)
	return err
}

func TestCompleteAttachmentDigestMismatchStaysUnsealed(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")

	content := []byte("0123456789")
	wrongDigest := shaHex([]byte("different"))
	// chunkSize 4 -> 3 chunks covering exactly 10 bytes, so length checks pass
	// but the digest does not.
	if _, err := s.CreateAttachment("dev", "att", int64(len(content)), 4, wrongDigest); err != nil {
		t.Fatal(err)
	}
	if err := s.uploadChunks("dev", "att", content, 4, 3); !errors.Is(err, ErrAttachmentDigest) {
		t.Fatalf("digest mismatch err = %v, want ErrAttachmentDigest", err)
	}

	// Still pending: metadata shows completion false and chunks remain readable.
	meta, err := s.GetAttachmentMeta("dev", "att")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Completed || len(meta.Received) != 3 {
		t.Fatalf("attachment should remain unsealed: %+v", meta)
	}
	if _, err := s.GetAttachmentChunk("dev", "att", 2); err != nil {
		t.Fatalf("chunk should remain readable after digest failure: %v", err)
	}
}

func TestCompleteAttachmentReusesIdenticalContent(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")

	content := []byte("dedup-me-please")
	digest := shaHex(content)

	// First attachment completes and owns the blob.
	if _, err := s.CreateAttachment("dev", "a1", int64(len(content)), 5, digest); err != nil {
		t.Fatal(err)
	}
	if err := s.uploadChunks("dev", "a1", content, 5, 3); err != nil {
		t.Fatalf("a1 complete: %v", err)
	}

	// Second attachment, same digest AND size (different chunk layout): the
	// sealed content is reused, no bytes copied.
	if _, err := s.CreateAttachment("dev", "a2", int64(len(content)), 7, digest); err != nil {
		t.Fatal(err)
	}
	if err := s.uploadChunks("dev", "a2", content, 7, 3); err != nil {
		t.Fatalf("a2 complete: %v", err)
	}
	res, err := s.CompleteAttachment("dev", "a2")
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reused || res.ReusedFrom != "a1" || res.Size != int64(len(content)) || res.SHA256 != digest {
		t.Fatalf("expected reuse of a1, got %+v", res)
	}

	// Exactly one blob row exists.
	var blobs int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM attachment_blobs`).Scan(&blobs); err != nil {
		t.Fatal(err)
	}
	if blobs != 1 {
		t.Fatalf("blob rows = %d, want 1 (content copied instead of reused)", blobs)
	}
}

func TestCompleteAttachmentSameDigestDifferentSizeConflicts(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")

	content := []byte("01234")
	digest := shaHex(content)

	// Seed a sealed blob that carries the same digest but a different size. In
	// honest operation a digest pins the content (and hence its size), so this
	// directly inserts the invariant the completion guard must refuse to reuse.
	if _, err := s.db.Exec(
		`INSERT INTO attachments (id, device_id, size, chunk_size, sha256, status, content_hash, completed_at)
		 VALUES ('prior', 'dev', 99, 99, ?, 'completed', ?, CURRENT_TIMESTAMP)`,
		digest, digest,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(
		`INSERT INTO attachment_blobs (sha256, size, data) VALUES (?, 99, ?)`,
		digest, content,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateAttachment("dev", "a2", int64(len(content)), 5, digest); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk("dev", "a2", 0, content); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev", "a2"); !errors.Is(err, ErrAttachmentContentConflict) {
		t.Fatalf("same digest different size err = %v, want ErrAttachmentContentConflict", err)
	}

	// The upload remains resumable: chunks are intact and completion still fails
	// the same way rather than having mutated any state.
	meta, err := s.GetAttachmentMeta("dev", "a2")
	if err != nil {
		t.Fatal(err)
	}
	if meta.Completed || len(meta.Received) != 1 {
		t.Fatalf("a2 should stay resumable: %+v", meta)
	}
}

func TestGetAttachmentChunkStates(t *testing.T) {
	s, _ := Open("")
	defer func() { _ = s.Close() }()
	mustRegisterAttachmentDevice(t, s, "dev")
	mustRegisterAttachmentDevice(t, s, "other")

	content := []byte("0123456789")
	digest := shaHex(content)
	if _, err := s.CreateAttachment("dev", "att", int64(len(content)), 4, digest); err != nil {
		t.Fatal(err)
	}
	if err := s.PutChunk("dev", "att", 0, content[0:4]); err != nil {
		t.Fatal(err)
	}

	// Unknown attachment.
	if _, err := s.GetAttachmentChunk("dev", "ghost", 0); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("unknown attachment err = %v", err)
	}
	// Non-owner.
	if _, err := s.GetAttachmentChunk("other", "att", 0); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("non-owner err = %v", err)
	}
	// Out-of-range index.
	if _, err := s.GetAttachmentChunk("dev", "att", 3); !errors.Is(err, ErrAttachmentBadChunk) {
		t.Fatalf("bad index err = %v", err)
	}
	// In-range but not yet received.
	if _, err := s.GetAttachmentChunk("dev", "att", 1); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("missing chunk err = %v", err)
	}
	// Received chunk returns its bytes.
	got, err := s.GetAttachmentChunk("dev", "att", 0)
	if err != nil || string(got) != "0123" {
		t.Fatalf("chunk = %q err=%v", got, err)
	}
}

func TestAttachmentsPersistAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	mustRegisterAttachmentDevice(t, s, "dev")
	content := []byte("persist-this")
	digest := shaHex(content)
	if _, err := s.CreateAttachment("dev", "att", int64(len(content)), 6, digest); err != nil {
		t.Fatal(err)
	}
	// Upload only the first chunk, then restart mid-upload.
	if err := s.PutChunk("dev", "att", 0, content[0:6]); err != nil {
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

	// Create is idempotent after restart, metadata and received chunk survive.
	created, err := s2.CreateAttachment("dev", "att", int64(len(content)), 6, digest)
	if err != nil || created {
		t.Fatalf("create replay = created:%v err:%v", created, err)
	}
	meta, err := s2.GetAttachmentMeta("dev", "att")
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Received) != 1 || meta.Received[0] != 0 || meta.Completed {
		t.Fatalf("mid-upload state lost: %+v", meta)
	}
	got, err := s2.GetAttachmentChunk("dev", "att", 0)
	if err != nil || string(got) != string(content[0:6]) {
		t.Fatalf("chunk bytes lost: %q err=%v", got, err)
	}

	// Resume, complete, restart: sealed state and dedup decision persist.
	if err := s2.PutChunk("dev", "att", 1, content[6:]); err != nil {
		t.Fatal(err)
	}
	res, err := s2.CompleteAttachment("dev", "att")
	if err != nil || !res.Completed {
		t.Fatalf("resumed complete = %+v err=%v", res, err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s3.Close() }()
	res2, err := s3.CompleteAttachment("dev", "att")
	if err != nil {
		t.Fatal(err)
	}
	if !res2.Completed || res2.SHA256 != digest || res2.Size != int64(len(content)) {
		t.Fatalf("sealed state lost: %+v", res2)
	}
	// Chunk idempotency still holds after restart.
	if err := s3.PutChunk("dev", "att", 0, content[0:6]); err != nil {
		t.Fatalf("chunk replay after restart err = %v", err)
	}
	if err := s3.PutChunk("dev", "att", 0, []byte("WRONG!")); err == nil {
		t.Fatal("conflicting chunk after restart unexpectedly accepted")
	} else if !errors.As(err, new(*ErrChunkConflict)) {
		t.Fatalf("chunk conflict after restart err = %v", err)
	}
}

func TestChunkCountDividesLayout(t *testing.T) {
	cases := []struct {
		size, chunk, want int64
	}{
		{1, 1, 1},
		{4, 4, 1},
		{5, 4, 2},
		{8, 4, 2},
		{9, 4, 3},
		{10, 4, 3},
	}
	for _, c := range cases {
		if got := chunkCount(c.size, c.chunk); got != c.want {
			t.Fatalf("chunkCount(%d,%d) = %d, want %d", c.size, c.chunk, got, c.want)
		}
	}
}
