package store

import (
	"bytes"
	"errors"
	"testing"
)

// seedSealedAttachment creates a sealed three-chunk attachment owned by
// dev-1 holding the bytes of content with the given chunk size.
func seedSealedAttachment(t *testing.T, s *Store, id, device string, content []byte, chunkSize int64) {
	t.Helper()
	if _, err := s.CreateAttachment(device, toAttachment(id, int64(len(content)), chunkSize, content)); err != nil {
		t.Fatalf("create: %v", err)
	}
	for off := int64(0); off < int64(len(content)); off += chunkSize {
		end := off + chunkSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		if _, err := s.PutChunk(device, id, off/chunkSize, content[off:end]); err != nil {
			t.Fatalf("put chunk: %v", err)
		}
	}
	if _, err := s.CompleteAttachment(device, id); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

// GetAttachmentContent assembles the stored chunks in ascending index order
// and returns the exact sealed bytes; repeated reads are identical.
func TestGetAttachmentContentAssemblesChunks(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("hello world") // chunks of 4: [hell][o wo][rld]
	seedSealedAttachment(t, s, "att-1", "dev-1", content, 4)

	got, err := s.GetAttachmentContent("dev-1", "att-1")
	if err != nil {
		t.Fatalf("content = %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content = %q, want %q", got, content)
	}
	again, err := s.GetAttachmentContent("dev-1", "att-1")
	if err != nil || !bytes.Equal(again, got) {
		t.Fatalf("repeat content = %v %q, want identical", err, again)
	}

	// A granted device reads the same bytes; a stranger is forbidden.
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatalf("grant: %v", err)
	}
	got, err = s.GetAttachmentContent("dev-2", "att-1")
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("granted content = %v %q", err, got)
	}
	mustRegisterDevice(t, s, "dev-3")
	if _, err := s.GetAttachmentContent("dev-3", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("stranger = %v, want ErrAttachmentForbidden", err)
	}
}

// The content-read judgments: unknown attachment is ErrAttachmentNotFound,
// neither-creator-nor-granted caller is ErrAttachmentForbidden, and an
// unsealed upload is ErrAttachmentNotSealed — in that fixed order and with
// zero writes.
func TestGetAttachmentContentJudgments(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	// Unknown attachment misses for every caller.
	if _, err := s.GetAttachmentContent("dev-1", "nope"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("creator unknown = %v, want ErrAttachmentNotFound", err)
	}
	if _, err := s.GetAttachmentContent("dev-2", "nope"); !errors.Is(err, ErrAttachmentNotFound) {
		t.Fatalf("stranger unknown = %v, want ErrAttachmentNotFound", err)
	}

	// Unsealed upload: the creator gets not-sealed, the other device forbidden.
	content := []byte("data")
	if _, err := s.CreateAttachment("dev-1", toAttachment("att-1", 4, 2, content)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutChunk("dev-1", "att-1", 0, content[:2]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAttachmentContent("dev-2", "att-1"); !errors.Is(err, ErrAttachmentForbidden) {
		t.Fatalf("stranger unsealed = %v, want ErrAttachmentForbidden", err)
	}
	if _, err := s.GetAttachmentContent("dev-1", "att-1"); !errors.Is(err, ErrAttachmentNotSealed) {
		t.Fatalf("creator unsealed = %v, want ErrAttachmentNotSealed", err)
	}

	// The reads changed nothing: the upload finishes and then reads complete.
	if _, err := s.PutChunk("dev-1", "att-1", 1, content[2:]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, err := s.GetAttachmentContent("dev-1", "att-1")
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("sealed content = %v %q", err, got)
	}
}

// A deleted attachment misses the content read for everyone, and the read
// itself leaves no state behind.
func TestGetAttachmentContentAfterDelete(t *testing.T) {
	s, err := Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	mustRegisterDevice(t, s, "dev-1")
	mustRegisterDevice(t, s, "dev-2")

	content := []byte("gone soon")
	seedSealedAttachment(t, s, "att-1", "dev-1", content, 4)
	if _, err := s.SetAttachmentAccess("dev-1", "att-1", "dev-2", true); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteAttachment("dev-1", "att-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	for _, device := range []string{"dev-1", "dev-2"} {
		if _, err := s.GetAttachmentContent(device, "att-1"); !errors.Is(err, ErrAttachmentNotFound) {
			t.Fatalf("content after delete (%s) = %v, want ErrAttachmentNotFound", device, err)
		}
	}
}
