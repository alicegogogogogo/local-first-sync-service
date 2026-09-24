package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrAttachmentNotFound reports that no attachment exists with the given id.
// The caller maps it to 404.
var ErrAttachmentNotFound = errors.New("attachment not found")

// ErrAttachmentForbidden reports that the device in the path is not the
// device that created the attachment. The caller maps it to 403.
var ErrAttachmentForbidden = errors.New("attachment belongs to another device")

// ErrAttachmentIncomplete reports that a finish was attempted before every
// chunk arrived, or the received bytes do not add up to the declared total.
// The upload stays resumable; the caller maps it to 409.
var ErrAttachmentIncomplete = errors.New("attachment chunks are missing or shorter than the declared total")

// ErrAttachmentDigestMismatch reports that the assembled content does not
// hash to the declared digest. The attachment stays incomplete; the caller
// maps it to 422.
var ErrAttachmentDigestMismatch = errors.New("assembled content does not match the declared sha256")

// ErrAttachmentDigestConflict reports that a different-size completed content
// already exists under the same digest, so this upload cannot be finished or
// reused. The upload stays resumable; the caller maps it to 409.
var ErrAttachmentDigestConflict = errors.New("a completed content with the same digest but a different size exists")

// ErrAttachmentConflict reports that an attachment id is already taken — by
// another device, or by this device with different metadata. The original
// record is unchanged; the caller maps it to 409.
type ErrAttachmentConflict struct {
	ID string
}

func (e *ErrAttachmentConflict) Error() string {
	return fmt.Sprintf("attachment %q already exists with different owner or metadata", e.ID)
}

// ErrAttachmentSealed reports a chunk write to an upload that was already
// finished. A sealed attachment is immutable: every further chunk write —
// even a byte-identical resubmission — is rejected and the recorded state and
// content are left unchanged. The caller maps it to 409.
var ErrAttachmentSealed = errors.New("attachment is complete and no longer accepts chunks")

// ErrChunkInvalid reports a chunk that violates the upload's declared shape:
// an out-of-range index, a non-final chunk shorter than the declared chunk
// size, or a final chunk beyond the declared total. The caller maps it to
// 400; nothing is written.
type ErrChunkInvalid struct {
	Reason string
}

func (e *ErrChunkInvalid) Error() string { return e.Reason }

// ErrChunkConflict reports that a chunk index already holds different bytes.
// The first content is kept; the caller maps it to 409.
type ErrChunkConflict struct {
	Index int64
}

func (e *ErrChunkConflict) Error() string {
	return fmt.Sprintf("chunk %d already exists with different content", e.Index)
}

// ErrChunkNotFound reports that no chunk exists at the given index. The
// caller maps it to 404.
var ErrChunkNotFound = errors.New("chunk not found")

// Attachment is the stored metadata of one resumable upload.
type Attachment struct {
	ID         string
	DeviceID   string
	TotalBytes int64
	ChunkSize  int64
	SHA256     string
	Complete   bool
	Reused     bool
}

// CompleteResult reports the outcome of a successful (or repeated) finish.
type CompleteResult struct {
	ID       string `json:"attachmentId"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
	Complete bool   `json:"complete"`
	Reused   bool   `json:"reused"`
}

// chunkCount returns how many chunks the declared shape implies.
func (a Attachment) chunkCount() int64 {
	return (a.TotalBytes + a.ChunkSize - 1) / a.ChunkSize
}

// checkChunkShape validates index and len(data) against the declared
// totalBytes/chunkSize, returning *ErrChunkInvalid on any violation.
func (a Attachment) checkChunkShape(index int64, length int64) error {
	count := a.chunkCount()
	if index < 0 || index >= count {
		return &ErrChunkInvalid{Reason: fmt.Sprintf("chunk index %d is out of range [0, %d)", index, count)}
	}
	if index < count-1 {
		if length != a.ChunkSize {
			return &ErrChunkInvalid{Reason: fmt.Sprintf("non-final chunk %d must be exactly %d bytes", index, a.ChunkSize)}
		}
		return nil
	}
	remaining := a.TotalBytes - a.ChunkSize*(count-1)
	if length < 1 || length > remaining {
		return &ErrChunkInvalid{Reason: fmt.Sprintf("final chunk must be between 1 and %d bytes", remaining)}
	}
	return nil
}

// getAttachmentTx loads an attachment row inside tx and enforces ownership:
// an unknown id yields ErrAttachmentNotFound and a device other than the
// creator yields ErrAttachmentForbidden.
func getAttachmentTx(tx *sql.Tx, id, deviceID string) (Attachment, error) {
	var a Attachment
	var complete, reused int
	err := tx.QueryRow(
		`SELECT id, device_id, total_bytes, chunk_size, sha256, complete, reused
		 FROM attachments WHERE id = ?`, id,
	).Scan(&a.ID, &a.DeviceID, &a.TotalBytes, &a.ChunkSize, &a.SHA256, &complete, &reused)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Attachment{}, ErrAttachmentNotFound
	case err != nil:
		return Attachment{}, err
	}
	if a.DeviceID != deviceID {
		return Attachment{}, ErrAttachmentForbidden
	}
	a.Complete = complete != 0
	a.Reused = reused != 0
	return a, nil
}

// CreateAttachment registers a new resumable upload owned by deviceID.
//
//   - An unregistered device yields ErrDeviceNotFound; nothing is written.
//   - Re-posting identical metadata under the same device is idempotent
//     (created=false).
//   - An id already taken — by another device (even with identical metadata)
//     or by this device with different metadata — yields
//     *ErrAttachmentConflict; the original record is unchanged.
//
// The check and the insert run in one serialized transaction and commit
// synchronously, so a retry after a crash reaches the same decision.
func (s *Store) CreateAttachment(deviceID string, a Attachment) (created bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return false, err
	}
	if !deviceExists {
		return false, ErrDeviceNotFound
	}

	var owner string
	var totalBytes, chunkSize int64
	var digest string
	err = tx.QueryRow(
		`SELECT device_id, total_bytes, chunk_size, sha256 FROM attachments WHERE id = ?`, a.ID,
	).Scan(&owner, &totalBytes, &chunkSize, &digest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO attachments (id, device_id, total_bytes, chunk_size, sha256, complete, reused)
			 VALUES (?, ?, ?, ?, ?, 0, 0)`,
			a.ID, deviceID, a.TotalBytes, a.ChunkSize, a.SHA256,
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	}

	if owner != deviceID || totalBytes != a.TotalBytes || chunkSize != a.ChunkSize || digest != a.SHA256 {
		return false, &ErrAttachmentConflict{ID: a.ID}
	}
	return false, tx.Commit()
}

// PutChunk stores one chunk of an upload, durably, inside one transaction.
//
// Ownership and shape are enforced first: an unknown attachment yields
// ErrAttachmentNotFound, a non-creator device ErrAttachmentForbidden, and an
// out-of-range index or wrong length *ErrChunkInvalid — none of these write
// anything. A finished upload is sealed: any further chunk write, even a
// byte-identical resubmission, yields ErrAttachmentSealed and changes
// nothing. On an open upload, re-submitting identical bytes for an existing
// index is idempotent (created=false); different bytes for the same index
// yield *ErrChunkConflict and the first content is kept. Chunks may arrive in
// any order.
func (s *Store) PutChunk(deviceID, attachmentID string, index int64, data []byte) (created bool, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	a, err := getAttachmentTx(tx, attachmentID, deviceID)
	if err != nil {
		return false, err
	}
	// A sealed upload is immutable: reject every further write before any
	// shape check or content comparison, so the recorded state and content
	// cannot change once the finish committed.
	if a.Complete {
		return false, ErrAttachmentSealed
	}
	if err := a.checkChunkShape(index, int64(len(data))); err != nil {
		return false, err
	}

	var existing []byte
	err = tx.QueryRow(
		`SELECT data FROM attachment_chunks WHERE attachment_id = ? AND idx = ?`,
		attachmentID, index,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO attachment_chunks (attachment_id, idx, data) VALUES (?, ?, ?)`,
			attachmentID, index, data,
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	case err != nil:
		return false, err
	}

	if !bytesEqual(existing, data) {
		return false, &ErrChunkConflict{Index: index}
	}
	return false, tx.Commit()
}

// reuseContentTx resolves the digest-addressed content reuse decision inside
// tx: it returns reused=true when finished content with the same digest and
// size already exists (the bytes are not copied), reused=false when no such
// content exists yet (the caller inserts it), and ErrAttachmentDigestConflict
// when a finished content carries the same digest but a different size. That
// last state cannot arise from honest chunk assembly without a hash collision,
// but it must never silently reuse mismatched bytes.
func reuseContentTx(tx *sql.Tx, digest string, size int64) (bool, error) {
	var contentSize int64
	err := tx.QueryRow(
		`SELECT size FROM attachment_contents WHERE sha256 = ?`, digest,
	).Scan(&contentSize)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	case contentSize != size:
		return false, ErrAttachmentDigestConflict
	default:
		return true, nil
	}
}

// CompleteAttachment seals an upload: every declared chunk must be present
// and their lengths must add up to the declared total, after which the
// concatenated content must hash to the declared digest.
//
//   - Unknown attachment: ErrAttachmentNotFound; non-creator device:
//     ErrAttachmentForbidden.
//   - Missing chunks or a total-length mismatch: ErrAttachmentIncomplete; the
//     upload stays resumable.
//   - Digest mismatch: ErrAttachmentDigestMismatch; the upload stays
//     incomplete.
//   - A completed content with the same digest but a different size:
//     ErrAttachmentDigestConflict; the upload stays resumable.
//   - A completed content with the same digest and size is reused: the bytes
//     are not copied and the result carries Reused=true.
//
// A successful finish is persisted with its result, so repeating the call
// returns the same result without recomputing anything.
func (s *Store) CompleteAttachment(deviceID, attachmentID string) (CompleteResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return CompleteResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	a, err := getAttachmentTx(tx, attachmentID, deviceID)
	if err != nil {
		return CompleteResult{}, err
	}
	if a.Complete {
		// A repeat finish returns the recorded result unchanged.
		return CompleteResult{
			ID:       a.ID,
			Size:     a.TotalBytes,
			SHA256:   a.SHA256,
			Complete: true,
			Reused:   a.Reused,
		}, tx.Commit()
	}

	rows, err := tx.Query(
		`SELECT idx, data FROM attachment_chunks WHERE attachment_id = ? ORDER BY idx ASC`,
		attachmentID,
	)
	if err != nil {
		return CompleteResult{}, err
	}
	var content []byte
	var received int64
	var total int64
	var prevIndex int64 = -1
	complete := true
	for rows.Next() {
		var idx int64
		var data []byte
		if err := rows.Scan(&idx, &data); err != nil {
			_ = rows.Close()
			return CompleteResult{}, err
		}
		if idx != prevIndex+1 {
			complete = false
		}
		prevIndex = idx
		received++
		total += int64(len(data))
		content = append(content, data...)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return CompleteResult{}, err
	}
	_ = rows.Close()

	if !complete || received != a.chunkCount() || total != a.TotalBytes {
		return CompleteResult{}, ErrAttachmentIncomplete
	}

	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != a.SHA256 {
		return CompleteResult{}, ErrAttachmentDigestMismatch
	}

	// Content is addressed by digest: an existing row means another finished
	// attachment already stored these bytes.
	reused, err := reuseContentTx(tx, a.SHA256, a.TotalBytes)
	if err != nil {
		return CompleteResult{}, err
	}
	if !reused {
		if _, err := tx.Exec(
			`INSERT INTO attachment_contents (sha256, size, data) VALUES (?, ?, ?)`,
			a.SHA256, a.TotalBytes, content,
		); err != nil {
			return CompleteResult{}, err
		}
	}

	if _, err := tx.Exec(
		`UPDATE attachments SET complete = 1, reused = ? WHERE id = ?`,
		boolToInt(reused), attachmentID,
	); err != nil {
		return CompleteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CompleteResult{}, err
	}
	return CompleteResult{
		ID:       a.ID,
		Size:     a.TotalBytes,
		SHA256:   a.SHA256,
		Complete: true,
		Reused:   reused,
	}, nil
}

// GetAttachment returns the stored metadata of an upload together with the
// sorted indices of the chunks received so far. An unknown id yields
// ErrAttachmentNotFound; a non-creator device yields ErrAttachmentForbidden.
func (s *Store) GetAttachment(deviceID, attachmentID string) (Attachment, []int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Attachment{}, nil, err
	}
	defer func() { _ = tx.Rollback() }()

	a, err := getAttachmentTx(tx, attachmentID, deviceID)
	if err != nil {
		return Attachment{}, nil, err
	}

	rows, err := tx.Query(
		`SELECT idx FROM attachment_chunks WHERE attachment_id = ? ORDER BY idx ASC`,
		attachmentID,
	)
	if err != nil {
		return Attachment{}, nil, err
	}
	indices := make([]int64, 0)
	for rows.Next() {
		var idx int64
		if err := rows.Scan(&idx); err != nil {
			_ = rows.Close()
			return Attachment{}, nil, err
		}
		indices = append(indices, idx)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return Attachment{}, nil, err
	}
	_ = rows.Close()

	return a, indices, tx.Commit()
}

// GetAttachmentChunk returns the bytes stored at index. An unknown attachment
// yields ErrAttachmentNotFound and a non-creator device ErrAttachmentForbidden.
// For a known attachment, a negative index or one beyond the declared chunk
// range yields *ErrChunkInvalid (the caller maps it to 400); an in-range index
// with no stored chunk yields ErrChunkNotFound (404).
func (s *Store) GetAttachmentChunk(deviceID, attachmentID string, index int64) ([]byte, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	a, err := getAttachmentTx(tx, attachmentID, deviceID)
	if err != nil {
		return nil, err
	}
	// The path index is already a non-negative decimal; reject one outside the
	// shape the creator declared so out-of-range reads fail as a 400 rather than
	// masquerading as a missing chunk (404).
	if index < 0 || index >= a.chunkCount() {
		return nil, &ErrChunkInvalid{Reason: fmt.Sprintf("chunk index %d is out of range [0, %d)", index, a.chunkCount())}
	}

	var data []byte
	err = tx.QueryRow(
		`SELECT data FROM attachment_chunks WHERE attachment_id = ? AND idx = ?`,
		attachmentID, index,
	).Scan(&data)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrChunkNotFound
	case err != nil:
		return nil, err
	}
	return data, tx.Commit()
}

func bytesEqual(a, b []byte) bool {
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

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
