// Resumable, content-addressed attachments owned by registered devices.
//
// An attachment is created up front with its full declared metadata — a
// client-supplied id, total size, chunk size and lowercase-hex SHA-256 digest
// — after which its bytes arrive as numbered binary chunks. Chunk indices are
// zero-based and may be posted out of order or retried: re-posting a chunk
// with identical bytes is idempotent, while different bytes are a conflict and
// the first bytes win. Nothing is sealed until complete: only when every chunk
// is present and their concatenated length equals the declared size are the
// bytes assembled in order and checked against the declared digest. A missing
// chunk or a length mismatch leaves the upload resumable (409); a digest
// mismatch leaves it unsealed as well (422).
//
// Sealed content is stored once, in a blobs table keyed by its SHA-256 and
// size. When a newly completed attachment has the same digest AND size as an
// already sealed one, it points at the existing blob and copies no bytes; the
// completion reports the reuse. Same digest with a different size is a
// conflict, never a reuse. Completion is idempotent: repeating it returns the
// same result.
//
// Every state transition runs in one serialized transaction and is committed
// to disk before the call returns, so received chunks, ownership, idempotency
// and dedup decisions all survive a restart.

package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
)

// ErrAttachmentNotFound reports that no attachment with the id exists (for the
// requesting device's view). The caller maps it to 404; nothing is written.
var ErrAttachmentNotFound = errors.New("attachment not found")

// ErrAttachmentForbidden reports that the attachment exists but belongs to a
// different device. The caller maps it to 403; nothing is written.
var ErrAttachmentForbidden = errors.New("attachment belongs to another device")

// ErrAttachmentConflict reports that an attachment id already belongs to a
// different device. The existing record is left unchanged. The caller maps it
// to 409.
type ErrAttachmentConflict struct {
	ID string
}

func (e *ErrAttachmentConflict) Error() string {
	return fmt.Sprintf("attachment %q belongs to another device", e.ID)
}

// ErrChunkConflict reports that a chunk was already stored with different
// bytes. The first bytes are kept. The caller maps it to 409.
type ErrChunkConflict struct {
	Index int64
}

func (e *ErrChunkConflict) Error() string {
	return fmt.Sprintf("chunk %d already exists with different bytes", e.Index)
}

// ErrAttachmentBadChunk reports a malformed chunk request: an out-of-range
// index or a body whose length does not match the chunk's declared length. The
// caller maps it to 400; nothing is written.
var ErrAttachmentBadChunk = errors.New("invalid chunk index or length")

// ErrAttachmentIncomplete reports that completion was requested while chunks
// are missing or their concatenated length differs from the declared size.
// The upload stays open and resumable. The caller maps it to 409.
var ErrAttachmentIncomplete = errors.New("attachment chunks are missing or the total length does not match")

// ErrAttachmentDigest reports that the assembled bytes do not hash to the
// declared SHA-256 digest. The attachment is left unsealed so chunks can be
// inspected and the upload retried. The caller maps it to 422.
var ErrAttachmentDigest = errors.New("assembled content does not match the declared SHA-256 digest")

// ErrAttachmentContentConflict reports that the assembled content has the
// declared digest but a size different from an already sealed blob with that
// digest, so it cannot be deduplicated. The current upload stays resumable.
// The caller maps it to 409.
var ErrAttachmentContentConflict = errors.New("digest matches existing content with a different size")

// AttachmentMeta describes one attachment and its upload progress.
type AttachmentMeta struct {
	ID        string  `json:"attachmentId"`
	DeviceID  string  `json:"deviceId"`
	Size      int64   `json:"size"`
	ChunkSize int64   `json:"chunkSize"`
	SHA256    string  `json:"sha256"`
	Completed bool    `json:"completed"`
	Received  []int64 `json:"received"`
}

// AttachmentCompletion is the result of a successful CompleteAttachment call.
// Reused is true when the sealed bytes were shared with a pre-existing
// attachment of the same digest and size; ReusedFrom then names it.
type AttachmentCompletion struct {
	ID         string `json:"attachmentId"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Completed  bool   `json:"completed"`
	Reused     bool   `json:"reused"`
	ReusedFrom string `json:"reusedFrom,omitempty"`
}

// CreateAttachment registers the metadata for a new upload owned by deviceID.
//
//   - An unregistered device yields ErrDeviceNotFound; nothing is written.
//   - An id already owned by the same device with identical metadata is an
//     idempotent re-post: created=false.
//   - An id owned by another device yields *ErrAttachmentConflict, even when
//     the metadata matches; the original record is untouched.
//
// Fields are validated by the caller.
func (s *Store) CreateAttachment(deviceID, id string, size, chunkSize int64, digest string) (created bool, err error) {
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
	var existingSize, existingChunk int64
	var existingDigest string
	err = tx.QueryRow(
		`SELECT device_id, size, chunk_size, sha256 FROM attachments WHERE id = ?`, id,
	).Scan(&owner, &existingSize, &existingChunk, &existingDigest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO attachments (id, device_id, size, chunk_size, sha256, status)
			 VALUES (?, ?, ?, ?, ?, 'pending')`,
			id, deviceID, size, chunkSize, digest,
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

	if owner != deviceID {
		return false, &ErrAttachmentConflict{ID: id}
	}
	if existingSize != size || existingChunk != chunkSize || existingDigest != digest {
		// Same owner, different metadata: treat the id as occupied as well; the
		// first declaration wins and is never rewritten.
		return false, &ErrAttachmentConflict{ID: id}
	}
	return false, tx.Commit()
}

// PutChunk stores one binary chunk of an attachment.
//
// The attachment must exist and belong to deviceID (ErrAttachmentNotFound /
// ErrAttachmentForbidden). index is zero-based and must be below the number of
// chunks the declared size implies; the body length must equal the chunk's
// expected length — chunkSize for every chunk except the last, which takes the
// remainder. Any violation yields ErrAttachmentBadChunk and writes nothing.
// Re-posting the same index with identical bytes is a no-op; different bytes
// yield *ErrChunkConflict and the first bytes are retained.
func (s *Store) PutChunk(deviceID, id string, index int64, data []byte) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var owner string
	var size, chunkSize int64
	err = tx.QueryRow(
		`SELECT device_id, size, chunk_size FROM attachments WHERE id = ?`, id,
	).Scan(&owner, &size, &chunkSize)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrAttachmentNotFound
	case err != nil:
		return err
	}
	if owner != deviceID {
		return ErrAttachmentForbidden
	}

	totalChunks := chunkCount(size, chunkSize)
	if index < 0 || index >= totalChunks {
		return ErrAttachmentBadChunk
	}
	expected := chunkSize
	if index == totalChunks-1 {
		expected = size - (totalChunks-1)*chunkSize
	}
	if int64(len(data)) != expected {
		return ErrAttachmentBadChunk
	}

	var existing []byte
	err = tx.QueryRow(
		`SELECT data FROM attachment_chunks WHERE attachment_id = ? AND chunk_index = ?`,
		id, index,
	).Scan(&existing)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO attachment_chunks (attachment_id, chunk_index, data) VALUES (?, ?, ?)`,
			id, index, data,
		); err != nil {
			return err
		}
		return tx.Commit()
	case err != nil:
		return err
	default:
		if !bytesEqual(existing, data) {
			return &ErrChunkConflict{Index: index}
		}
		// Identical re-post: idempotent no-op.
		return tx.Commit()
	}
}

// CompleteAttachment seals an upload owned by deviceID.
//
// It requires every declared chunk to be present with the declared lengths;
// otherwise ErrAttachmentIncomplete is returned and the upload stays open. The
// chunks are then concatenated in order and hashed: a mismatch with the
// declared digest is ErrAttachmentDigest and the attachment stays unsealed.
// When the digest is valid but matches a sealed blob of a different size,
// ErrAttachmentContentConflict is returned and the upload remains resumable.
// A digest+size match reuses the existing blob without copying bytes.
// Repeating completion after success returns the same result.
func (s *Store) CompleteAttachment(deviceID, id string) (AttachmentCompletion, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return AttachmentCompletion{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var owner string
	var size, chunkSize int64
	var digest, status string
	var contentHash, reusedFrom sql.NullString
	err = tx.QueryRow(
		`SELECT device_id, size, chunk_size, sha256, status, content_hash, reused_from
		 FROM attachments WHERE id = ?`, id,
	).Scan(&owner, &size, &chunkSize, &digest, &status, &contentHash, &reusedFrom)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return AttachmentCompletion{}, ErrAttachmentNotFound
	case err != nil:
		return AttachmentCompletion{}, err
	}
	if owner != deviceID {
		return AttachmentCompletion{}, ErrAttachmentForbidden
	}

	completion := AttachmentCompletion{
		ID:        id,
		Size:      size,
		SHA256:    digest,
		Completed: true,
	}

	if status == "completed" {
		// Idempotent repeat: report the recorded outcome.
		if reusedFrom.Valid {
			completion.Reused = true
			completion.ReusedFrom = reusedFrom.String
		}
		return completion, tx.Commit()
	}

	totalChunks := chunkCount(size, chunkSize)
	var received int
	var receivedBytes int64
	err = tx.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(LENGTH(data)), 0) FROM attachment_chunks WHERE attachment_id = ?`,
		id,
	).Scan(&received, &receivedBytes)
	if err != nil {
		return AttachmentCompletion{}, err
	}
	if int64(received) != totalChunks || receivedBytes != size {
		return AttachmentCompletion{}, ErrAttachmentIncomplete
	}

	// Assemble in index order, then verify the digest over the exact bytes that
	// would be sealed.
	assembled, err := s.assembleLocked(tx, id)
	if err != nil {
		return AttachmentCompletion{}, err
	}
	sum := sha256.Sum256(assembled)
	if hex.EncodeToString(sum[:]) != digest {
		return AttachmentCompletion{}, ErrAttachmentDigest
	}

	// Dedup against already sealed content. A blob is keyed by its digest; a
	// matching digest with the same size shares the bytes, while a different
	// size is a conflict and the upload is left open.
	var blobSize int64
	var blobSource string
	err = tx.QueryRow(
		`SELECT b.size, a.id
		 FROM attachment_blobs b
		 JOIN attachments a ON a.content_hash = b.sha256
		 WHERE b.sha256 = ? AND a.status = 'completed'
		 ORDER BY CASE WHEN b.size = ? THEN 0 ELSE 1 END, a.completed_at ASC, a.id ASC
		 LIMIT 1`,
		digest, size,
	).Scan(&blobSize, &blobSource)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No sealed content with this digest: store the assembled bytes once.
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO attachment_blobs (sha256, size, data) VALUES (?, ?, ?)`,
			digest, size, assembled,
		); err != nil {
			return AttachmentCompletion{}, err
		}
		if _, err := tx.Exec(
			`UPDATE attachments
			 SET status = 'completed', content_hash = ?, reused_from = NULL, completed_at = CURRENT_TIMESTAMP
			 WHERE id = ?`,
			digest, id,
		); err != nil {
			return AttachmentCompletion{}, err
		}
	case err != nil:
		return AttachmentCompletion{}, err
	default:
		if blobSize != size {
			// Same digest, different size: never reuse, leave the upload open.
			return AttachmentCompletion{}, ErrAttachmentContentConflict
		}
		if _, err := tx.Exec(
			`UPDATE attachments
			 SET status = 'completed', content_hash = ?, reused_from = ?, completed_at = CURRENT_TIMESTAMP
			 WHERE id = ?`,
			digest, blobSource, id,
		); err != nil {
			return AttachmentCompletion{}, err
		}
		completion.Reused = true
		completion.ReusedFrom = blobSource
	}

	if err := tx.Commit(); err != nil {
		return AttachmentCompletion{}, err
	}
	return completion, nil
}

// assembleLocked concatenates all chunks of id in index order. The chunk
// coverage and total length have already been verified by the caller.
func (s *Store) assembleLocked(tx *sql.Tx, id string) ([]byte, error) {
	rows, err := tx.Query(
		`SELECT data, LENGTH(data) FROM attachment_chunks WHERE attachment_id = ? ORDER BY chunk_index ASC`,
		id,
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var chunks [][]byte
	var total int64
	for rows.Next() {
		var data []byte
		var n int64
		if err := rows.Scan(&data, &n); err != nil {
			return nil, err
		}
		chunks = append(chunks, data)
		total += n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out, nil
}

// GetAttachmentMeta returns an attachment's metadata and the sorted indices of
// chunks received so far. An unknown id yields ErrAttachmentNotFound; an
// attachment owned by another device yields ErrAttachmentForbidden.
func (s *Store) GetAttachmentMeta(deviceID, id string) (AttachmentMeta, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return AttachmentMeta{}, err
	}
	defer func() { _ = tx.Rollback() }()

	meta, err := s.loadAttachmentMeta(tx, deviceID, id)
	if err != nil {
		return AttachmentMeta{}, err
	}
	return meta, tx.Commit()
}

func (s *Store) loadAttachmentMeta(q queryRower, deviceID, id string) (AttachmentMeta, error) {
	var owner string
	var size, chunkSize int64
	var digest, status string
	err := q.QueryRow(
		`SELECT device_id, size, chunk_size, sha256, status FROM attachments WHERE id = ?`, id,
	).Scan(&owner, &size, &chunkSize, &digest, &status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return AttachmentMeta{}, ErrAttachmentNotFound
	case err != nil:
		return AttachmentMeta{}, err
	}
	if owner != deviceID {
		return AttachmentMeta{}, ErrAttachmentForbidden
	}

	rows, err := q.Query(
		`SELECT chunk_index FROM attachment_chunks WHERE attachment_id = ? ORDER BY chunk_index ASC`,
		id,
	)
	if err != nil {
		return AttachmentMeta{}, err
	}
	defer func() { _ = rows.Close() }()
	received := make([]int64, 0)
	for rows.Next() {
		var idx int64
		if err := rows.Scan(&idx); err != nil {
			return AttachmentMeta{}, err
		}
		received = append(received, idx)
	}
	if err := rows.Err(); err != nil {
		return AttachmentMeta{}, err
	}

	return AttachmentMeta{
		ID:        id,
		DeviceID:  owner,
		Size:      size,
		ChunkSize: chunkSize,
		SHA256:    digest,
		Completed: status == "completed",
		Received:  received,
	}, nil
}

// GetAttachmentChunk returns the stored bytes of one chunk. An unknown
// attachment yields ErrAttachmentNotFound and one owned by another device
// ErrAttachmentForbidden. An index outside the declared range yields
// ErrAttachmentBadChunk; an in-range chunk that has not been uploaded (or kept)
// yields ErrAttachmentNotFound.
func (s *Store) GetAttachmentChunk(deviceID, id string, index int64) ([]byte, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	var owner string
	var size, chunkSize int64
	err = tx.QueryRow(
		`SELECT device_id, size, chunk_size FROM attachments WHERE id = ?`, id,
	).Scan(&owner, &size, &chunkSize)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrAttachmentNotFound
	case err != nil:
		return nil, err
	}
	if owner != deviceID {
		return nil, ErrAttachmentForbidden
	}
	if index < 0 || index >= chunkCount(size, chunkSize) {
		return nil, ErrAttachmentBadChunk
	}

	var data []byte
	err = tx.QueryRow(
		`SELECT data FROM attachment_chunks WHERE attachment_id = ? AND chunk_index = ?`,
		id, index,
	).Scan(&data)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrAttachmentNotFound
	case err != nil:
		return nil, err
	default:
		return data, tx.Commit()
	}
}

// queryRower is the subset of *sql.Tx/*sql.DB used by the read helpers.
type queryRower interface {
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
}

// chunkCount returns the number of chunks an attachment of size bytes with the
// given (positive) chunk size implies.
func chunkCount(size, chunkSize int64) int64 {
	return (size + chunkSize - 1) / chunkSize
}

// bytesEqual compares two byte slices without pulling in bytes at call sites.
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
