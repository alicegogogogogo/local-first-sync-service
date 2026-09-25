// Operation fingerprints preserve the comparable identity of every accepted
// CRDT operation independently of the operation rows the merge reads.
// Compaction trims operation rows and tombstones that no longer affect the
// merged result, but the submission layer must still answer its two questions
// about a re-posted id afterward: is this the same operation again (same
// device, same compared content — an idempotent replay that reports
// created=false and applies nothing), or the same id carrying different
// content or a different origin (an ErrCRDTConflict that writes nothing)?
// A trimmed id must never look like a new submission.
//
// A fingerprint is the originating device plus a fixed-size canonical digest
// of exactly the content the type's idempotency rule compares:
//
//   - counter: the contribution value, canonicalized like jsonEqual compares
//     it (1 and 1.0 digest alike);
//   - gset: the set of elements, sorted and deduplicated (element order and
//     repeats are irrelevant to the comparison);
//   - register: the version and the canonicalized value;
//   - orset: the action and the element.
//
// The digest is a canonical summary, not the content itself, so the table
// stays small no matter how large the trimmed operations were: one narrow row
// per accepted id, never the trimmed rows again. Fingerprints serve only
// idempotency and conflict decisions — the merge never reads them, the
// snapshot counts never include them, and compaction never deletes them.

package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
)

// counterOpDigest digests the content a counter idempotency check compares:
// the canonical contribution value.
func counterOpDigest(value json.RawMessage) []byte {
	return crdtOpDigest([]byte("counter"), canonicalJSON(value))
}

// gsetOpDigest digests the content a gset idempotency check compares: the set
// of elements, order and duplicates normalized away.
func gsetOpDigest(elements []string) []byte {
	return crdtOpDigest([]byte("gset"), canonicalSetElements(elements))
}

// registerOpDigest digests the content a register idempotency check compares:
// the version and the canonical value.
func registerOpDigest(version int64, value json.RawMessage) []byte {
	return crdtOpDigest([]byte("register"), []byte(strconv.FormatInt(version, 10)), canonicalJSON(value))
}

// orsetOpDigest digests the content an orset idempotency check compares: the
// action and the element.
func orsetOpDigest(action, element string) []byte {
	return crdtOpDigest([]byte("orset"), []byte(action), []byte(element))
}

// crdtOpDigest hashes length-prefixed parts so no concatenation of shorter
// parts can collide with a longer one.
func crdtOpDigest(parts ...[]byte) []byte {
	h := sha256.New()
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(part)
	}
	return h.Sum(nil)
}

// canonicalJSON renders raw the way jsonEqual compares it: decoded and
// re-encoded, so 1 and 1.0, or {"a":1,"b":2} and {"b":2,"a":1}, canonicalize
// to the same bytes (the encoder sorts object keys and normalizes numbers).
// Invalid JSON — which validation never lets through — falls back to the raw
// bytes.
func canonicalJSON(raw json.RawMessage) []byte {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return raw
	}
	canonical, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return canonical
}

// canonicalSetElements renders the element set of a gset operation the way
// stringSetEqual compares it: sorted, duplicates removed.
func canonicalSetElements(elements []string) []byte {
	sorted := make([]string, 0, len(elements))
	seen := make(map[string]struct{}, len(elements))
	for _, element := range elements {
		if _, ok := seen[element]; ok {
			continue
		}
		seen[element] = struct{}{}
		sorted = append(sorted, element)
	}
	sort.Strings(sorted)
	raw, err := json.Marshal(sorted)
	if err != nil {
		// Strings always marshal.
		return []byte("[]")
	}
	return raw
}

// lookupCRDTOpFingerprint returns the stored device and content digest for an
// accepted operation id, or ok=false when the id was never accepted.
func lookupCRDTOpFingerprint(tx *sql.Tx, documentID, id string) (deviceID string, digest []byte, ok bool, err error) {
	err = tx.QueryRow(
		`SELECT device_id, digest FROM crdt_op_fingerprints WHERE document_id = ? AND id = ?`,
		documentID, id,
	).Scan(&deviceID, &digest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil, false, nil
	case err != nil:
		return "", nil, false, err
	default:
		return deviceID, digest, true, nil
	}
}

// recordCRDTOpFingerprint stores an accepted operation's comparable identity
// inside its commit transaction. Every accepted id is recorded exactly once;
// compaction never deletes the row.
func recordCRDTOpFingerprint(tx *sql.Tx, documentID, id, deviceID string, digest []byte) error {
	_, err := tx.Exec(
		`INSERT INTO crdt_op_fingerprints (document_id, id, device_id, digest) VALUES (?, ?, ?, ?)`,
		documentID, id, deviceID, digest,
	)
	return err
}

// backfillCRDTFingerprints records fingerprints for every stored operation
// that predates the fingerprint table (a database written before it existed),
// so the invariant "every accepted operation has a fingerprint" holds before
// any submission or compaction runs. It is idempotent — a no-op once every
// operation is covered.
func (s *Store) backfillCRDTFingerprints() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Counter, gset and orset operations share crdt_ops; the document's fixed
	// type selects how the stored content digests.
	rows, err := tx.Query(
		`SELECT o.document_id, o.id, o.device_id, o.value, d.type
		 FROM crdt_ops o
		 JOIN crdt_documents d ON d.document_id = o.document_id
		 WHERE NOT EXISTS (
			SELECT 1 FROM crdt_op_fingerprints f
			WHERE f.document_id = o.document_id AND f.id = o.id
		 )`,
	)
	if err != nil {
		return err
	}
	type opRow struct {
		documentID, id, deviceID string
		digest                   []byte
	}
	var pending []opRow
	for rows.Next() {
		var documentID, id, deviceID, docType string
		var raw []byte
		if err := rows.Scan(&documentID, &id, &deviceID, &raw, &docType); err != nil {
			_ = rows.Close()
			return err
		}
		var digest []byte
		switch docType {
		case CRDTTypeCounter:
			digest = counterOpDigest(raw)
		case CRDTTypeGSet:
			var elements []string
			if err := json.Unmarshal(raw, &elements); err != nil {
				_ = rows.Close()
				return err
			}
			digest = gsetOpDigest(elements)
		case CRDTTypeORSet:
			var content struct {
				Action  string `json:"action"`
				Element string `json:"element"`
			}
			if err := json.Unmarshal(raw, &content); err != nil {
				_ = rows.Close()
				return err
			}
			digest = orsetOpDigest(content.Action, content.Element)
		default:
			continue
		}
		pending = append(pending, opRow{documentID, id, deviceID, digest})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	registerRows, err := tx.Query(
		`SELECT r.document_id, r.id, r.device_id, r.version, r.value
		 FROM crdt_register_ops r
		 WHERE NOT EXISTS (
			SELECT 1 FROM crdt_op_fingerprints f
			WHERE f.document_id = r.document_id AND f.id = r.id
		 )`,
	)
	if err != nil {
		return err
	}
	for registerRows.Next() {
		var documentID, id, deviceID string
		var version int64
		var raw []byte
		if err := registerRows.Scan(&documentID, &id, &deviceID, &version, &raw); err != nil {
			_ = registerRows.Close()
			return err
		}
		pending = append(pending, opRow{documentID, id, deviceID, registerOpDigest(version, raw)})
	}
	if err := registerRows.Err(); err != nil {
		_ = registerRows.Close()
		return err
	}
	_ = registerRows.Close()

	for _, op := range pending {
		if _, err := tx.Exec(
			`INSERT OR IGNORE INTO crdt_op_fingerprints (document_id, id, device_id, digest) VALUES (?, ?, ?, ?)`,
			op.documentID, op.id, op.deviceID, op.digest,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}
