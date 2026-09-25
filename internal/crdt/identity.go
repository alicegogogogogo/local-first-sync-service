// Accepted-operation identities keep idempotency and conflict decisions
// working after compaction. Compaction trims the rows a merge derives its
// result from, but a re-submitted operation id must still be recognized. When
// a row is trimmed, compaction moves the only information that decision
// compares — the originating device and a digest of the type-specific
// comparison content — into crdt_op_identities. Untrimmed ids keep answering
// from their merge row, so the table holds one small fixed-size record per
// trimmed id, never one per accepted operation.
//
// The table never participates in a merge: the counter sum, the set union,
// the register winner and the live orset tags are all derived without it.

package crdt

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
)

// canonicalDigest renders the type-specific comparison content of op in a
// single canonical form and hashes it, prefixed by the type so contents of
// different types never collide. Two submissions of one id carry the same
// digest exactly when the submission layer's existing equality rules accept
// the second as idempotent:
//
//   - counter: the decoded non-negative integer (client formatting ignored);
//   - gset: the element set (order and duplicates ignored), rendered as a
//     sorted unique JSON array;
//   - register: the version together with the JSON-semantic value (1 and 1.0
//     and reordered object keys compare equal);
//   - orset: the action and the element.
func canonicalDigest(docType string, op Op) ([]byte, error) {
	var content []byte
	switch docType {
	case TypeCounter:
		n, err := decodeCounterValue(op.Value)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(n)
		if err != nil {
			return nil, err
		}
		content = raw
	case TypeGSet:
		set := make(map[string]struct{}, len(op.Elements))
		for _, element := range op.Elements {
			set[element] = struct{}{}
		}
		elements := make([]string, 0, len(set))
		for element := range set {
			elements = append(elements, element)
		}
		sort.Strings(elements)
		raw, err := json.Marshal(elements)
		if err != nil {
			return nil, err
		}
		content = raw
	case TypeRegister:
		if op.Version < 0 {
			return nil, errors.New("register version must be a non-negative integer")
		}
		var value any
		if err := json.Unmarshal(op.Value, &value); err != nil {
			return nil, err
		}
		// Re-marshaling the decoded value canonicalizes whitespace and object
		// key order exactly the way JSONEqual compares values; wrapping it in
		// a fixed-field struct folds the version into the same canonical bytes.
		valueBytes, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(struct {
			Version int64           `json:"version"`
			Value   json.RawMessage `json:"value"`
		}{Version: op.Version, Value: json.RawMessage(valueBytes)})
		if err != nil {
			return nil, err
		}
		content = raw
	case TypeORSet:
		if op.Action != ORSetAdd && op.Action != ORSetRemove {
			return nil, errors.New(`orset action must be "add" or "remove"`)
		}
		content = []byte(encodeORSetContent(op.Action, op.Element))
	default:
		return nil, errors.New("unknown crdt type")
	}

	h := sha256.New()
	_, _ = h.Write([]byte(docType))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(content)
	return h.Sum(nil), nil
}

// resolveTrimmedIdentity answers the submission question for an id whose
// merge row is absent because compaction trimmed it. It returns true when the
// retained identity matches op's originating device and comparison content
// (an idempotent replay), an *ErrConflict when a retained identity exists but
// differs, and false with no error when no identity was retained (a genuinely
// new id, which proceeds through the normal acceptance path).
func resolveTrimmedIdentity(tx *sql.Tx, documentID, docType, mismatchReason string, op Op) (bool, error) {
	digest, err := canonicalDigest(docType, op)
	if err != nil {
		return false, &ErrConflict{ID: op.ID, Reason: err.Error()}
	}

	var existingDevice string
	var existingDigest []byte
	err = tx.QueryRow(
		`SELECT device_id, digest FROM crdt_op_identities WHERE document_id = ? AND id = ?`,
		documentID, op.ID,
	).Scan(&existingDevice, &existingDigest)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	}

	if existingDevice != op.DeviceID || !bytes.Equal(existingDigest, digest) {
		return false, &ErrConflict{ID: op.ID, Reason: mismatchReason}
	}
	return true, nil
}

// retainIdentity moves the comparable identity (device plus content digest) of
// one operation row compaction is about to drop into crdt_op_identities. A
// row is dropped at most once so the id is absent, but INSERT OR IGNORE keeps
// a repeat compaction finding the same row a harmless no-op.
func retainIdentity(tx *sql.Tx, documentID, docType string, op Op) error {
	digest, err := canonicalDigest(docType, op)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT OR IGNORE INTO crdt_op_identities (document_id, id, device_id, digest) VALUES (?, ?, ?, ?)`,
		documentID, op.ID, op.DeviceID, digest,
	)
	return err
}
