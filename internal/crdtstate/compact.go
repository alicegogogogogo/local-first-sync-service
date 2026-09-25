package crdtstate

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// Compact trims every stored operation and tombstone that no longer affects
// the document's merged result, in one serialized transaction, and returns the
// post-compaction snapshot (byte-for-byte what a snapshot read returns
// immediately afterward).
//
// The gate is enforced exactly as in Submit and before any CRDT content is
// observed: an unregistered device and a revoked device each fail with the
// gate's sentinel error, and neither touches any state. A document with no
// committed CRDT operation yields ErrNotFound. Any failure leaves the state
// untouched.
//
// Per type, compaction keeps precisely the rows the merge and the submission
// checks still read:
//
//   - counter: each device's maximum contribution (ties keep the
//     lexicographically smaller operation id); superseded contributions are
//     dropped. The per-device maxima table is untouched, so monotonicity
//     checks and the merged sum are unchanged. Each dropped id keeps a
//     device-plus-value identity.
//   - gset: a minimal prefix cover of the union — operations are considered
//     in ascending id order and one is kept only when it contributes an
//     element no previously kept operation covers. The union table is
//     untouched, so the merged set is unchanged. Each dropped id keeps a
//     device-plus-elements identity.
//   - register: each device's greatest-version operation (which includes the
//     document's winner). Regression checks derive per-device maxima from
//     these rows, so later version checks and the winning value are
//     unchanged. Each dropped older id keeps a device-plus-version-plus-value
//     identity.
//   - orset: live add tags. Tombstoned tags are dropped together with the
//     tombstones covering them — a dead tag and its tombstone cancel out of
//     the merge — so the live element set is unchanged. The operation log is
//     untouched (adds and removes alike), so replaying either one stays
//     idempotent and a replayed remove never tombstones tags that arrived
//     after it; no identity row is needed.
//
// Compaction is idempotent: a second run finds nothing to trim and returns
// the same snapshot. It commits no change-log row, allocates no cursor and,
// because the merged value never moves, notifies no subscriber.
func (s *Service) Compact(documentID, deviceID string) (Snapshot, error) {
	// Serialize with submissions and other compactions: the trim and the
	// submission checks read the same rows, so they must not interleave.
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := s.gateDevice(tx, documentID, deviceID); err != nil {
		return Snapshot{}, err
	}

	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Snapshot{}, ErrNotFound
	case err != nil:
		return Snapshot{}, err
	}

	switch docType {
	case TypeCounter:
		if err := compactCounterOps(tx, documentID); err != nil {
			return Snapshot{}, err
		}
	case TypeGSet:
		if err := compactGSetOps(tx, documentID); err != nil {
			return Snapshot{}, err
		}
	case TypeRegister:
		if err := compactRegisterOps(tx, documentID); err != nil {
			return Snapshot{}, err
		}
	case TypeORSet:
		if err := compactORSetOps(tx, documentID); err != nil {
			return Snapshot{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	// The commit is durable and the lock is still held, so no submission or
	// compaction can interleave before this read: the returned snapshot is
	// exactly what a snapshot read immediately afterward observes.
	return s.snapshotLocked(documentID)
}

// compactCounterOps drops every counter operation superseded by a greater (or
// equal but later-id) contribution from the same device, keeping exactly one
// operation per device: its maximum contribution, ties broken by the
// lexicographically smaller operation id. The per-device maxima table that
// drives the merge and the regression checks is untouched. Before a row is
// dropped its identity is retained, so a replay of the trimmed id stays
// idempotent and a mismatched replay stays a conflict.
func compactCounterOps(tx *sql.Tx, documentID string) error {
	rows, err := tx.Query(
		`SELECT id, device_id, value FROM crdt_ops WHERE document_id = ? ORDER BY id ASC`,
		documentID,
	)
	if err != nil {
		return err
	}
	type contribution struct {
		id    string
		value int64
		raw   []byte
	}
	best := make(map[string]contribution)
	var drop []Op
	for rows.Next() {
		var id, deviceID string
		var raw []byte
		if err := rows.Scan(&id, &deviceID, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		value, err := decodeCounterValue(raw)
		if err != nil {
			_ = rows.Close()
			return err
		}
		// Ids arrive in ascending order, so the first contribution seen at the
		// device's maximum value carries the smallest id; only a strictly
		// greater value replaces it.
		if cur, ok := best[deviceID]; !ok || value > cur.value {
			if ok {
				drop = append(drop, Op{ID: cur.id, DeviceID: deviceID, Value: json.RawMessage(cur.raw)})
			}
			best[deviceID] = contribution{id: id, value: value, raw: raw}
		} else {
			drop = append(drop, Op{ID: id, DeviceID: deviceID, Value: json.RawMessage(raw)})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, op := range drop {
		if err := retainOpIdentity(tx, documentID, TypeCounter, op); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`DELETE FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		); err != nil {
			return err
		}
	}
	return nil
}

// compactGSetOps drops every gset operation whose elements are all covered by
// kept operations: operations are considered in ascending id order and one is
// kept only when it contributes at least one element no previously kept
// operation covers. The union table that drives the merge is untouched, so
// the kept operations still reproduce the exact merged set. A dropped
// operation's identity is retained before its row is deleted, so a replay of
// the trimmed id stays idempotent and a mismatched replay stays a conflict.
func compactGSetOps(tx *sql.Tx, documentID string) error {
	rows, err := tx.Query(
		`SELECT id, device_id, value FROM crdt_ops WHERE document_id = ? ORDER BY id ASC`,
		documentID,
	)
	if err != nil {
		return err
	}
	covered := make(map[string]struct{})
	var drop []Op
	for rows.Next() {
		var id, deviceID string
		var raw []byte
		if err := rows.Scan(&id, &deviceID, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		var elements []string
		if err := json.Unmarshal(raw, &elements); err != nil {
			_ = rows.Close()
			return err
		}
		keeps := false
		for _, element := range elements {
			if _, ok := covered[element]; !ok {
				keeps = true
				covered[element] = struct{}{}
			}
		}
		if !keeps {
			drop = append(drop, Op{ID: id, DeviceID: deviceID, Elements: elements})
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, op := range drop {
		if err := retainOpIdentity(tx, documentID, TypeGSet, op); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`DELETE FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		); err != nil {
			return err
		}
	}
	return nil
}

// compactRegisterOps drops every register operation superseded by a greater
// version from the same device, keeping each device's greatest-version
// operation. Versions strictly increase per device, so exactly one row per
// device survives; the per-device maxima the regression checks derive from
// the table, and the document's winning value, are unchanged. A dropped
// operation's identity is retained before its row is deleted, so a replay of
// the trimmed id stays idempotent and a mismatched replay stays a conflict.
func compactRegisterOps(tx *sql.Tx, documentID string) error {
	rows, err := tx.Query(
		`SELECT id, device_id, version, value FROM crdt_register_ops
		 WHERE document_id = ?
		   AND EXISTS (
			SELECT 1 FROM crdt_register_ops newer
			WHERE newer.document_id = crdt_register_ops.document_id
			  AND newer.device_id = crdt_register_ops.device_id
			  AND newer.version > crdt_register_ops.version
		   )`,
		documentID,
	)
	if err != nil {
		return err
	}
	var drop []Op
	for rows.Next() {
		var op Op
		if err := rows.Scan(&op.ID, &op.DeviceID, &op.Version, &op.Value); err != nil {
			_ = rows.Close()
			return err
		}
		drop = append(drop, op)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, op := range drop {
		if err := retainOpIdentity(tx, documentID, TypeRegister, op); err != nil {
			return err
		}
		if _, err := tx.Exec(
			`DELETE FROM crdt_register_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		); err != nil {
			return err
		}
	}
	return nil
}

// compactORSetOps drops every tombstoned add tag together with every
// tombstone: a dead tag and the tombstone covering it cancel out of the
// merge, so the live element set is unchanged. The operation log is
// untouched, so replaying an add or a remove stays idempotent and a replayed
// remove never tombstones tags accepted after it.
func compactORSetOps(tx *sql.Tx, documentID string) error {
	if _, err := tx.Exec(
		`DELETE FROM crdt_orset_tags
		 WHERE document_id = ?
		   AND EXISTS (
			SELECT 1 FROM crdt_orset_tombstones x
			WHERE x.document_id = crdt_orset_tags.document_id
			  AND x.element = crdt_orset_tags.element
			  AND x.op_id = crdt_orset_tags.op_id
		   )`,
		documentID,
	); err != nil {
		return err
	}
	_, err := tx.Exec(
		`DELETE FROM crdt_orset_tombstones WHERE document_id = ?`,
		documentID,
	)
	return err
}
