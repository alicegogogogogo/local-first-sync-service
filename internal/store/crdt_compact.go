// CRDT compaction and snapshot reads bound the storage a long-lived document
// accumulates in the CRDT state layer. Merges only ever need a small part of
// the accepted history: a counter merges from each device's maximum
// contribution, a gset from the element union, a register from each device's
// latest version and the winner among them, and an orset from its live tags
// and the tombstones covering dead ones. Everything else — superseded counter
// contributions, gset operations whose elements later operations also carry,
// older register versions, tombstoned orset tags and the tombstones covering
// them — no longer affects the merged result and can be trimmed.
//
// Compaction rewrites nothing outside the CRDT tables: it never allocates a
// change cursor, never writes a change record and never notifies subscribers,
// because the merged value does not move. The trimmed state still reproduces
// the exact merge and still answers the submission layer's questions — the
// per-device counter maxima and register versions that gate regressions live
// in the retained rows — so merge results, idempotency decisions and conflict
// responses are computed by the same rules afterward. A snapshot read reports
// the merged value together with the number of stored operations and
// tombstones, so a client can observe what compaction trimmed; both counts
// are non-negative and durable across restarts.

package store

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// CRDTSnapshot is a document's merged CRDT state together with the storage
// footprint of that state: Operations counts the stored operations the merge
// is derived from (counter and gset: rows of the operation log; register: the
// retained per-device operations; orset: the add tags, live or tombstoned)
// and Tombstones counts the stored orset tombstones (always zero for the
// other types). Both counts are non-negative; compaction drives them down to
// the operations and tombstones that still participate in the merge.
type CRDTSnapshot struct {
	Type       string          // the document's fixed CRDT type
	Value      json.RawMessage // the merged value, exactly as GetCRDTState derives it
	Operations int64           // stored operations the merge is derived from
	Tombstones int64           // stored orset tombstones; 0 for counter, gset and register
}

// CompactCRDT trims every stored operation and tombstone that no longer
// affects the document's merged result, in one serialized transaction, and
// returns the post-compaction snapshot (byte-for-byte what a snapshot read
// returns immediately afterward).
//
// The registration/permission layer is enforced exactly as in SubmitCRDTOps
// and before any CRDT content is observed: an unregistered device yields
// ErrDeviceNotFound and a revoked device yields ErrPermissionDenied, and
// neither touches any state. A document with no committed CRDT operation
// yields ErrCRDTNotFound. Any failure leaves the state untouched.
//
// Per type, compaction keeps precisely the rows the merge and the submission
// checks still read:
//
//   - counter: each device's maximum contribution (ties keep the
//     lexicographically smaller operation id); superseded contributions are
//     dropped. The per-device maxima table is untouched, so monotonicity
//     checks and the merged sum are unchanged.
//   - gset: a minimal prefix cover of the union — operations are considered
//     in ascending id order and one is kept only when it contributes an
//     element no previously kept operation covers. The union table is
//     untouched, so the merged set is unchanged.
//   - register: each device's greatest-version operation (which includes the
//     document's winner). Regression checks derive per-device maxima from
//     these rows, so later version checks and the winning value are
//     unchanged.
//   - orset: live add tags. Tombstoned tags are dropped together with the
//     tombstones covering them — a dead tag and its tombstone cancel out of
//     the merge — so the live element set is unchanged. The operation log is
//     untouched, so replaying an add or remove stays idempotent and a
//     replayed remove never tombstones tags that arrived after it.
//
// Compaction is idempotent: a second run finds nothing to trim and returns
// the same snapshot. It commits no change-log row, allocates no cursor and,
// because the merged value never moves, notifies no subscriber.
func (s *Store) CompactCRDT(documentID, deviceID string) (CRDTSnapshot, error) {
	// Serialize with submissions and other compactions: the trim and the
	// submission checks read the same rows, so they must not interleave.
	s.crdtMu.Lock()
	defer s.crdtMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return CRDTSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()

	// Registration and permission are enforced before any CRDT content is
	// observed, so a rejected compaction cannot reveal type or state.
	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return CRDTSnapshot{}, err
	}
	if !deviceExists {
		return CRDTSnapshot{}, ErrDeviceNotFound
	}
	var storedAuth int
	err = tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&storedAuth)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
	case err != nil:
		return CRDTSnapshot{}, err
	default:
		if storedAuth == 0 {
			return CRDTSnapshot{}, ErrPermissionDenied
		}
	}

	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return CRDTSnapshot{}, ErrCRDTNotFound
	case err != nil:
		return CRDTSnapshot{}, err
	}

	switch docType {
	case CRDTTypeCounter:
		if err := compactCounterOps(tx, documentID); err != nil {
			return CRDTSnapshot{}, err
		}
	case CRDTTypeGSet:
		if err := compactGSetOps(tx, documentID); err != nil {
			return CRDTSnapshot{}, err
		}
	case CRDTTypeRegister:
		if err := compactRegisterOps(tx, documentID); err != nil {
			return CRDTSnapshot{}, err
		}
	case CRDTTypeORSet:
		if err := compactORSetOps(tx, documentID); err != nil {
			return CRDTSnapshot{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return CRDTSnapshot{}, err
	}
	// The commit is durable and crdtMu is still held, so no submission or
	// compaction can interleave before this read: the returned snapshot is
	// exactly what a snapshot read immediately afterward observes.
	return s.GetCRDTSnapshot(documentID)
}

// compactCounterOps drops every counter operation superseded by a greater (or
// equal but later-id) contribution from the same device, keeping exactly one
// operation per device: its maximum contribution, ties broken by the
// lexicographically smaller operation id. The per-device maxima table that
// drives the merge and the regression checks is untouched.
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
	}
	best := make(map[string]contribution)
	var drop []string
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
				drop = append(drop, cur.id)
			}
			best[deviceID] = contribution{id: id, value: value}
		} else {
			drop = append(drop, id)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, id := range drop {
		if _, err := tx.Exec(
			`DELETE FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, id,
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
// the kept operations still reproduce the exact merged set.
func compactGSetOps(tx *sql.Tx, documentID string) error {
	rows, err := tx.Query(
		`SELECT id, value FROM crdt_ops WHERE document_id = ? ORDER BY id ASC`,
		documentID,
	)
	if err != nil {
		return err
	}
	covered := make(map[string]struct{})
	var drop []string
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
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
			drop = append(drop, id)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	_ = rows.Close()

	for _, id := range drop {
		if _, err := tx.Exec(
			`DELETE FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, id,
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
// the table, and the document's winning value, are unchanged.
func compactRegisterOps(tx *sql.Tx, documentID string) error {
	_, err := tx.Exec(
		`DELETE FROM crdt_register_ops
		 WHERE document_id = ?
		   AND EXISTS (
			SELECT 1 FROM crdt_register_ops newer
			WHERE newer.document_id = crdt_register_ops.document_id
			  AND newer.device_id = crdt_register_ops.device_id
			  AND newer.version > crdt_register_ops.version
		   )`,
		documentID,
	)
	return err
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

// GetCRDTSnapshot returns the document's merged CRDT state together with the
// number of stored operations and tombstones the merge is derived from. A
// document with no committed operations yields ErrCRDTNotFound (the caller
// answers 404). The read is pure: it advances no cursor, writes nothing and
// registers no subscription.
func (s *Store) GetCRDTSnapshot(documentID string) (CRDTSnapshot, error) {
	state, err := s.GetCRDTState(documentID)
	if err != nil {
		return CRDTSnapshot{}, err
	}
	snapshot := CRDTSnapshot{Type: state.Type, Value: state.Value}

	count := func(query string) (int64, error) {
		var n int64
		if err := s.db.QueryRow(query, documentID).Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}

	switch state.Type {
	case CRDTTypeCounter, CRDTTypeGSet:
		operations, err := count(`SELECT COUNT(*) FROM crdt_ops WHERE document_id = ?`)
		if err != nil {
			return CRDTSnapshot{}, err
		}
		snapshot.Operations = operations
	case CRDTTypeRegister:
		operations, err := count(`SELECT COUNT(*) FROM crdt_register_ops WHERE document_id = ?`)
		if err != nil {
			return CRDTSnapshot{}, err
		}
		snapshot.Operations = operations
	case CRDTTypeORSet:
		operations, err := count(`SELECT COUNT(*) FROM crdt_orset_tags WHERE document_id = ?`)
		if err != nil {
			return CRDTSnapshot{}, err
		}
		tombstones, err := count(`SELECT COUNT(*) FROM crdt_orset_tombstones WHERE document_id = ?`)
		if err != nil {
			return CRDTSnapshot{}, err
		}
		snapshot.Operations = operations
		snapshot.Tombstones = tombstones
	}
	return snapshot, nil
}
