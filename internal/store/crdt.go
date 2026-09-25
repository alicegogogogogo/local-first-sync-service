package store

import (
	"database/sql"
	"errors"
	"strconv"
)

// CRDT document type names fixed by the first accepted operation.
const (
	CRDTTypeCounter = "counter"
	CRDTTypeGSet    = "gset"
)

// ErrCRDTNotFound reports that a document has no accepted CRDT operation yet,
// so it carries neither a type nor a merged state. The caller maps it to 404.
var ErrCRDTNotFound = errors.New("crdt state not found")

// ErrCRDTTypeConflict reports that a submit declares a CRDT type other than
// the one the document's first operation fixed. The established type and
// state are left untouched; the caller maps it to 409.
var ErrCRDTTypeConflict = errors.New("crdt document type is already fixed to another type")

// ErrCRDTValueRejected reports that a counter op carries a cumulative
// contribution below the device's already accepted value. Contributions must
// be monotonic; nothing is written. The caller maps it to 409.
var ErrCRDTValueRejected = errors.New("crdt counter contribution must be monotonically non-decreasing")

// ErrCRDTOpConflict reports that an op id is already recorded for the
// document with a different deviceId or value. Nothing is written; the caller
// maps it to 409.
type ErrCRDTOpConflict struct {
	ID string
}

func (e *ErrCRDTOpConflict) Error() string {
	return "crdt op " + strconv.Quote(e.ID) + " already exists with a different deviceId or value"
}

// CRDTOp is one element of a CRDT submit. For counters Value is the device's
// cumulative contribution (a non-negative integer); for grow-only sets Value
// is the string element being added.
type CRDTOp struct {
	ID    string // client-supplied stable op id, unique per document
	Value string // counter contribution as decimal text, or set element
}

// CRDTResult reports the outcome of one accepted CRDT op.
type CRDTResult struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
}

// CRDTState is the document's current type and merged result. Exactly one of
// Counter or Members carries the merged value according to Type.
type CRDTState struct {
	Type    string   `json:"type"`
	Counter int64    `json:"counter,omitempty"`
	Members []string `json:"members,omitempty"`
}

// SubmitCRDT validates and commits one CRDT batch atomically, all inside a
// single serialized transaction (the store's one connection plus immediate
// transactions already serialize writers).
//
// crdtType is "counter" or "gset". The submit and the deviceID/document pair
// go through the same registration/permission gate as replay before any op id
// or state is touched, so a rejected submit never reveals state: an
// unregistered device yields ErrDeviceNotFound (404) and a revoked one
// ErrPermissionDenied (403).
//
// The document's type is fixed by the first accepted batch. A later submit
// declaring another type yields ErrCRDTTypeConflict and writes nothing; two
// concurrent first-of-type declares therefore serialize and exactly one wins.
//
// Every op carries a stable id: a re-posted id is idempotent (created=false)
// only when its device and value match the stored op; a mismatch is
// *ErrCRDTOpConflict and aborts the whole batch before any row is written.
//
// Counter values are cumulative per device. A new counter op whose value is
// below the device's current maximum yields ErrCRDTValueRejected; equal or
// higher values move the maximum. The merged counter is the sum of every
// device's maximum. G-set ops add their element to the document-wide union;
// re-adding an existing element changes nothing (and is still created=true
// only when its op id is new).
func (s *Store) SubmitCRDT(documentID, deviceID, crdtType string, ops []CRDTOp) ([]CRDTResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	// Gate first, as in ReplayChanges: registration and permission are read
	// before any CRDT row, so 404/403 responses never expose state content.
	var deviceExists bool
	if err := tx.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM devices WHERE id = ?)`, deviceID,
	).Scan(&deviceExists); err != nil {
		return nil, err
	}
	if !deviceExists {
		return nil, ErrDeviceNotFound
	}
	var stored int
	err = tx.QueryRow(
		`SELECT authorized FROM document_permissions WHERE document_id = ? AND device_id = ?`,
		documentID, deviceID,
	).Scan(&stored)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// No deviation row: devices start authorized.
	case err != nil:
		return nil, err
	default:
		if stored == 0 {
			return nil, ErrPermissionDenied
		}
	}

	// Resolve or establish the document's fixed type. A first-ever insert
	// races-safe under the serialized transaction: the loser of two
	// concurrent declares reads the winner's row and gets ErrCRDTTypeConflict.
	var established string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&established)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := tx.Exec(
			`INSERT INTO crdt_documents (document_id, type) VALUES (?, ?)`,
			documentID, crdtType,
		); err != nil {
			return nil, err
		}
		established = crdtType
	case err != nil:
		return nil, err
	default:
		if established != crdtType {
			return nil, ErrCRDTTypeConflict
		}
	}

	results := make([]CRDTResult, len(ops))

	// First pass: resolve every stable id against existing ops. A single
	// mismatch aborts the whole batch before any new row is inserted.
	pending := make([]int, 0, len(ops))
	for i, op := range ops {
		var existingDevice, existingValue string
		err := tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			pending = append(pending, i)
		case err != nil:
			return nil, err
		default:
			if existingDevice != deviceID || existingValue != op.Value {
				return nil, &ErrCRDTOpConflict{ID: op.ID}
			}
			results[i] = CRDTResult{ID: op.ID, Created: false}
		}
	}

	switch established {
	case CRDTTypeCounter:
		// New counter ops must not move this device's cumulative contribution
		// backward. The check happens before any insert so a rejected batch
		// leaves the per-device maximum untouched.
		var current sql.NullString
		if err := tx.QueryRow(
			`SELECT MAX(CAST(value AS INTEGER)) FROM crdt_ops
			 WHERE document_id = ? AND device_id = ?`,
			documentID, deviceID,
		).Scan(&current); err != nil {
			return nil, err
		}
		var max int64
		if current.Valid {
			max, _ = strconv.ParseInt(current.String, 10, 64)
		}
		for _, i := range pending {
			v, convErr := strconv.ParseInt(ops[i].Value, 10, 64)
			if convErr != nil || v < max {
				return nil, ErrCRDTValueRejected
			}
			if v > max {
				max = v
			}
		}
	case CRDTTypeGSet:
		// Any element is accepted; the members table below is the union. No
		// per-device monotonic rule applies.
	}

	for _, i := range pending {
		op := ops[i]
		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, deviceID, op.Value,
		); err != nil {
			return nil, err
		}
		results[i] = CRDTResult{ID: op.ID, Created: true}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return results, nil
}

// GetCRDTState returns the document's fixed type and its current merged
// result. A document with no accepted CRDT op (and therefore no type row)
// yields ErrCRDTNotFound so the caller answers 404.
//
// The counter merge is the sum of each device's maximum cumulative
// contribution; the g-set merge is every distinct element, ascending. Both
// are derived from the op log itself, so the merged value is always exactly
// what was committed and is unchanged by restart.
func (s *Store) GetCRDTState(documentID string) (CRDTState, error) {
	var crdtType string
	err := s.db.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&crdtType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return CRDTState{}, ErrCRDTNotFound
	case err != nil:
		return CRDTState{}, err
	}

	state := CRDTState{Type: crdtType}
	switch crdtType {
	case CRDTTypeCounter:
		// Per-device maxima, then sum. Each row contributes at most once per
		// device: MAX keeps only the device's top cumulative contribution.
		rows, err := s.db.Query(
			`SELECT COALESCE(MAX(CAST(value AS INTEGER)), 0)
			 FROM crdt_ops WHERE document_id = ? GROUP BY device_id`,
			documentID,
		)
		if err != nil {
			return CRDTState{}, err
		}
		var total int64
		for rows.Next() {
			var v int64
			if err := rows.Scan(&v); err != nil {
				_ = rows.Close()
				return CRDTState{}, err
			}
			total += v
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return CRDTState{}, err
		}
		_ = rows.Close()
		state.Counter = total
	case CRDTTypeGSet:
		rows, err := s.db.Query(
			`SELECT DISTINCT value FROM crdt_ops
			 WHERE document_id = ? ORDER BY value ASC`,
			documentID,
		)
		if err != nil {
			return CRDTState{}, err
		}
		members := make([]string, 0)
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				_ = rows.Close()
				return CRDTState{}, err
			}
			members = append(members, v)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return CRDTState{}, err
		}
		_ = rows.Close()
		state.Members = members
	}
	return state, nil
}
