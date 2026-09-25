package crdt

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/alicegogogogogo/local-first-sync-service/internal/store"
)

// Service is the CRDT state service: the durable CRDT tables plus the
// in-memory merged-state subscriptions, serialized against each other.
type Service struct {
	db   *sql.DB
	gate Gate

	// mu serializes submissions and compactions against each other and against
	// a subscription's atomic register-plus-initial-read. Holding it across a
	// state-changing commit and its notification makes notifications leave in
	// transaction-commit order, and makes a commit's state land either in a
	// subscriber's initial read or in its queue, never both and never neither.
	// The single database connection already serializes the transactions
	// themselves, so this costs no concurrency.
	mu sync.Mutex

	subMu     sync.Mutex
	subs      map[crdtSubKey]map[uint64]*Subscription
	nextSubID uint64
	subWG     sync.WaitGroup
	closed    bool
}

// New constructs the CRDT state service over the shared kernel handle and
// creates its tables. gate answers the registration/permission checks inside
// a write transaction; it may be nil for callers that only read state.
func New(kernel *store.Store, gate Gate) (*Service, error) {
	db := kernel.DB()
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return &Service{
		db:   db,
		gate: gate,
		subs: make(map[crdtSubKey]map[uint64]*Subscription),
	}, nil
}

// SubmitOps validates and commits one CRDT batch atomically.
//
// declaredType fixes the type on the document's first batch; every later
// batch must declare the same type. Two batches declaring different types
// race in one serialized transaction, so exactly one wins and the other gets
// an *ErrConflict (409).
//
// The device is gated first (store.ErrDeviceNotFound /
// store.ErrPermissionDenied) before any CRDT content is read. Every operation
// is then resolved against the committed history: a repeated id is idempotent
// only with the same device and content, otherwise it is an *ErrConflict; a
// counter contribution below the device's current maximum, or a register
// version not strictly greater than the device's accepted versions, is
// likewise an *ErrConflict. Any failure rejects the whole batch. Results come
// back in request order.
//
// When the committed batch actually moved the merged state the document's
// state subscribers are notified with the new merged state after the commit;
// idempotent repeats, rejected batches and no-op additions or removals change
// nothing and notify nobody.
func (s *Service) SubmitOps(documentID, declaredType string, ops []Op) ([]Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	deviceID := ops[0].DeviceID

	// Registration and permission are enforced before any CRDT content is
	// observed, so a rejected submission cannot reveal type or state.
	if s.gate != nil {
		if err := s.gate.DeviceAuthorizedTx(tx, documentID, deviceID); err != nil {
			return nil, err
		}
	}

	// Resolve the document type: fixed on the first accepted batch.
	var docType string
	err = tx.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// First batch for this document: its declared type wins the race.
		if _, err := tx.Exec(
			`INSERT INTO crdt_documents (document_id, type) VALUES (?, ?)`,
			documentID, declaredType,
		); err != nil {
			return nil, err
		}
		docType = declaredType
	case err != nil:
		return nil, err
	default:
		if docType != declaredType {
			return nil, &ErrConflict{
				Reason: fmt.Sprintf("document type is already %q", docType),
			}
		}
	}

	results := make([]Result, len(ops))

	var stateChanged bool
	switch docType {
	case TypeCounter:
		changed, err := applyCounterOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeGSet:
		changed, err := applyGSetOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeRegister:
		changed, err := applyRegisterOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	case TypeORSet:
		changed, err := applyORSetOps(tx, documentID, ops, results)
		if err != nil {
			return nil, err
		}
		stateChanged = changed
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if stateChanged {
		// The committed batch moved the merged state; re-derive it (the merge
		// is never cached) and publish it after commit, so a subscriber only
		// ever observes a durable state and commits arrive in commit order.
		state, err := s.GetState(documentID)
		if err != nil {
			return results, nil
		}
		s.notifySubscribers(documentID, state)
	}
	return results, nil
}

// applyCounterOps resolves and applies every counter operation inside tx. A
// repeated id is idempotent only with the same device and contribution. A new
// id with a contribution below the device's stored maximum regresses the
// counter and is rejected; an equal contribution is an accepted no-op; a
// larger one advances the device's maximum. It reports whether the merged
// state actually changed — only an advancing contribution moves the sum.
func applyCounterOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	changed := false
	for i, op := range ops {
		value, err := decodeCounterValue(op.Value)
		if err != nil {
			return false, &ErrConflict{ID: op.ID, Reason: err.Error()}
		}

		var existingDevice string
		var existingValue []byte
		err = tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The merge row may have been trimmed by compaction; the retained
			// identity still decides between idempotent replay and conflict.
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeCounter,
				"operation id already exists with a different device or value", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id: enforce monotonicity against this device's maximum.
			var current sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT value FROM crdt_counter_values WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&current); scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
				return false, scanErr
			}
			if current.Valid && value < current.Int64 {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "counter contribution must not decrease",
				}
			}
			if _, err := tx.Exec(
				`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
				documentID, op.ID, op.DeviceID, []byte(op.Value),
			); err != nil {
				return false, err
			}
			if !current.Valid || value > current.Int64 {
				if _, err := tx.Exec(
					`INSERT INTO crdt_counter_values (document_id, device_id, value) VALUES (?, ?, ?)
					 ON CONFLICT (document_id, device_id) DO UPDATE SET value = excluded.value`,
					documentID, op.DeviceID, value,
				); err != nil {
					return false, err
				}
				changed = true
			}
			results[i] = Result{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !store.JSONEqual(existingValue, op.Value) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or value",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
		}
	}
	return changed, nil
}

// applyGSetOps resolves and applies every grow-only-set operation inside tx.
// A repeated id is idempotent only with the same device and the same set of
// elements (element order is irrelevant to the merge). New elements are
// inserted into the union table; re-adding an element changes nothing. It
// reports whether the merged state actually changed.
func applyGSetOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	changed := false
	for i, op := range ops {
		var existingDevice string
		var existingElements []byte
		err := tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingElements)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeGSet,
				"operation id already exists with a different device or elements", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id; insert the op and its elements below.
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !stringSetEqual(existingElements, op.Elements) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device or elements",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
			continue
		}

		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, op.DeviceID, []byte(encodeSetElements(op.Elements)),
		); err != nil {
			return false, err
		}
		for _, element := range op.Elements {
			// The union table is an identity insert: a repeated element simply
			// exists once, and only a genuinely new element moves the merge.
			res, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_set_elements (document_id, element) VALUES (?, ?)`,
				documentID, element,
			)
			if err != nil {
				return false, err
			}
			if n, err := res.RowsAffected(); err == nil && n > 0 {
				changed = true
			}
		}
		results[i] = Result{ID: op.ID, Created: true}
	}
	return changed, nil
}

// applyRegisterOps resolves and applies every register operation inside tx. A
// repeated id is idempotent only with the same device, version and value. A
// new id whose version is not strictly greater than every version already
// accepted from the same device regresses (or stalls) the device's clock and
// is rejected. It reports whether the merged state actually changed.
func applyRegisterOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	// The merged value before the batch, so a batch that leaves the winner's
	// value untouched reports no change and notifies nobody.
	prev, prevOK, err := registerWinner(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Version < 0 {
			// The API boundary rejects negative versions; a negative version
			// here means the caller skipped validation.
			return false, &ErrConflict{ID: op.ID, Reason: "register version must be a non-negative integer"}
		}

		var existingDevice string
		var existingVersion int64
		var existingValue []byte
		err := tx.QueryRow(
			`SELECT device_id, version, value FROM crdt_register_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingVersion, &existingValue)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// An older per-device version may have been trimmed; the retained
			// identity still decides replay versus conflict before the version
			// monotonicity gate, which only applies to an id never accepted.
			idempotent, idErr := resolveTrimmedIdentity(
				tx, documentID, TypeRegister,
				"operation id already exists with a different device, version or value", op,
			)
			if idErr != nil {
				return false, idErr
			}
			if idempotent {
				results[i] = Result{ID: op.ID, Created: false}
				continue
			}
			// New id: the device's versions only move strictly forward. The
			// per-device maximum is derived from the retained operations, so
			// the decision survives compaction and restarts.
			var maxVersion sql.NullInt64
			if scanErr := tx.QueryRow(
				`SELECT MAX(version) FROM crdt_register_ops WHERE document_id = ? AND device_id = ?`,
				documentID, op.DeviceID,
			).Scan(&maxVersion); scanErr != nil {
				return false, scanErr
			}
			if maxVersion.Valid && op.Version <= maxVersion.Int64 {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "register version must be greater than the device's previously accepted version",
				}
			}
			if _, err := tx.Exec(
				`INSERT INTO crdt_register_ops (document_id, id, device_id, version, value) VALUES (?, ?, ?, ?, ?)`,
				documentID, op.ID, op.DeviceID, op.Version, []byte(op.Value),
			); err != nil {
				return false, err
			}
			results[i] = Result{ID: op.ID, Created: true}
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || existingVersion != op.Version || !store.JSONEqual(existingValue, op.Value) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, version or value",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
		}
	}

	next, nextOK, err := registerWinner(tx, documentID)
	if err != nil {
		return false, err
	}
	// The merge is the winner's value; a new winner carrying a JSON-equal
	// value does not move the state.
	changed := prevOK != nextOK || (prevOK && nextOK && !store.JSONEqual(prev, next))
	return changed, nil
}

// registerWinner returns the value the register currently merges to: the
// value of the accepted operation with the greatest version, ties broken by
// the lexicographically smaller operation id. It reports false when the
// document has no accepted register operation yet.
func registerWinner(tx *sql.Tx, documentID string) (json.RawMessage, bool, error) {
	var value []byte
	err := tx.QueryRow(
		`SELECT value FROM crdt_register_ops WHERE document_id = ?
		 ORDER BY version DESC, id ASC LIMIT 1`,
		documentID,
	).Scan(&value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, err
	default:
		return json.RawMessage(value), true, nil
	}
}

// applyORSetOps resolves and applies every observed-remove-set operation
// inside tx. A repeated id is idempotent only with the same device, action and
// element; an idempotent replay applies nothing, so a replayed remove never
// tombstones adds that landed after its first acceptance. An add tags the
// element with its own operation id; a remove tombstones exactly the tags
// already accepted for the element. Removing an element with no live tags is
// an accepted no-op. It reports whether the merged state actually changed.
func applyORSetOps(tx *sql.Tx, documentID string, ops []Op, results []Result) (bool, error) {
	prev, err := queryORSetElements(tx, documentID)
	if err != nil {
		return false, err
	}

	for i, op := range ops {
		if op.Action != ORSetAdd && op.Action != ORSetRemove {
			return false, &ErrConflict{ID: op.ID, Reason: `orset action must be "add" or "remove"`}
		}

		var existingDevice string
		var existingContent []byte
		err := tx.QueryRow(
			`SELECT device_id, value FROM crdt_ops WHERE document_id = ? AND id = ?`,
			documentID, op.ID,
		).Scan(&existingDevice, &existingContent)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// New id; insert the op and apply its effect below.
		case err != nil:
			return false, err
		default:
			if existingDevice != op.DeviceID || !store.JSONEqual(existingContent, encodeORSetContent(op.Action, op.Element)) {
				return false, &ErrConflict{
					ID:     op.ID,
					Reason: "operation id already exists with a different device, action or element",
				}
			}
			results[i] = Result{ID: op.ID, Created: false}
			continue
		}

		if _, err := tx.Exec(
			`INSERT INTO crdt_ops (document_id, id, device_id, value) VALUES (?, ?, ?, ?)`,
			documentID, op.ID, op.DeviceID, []byte(encodeORSetContent(op.Action, op.Element)),
		); err != nil {
			return false, err
		}
		if op.Action == ORSetAdd {
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_orset_tags (document_id, element, op_id) VALUES (?, ?, ?)`,
				documentID, op.Element, op.ID,
			); err != nil {
				return false, err
			}
		} else {
			// Tombstone exactly the tags this remove observes: the adds
			// accepted before it. Tags created later are not in the table yet
			// and survive; already-tombstoned tags are an identity insert.
			if _, err := tx.Exec(
				`INSERT OR IGNORE INTO crdt_orset_tombstones (document_id, element, op_id, removed_by)
				 SELECT ?, ?, op_id, ? FROM crdt_orset_tags
				 WHERE document_id = ? AND element = ?`,
				documentID, op.Element, op.ID, documentID, op.Element,
			); err != nil {
				return false, err
			}
		}
		results[i] = Result{ID: op.ID, Created: true}
	}

	next, err := queryORSetElements(tx, documentID)
	if err != nil {
		return false, err
	}
	return !stringSliceEqual(prev, next), nil
}

// orsetLiveElementsSQL selects the elements with at least one add tag no
// tombstone covers, in ascending order — the orset merge, derived from the
// accepted operations rather than cached.
const orsetLiveElementsSQL = `
SELECT DISTINCT element FROM crdt_orset_tags t
WHERE t.document_id = ? AND NOT EXISTS (
	SELECT 1 FROM crdt_orset_tombstones x
	WHERE x.document_id = t.document_id
	  AND x.element = t.element
	  AND x.op_id = t.op_id
)
ORDER BY element ASC`

// orsetQuerier is satisfied by both *sql.Tx (inside a submission) and *sql.DB
// (a state read), so the merge query exists exactly once.
type orsetQuerier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// queryORSetElements returns the document's live orset elements in ascending
// order. The empty set is an empty slice, never nil, so it marshals as [].
func queryORSetElements(q orsetQuerier, documentID string) ([]string, error) {
	rows, err := q.Query(orsetLiveElementsSQL, documentID)
	if err != nil {
		return nil, err
	}
	elements := make([]string, 0)
	for rows.Next() {
		var element string
		if err := rows.Scan(&element); err != nil {
			_ = rows.Close()
			return nil, err
		}
		elements = append(elements, element)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	_ = rows.Close()
	return elements, nil
}

// encodeORSetContent renders an orset operation's content (its action and
// element) as a JSON object for durable storage.
func encodeORSetContent(action, element string) json.RawMessage {
	raw, err := json.Marshal(struct {
		Action  string `json:"action"`
		Element string `json:"element"`
	}{Action: action, Element: element})
	if err != nil {
		// Strings always marshal.
		return json.RawMessage(`{"action":"","element":""}`)
	}
	return raw
}

// stringSliceEqual reports whether a and b hold the same strings in the same
// order. Both orset element lists are ascending, so it is set equality.
func stringSliceEqual(a, b []string) bool {
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

// decodeCounterValue requires raw to be a non-negative JSON integer with no
// fraction or exponent. Floats, strings, booleans and null are rejected at the
// API boundary; a malformed integer here means the caller skipped validation.
func decodeCounterValue(raw json.RawMessage) (int64, error) {
	var n int64
	if err := json.Unmarshal(raw, &n); err != nil || n < 0 {
		return 0, errors.New("counter value must be a non-negative integer")
	}
	return n, nil
}

// encodeSetElements renders the elements of one gset operation as a JSON array
// for durable storage of the original operation content.
func encodeSetElements(elements []string) json.RawMessage {
	raw, err := json.Marshal(elements)
	if err != nil {
		return json.RawMessage("[]")
	}
	return raw
}

// stringSetEqual reports whether raw (a JSON array of strings stored for a
// prior gset operation) contains exactly the elements of want, ignoring
// duplicates and order: the content of a set operation is the set of elements.
func stringSetEqual(raw json.RawMessage, want []string) bool {
	var got []string
	if err := json.Unmarshal(raw, &got); err != nil {
		return false
	}
	set := make(map[string]struct{}, len(got))
	for _, e := range got {
		set[e] = struct{}{}
	}
	for _, e := range want {
		if _, ok := set[e]; !ok {
			return false
		}
		delete(set, e)
	}
	return len(set) == 0
}

// GetState returns the merged CRDT state for documentID. A document with no
// committed operations yields ErrNotFound (the caller answers 404).
//
// The merged value is derived from the per-device maxima (counter), the union
// table (gset), the version/id ordering of accepted operations (register) or
// the live tags minus tombstones (orset), not cached.
func (s *Service) GetState(documentID string) (State, error) {
	var docType string
	err := s.db.QueryRow(
		`SELECT type FROM crdt_documents WHERE document_id = ?`, documentID,
	).Scan(&docType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return State{}, ErrNotFound
	case err != nil:
		return State{}, err
	}

	switch docType {
	case TypeCounter:
		var total int64
		if err := s.db.QueryRow(
			`SELECT COALESCE(SUM(value), 0) FROM crdt_counter_values WHERE document_id = ?`,
			documentID,
		).Scan(&total); err != nil {
			return State{}, err
		}
		raw, err := json.Marshal(total)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	case TypeGSet:
		rows, err := s.db.Query(
			`SELECT element FROM crdt_set_elements WHERE document_id = ? ORDER BY element ASC`,
			documentID,
		)
		if err != nil {
			return State{}, err
		}
		elements := make([]string, 0)
		for rows.Next() {
			var element string
			if err := rows.Scan(&element); err != nil {
				_ = rows.Close()
				return State{}, err
			}
			elements = append(elements, element)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return State{}, err
		}
		_ = rows.Close()
		raw, err := json.Marshal(elements)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	case TypeRegister:
		// A document's type row commits together with its first batch, so a
		// register document always has at least one accepted operation.
		var value []byte
		if err := s.db.QueryRow(
			`SELECT value FROM crdt_register_ops WHERE document_id = ?
			 ORDER BY version DESC, id ASC LIMIT 1`,
			documentID,
		).Scan(&value); err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: json.RawMessage(value)}, nil
	case TypeORSet:
		elements, err := queryORSetElements(s.db, documentID)
		if err != nil {
			return State{}, err
		}
		raw, err := json.Marshal(elements)
		if err != nil {
			return State{}, err
		}
		return State{Type: docType, Value: raw}, nil
	default:
		return State{}, fmt.Errorf("unknown crdt type %q stored for document", docType)
	}
}
