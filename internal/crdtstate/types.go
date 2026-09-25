// Package crdtstate is the CRDT state service: an independent, automatically
// mergeable state layer living beside the change-event log. It owns a
// document's fixed type, its accepted operations, the merged result derived
// from them, the compaction that bounds their storage and the in-memory
// subscriptions pushed to when the merged result actually moves.
//
// A document's CRDT type is declared with its first operation batch —
// "counter", "gset", "register" or "orset" — and never changes afterward; two
// batches declaring different types serialize in one transaction and exactly
// one takes effect. Every operation carries a client-supplied id, unique per
// document: a re-post with the same device and type-specific comparison
// content is idempotent, a differing one is a conflict. Submissions,
// compaction and the permission decision all serialize against one consistent
// state.
//
// The service owns its tables (crdt_documents, crdt_ops,
// crdt_counter_values, crdt_set_elements, crdt_register_ops, crdt_orset_tags,
// crdt_orset_tombstones, crdt_op_identities) and never reads or writes
// another service's storage or fields — in particular it never produces a
// change record or occupies a document cursor. The two things it needs from
// the rest of the system arrive through its Gate boundary (registration and
// the permission decision, enforced inside its own transaction before any
// CRDT content is observed) and its OnRevoke notification (a committed revoke
// ends live subscriptions stickily).
package crdtstate

import (
	"encoding/json"
	"errors"
	"fmt"
)

// CRDT document types.
const (
	// TypeCounter is the grow-only counter: the merged value is the sum of
	// each device's maximum accumulated contribution.
	TypeCounter = "counter"
	// TypeGSet is the grow-only set: the merged value is the union of all
	// added elements.
	TypeGSet = "gset"
	// TypeRegister is the last-writer-wins register: the merged value is the
	// value of the operation with the greatest version, ties broken by the
	// lexicographically smaller operation id.
	TypeRegister = "register"
	// TypeORSet is the observed-remove set: the merged value is the set of
	// elements with at least one add not tombstoned by an observed remove.
	TypeORSet = "orset"
)

// OR-Set operation actions.
const (
	// ORSetAdd adds the operation's element with a fresh tag.
	ORSetAdd = "add"
	// ORSetRemove tombstones every add tag for the element that the operation
	// observes.
	ORSetRemove = "remove"
)

// ErrNotFound reports that no CRDT operation has ever been committed for the
// document, so it has neither a type nor a merged state yet. The caller maps
// it to 404.
var ErrNotFound = errors.New("crdt state not found")

// ErrConflict reports a rejected CRDT batch: the document's type is already
// fixed to another type, a counter contribution regressed, or an operation id
// already exists with different content or origin. Nothing is written; the
// caller maps it to 409.
type ErrConflict struct {
	// ID is the conflicting operation id when one is known; it is empty for a
	// document-level conflict (a mismatched declared type).
	ID string
	// Reason carries a short, machine-facing description of the conflict.
	Reason string
}

func (e *ErrConflict) Error() string {
	if e.ID != "" {
		return fmt.Sprintf("crdt conflict for operation %q: %s", e.ID, e.Reason)
	}
	return "crdt conflict: " + e.Reason
}

// Op is one element of an inbound CRDT batch.
//
// For a counter, Value is the device's accumulated contribution: it must be a
// JSON number that is a non-negative integer, and it may never decrease for
// the same device. Elements and Version are unused.
//
// For a gset, Elements lists the strings the operation adds to the set and
// must be non-empty; Value and Version are unused.
//
// For a register, Value is an arbitrary JSON value stored verbatim and
// Version is a non-negative integer that must be strictly greater than every
// version the same device has already had accepted. Elements is unused.
//
// For an orset, Action is "add" or "remove" and Element is the non-empty
// string the operation adds or removes. Value, Elements and Version are
// unused.
type Op struct {
	ID       string          // client-supplied stable id, unique per document
	DeviceID string          // originating device (shared by the whole batch)
	Value    json.RawMessage // counter: accumulated contribution; register: the value
	Elements []string        // gset: elements to add
	Version  int64           // register: logical version, per-device strictly increasing
	Action   string          // orset: ORSetAdd or ORSetRemove
	Element  string          // orset: the element the action applies to
}

// State is a document's merged CRDT state.
//
// Value holds the type-specific merged result: for a counter it decodes to
// the JSON integer sum of per-device maxima; for a gset it decodes to the
// sorted JSON array of every accepted element (an empty set is []); for a
// register it is the winning operation's JSON value, exactly as submitted;
// for an orset it decodes to the sorted JSON array of the elements with at
// least one live add (an empty set is []).
type State struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// Result reports the outcome for one element of an accepted CRDT batch.
type Result struct {
	ID      string `json:"id"`
	Created bool   `json:"created"`
}

// Snapshot is a document's merged CRDT state together with the storage
// footprint of that state: Operations counts the stored operations the merge
// is derived from (counter and gset: rows of the operation log; register: the
// retained per-device operations; orset: the add tags, live or tombstoned)
// and Tombstones counts the stored orset tombstones (always zero for the
// other types). Both counts are non-negative; compaction drives them down to
// the operations and tombstones that still participate in the merge.
type Snapshot struct {
	Type       string          // the document's fixed CRDT type
	Value      json.RawMessage // the merged value, exactly as State derives it
	Operations int64           // stored operations the merge is derived from
	Tombstones int64           // stored orset tombstones; 0 for counter, gset and register
}

// jsonEqual reports whether two payloads are equal after JSON decoding, so
// 1 and 1.0, or reordered object keys, compare equal.
func jsonEqual(a, b json.RawMessage) bool {
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		return false
	}
	return deepEqual(va, vb)
}

func deepEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !deepEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			w, exists := bv[k]
			if !exists || !deepEqual(v, w) {
				return false
			}
		}
		return true
	default:
		return false
	}
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

// encodeSetElements renders the elements of one gset operation as a JSON
// array for durable storage of the original operation content.
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

// encodeORSetContent renders an orset operation's content (its action and
// element) as a JSON object for durable storage, so a later submission of the
// same id can be compared against the original content.
func encodeORSetContent(action, element string) json.RawMessage {
	raw, err := json.Marshal(struct {
		Action  string `json:"action"`
		Element string `json:"element"`
	}{Action: action, Element: element})
	if err != nil {
		return json.RawMessage(`{"action":"","element":""}`)
	}
	return raw
}

// stringSliceEqual reports whether a and b hold the same strings in the same
// order. Both orset element lists are ascending, so order-sensitive equality
// is set equality.
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
