// Package crdt is the CRDT state service: an automatically mergeable state
// layer that sits beside the change event log and never touches it.
//
// A document's CRDT type is declared with its first operation batch —
// "counter", "gset", "register" or "orset" — and never changes afterward; two
// batches declaring different types concurrently serialize in one transaction
// on the shared connection and exactly one takes effect.
//
// A counter carries each device's accumulated contribution; contributions
// only move forward and the merge is the sum of per-device maxima. A gset
// adds strings; the merge is the sorted union. A register carries an arbitrary
// JSON value and a strictly increasing per-device version; the merge is the
// greatest version, ties broken by the smaller operation id. An orset tags
// each add with its operation id and a remove tombstones only the adds it has
// observed, so concurrent removes never delete later adds; the merge is the
// sorted set of elements with a live tag. Every merge is commutative,
// associative and idempotent, so any reader converges to the same value.
//
// Operation ids are stable and unique per document; a repeat with the same
// device and comparison content is idempotent, any other repeat a conflict.
// Compaction trims rows that no longer participate in a merge while keeping
// one fixed-size identity digest per trimmed id, so idempotency and conflict
// decisions are unchanged afterward.
//
// The service owns only its own tables. Registration and authorization are
// reached through its Gate interface inside the write transaction, and
// committed state changes are published to in-memory state subscriptions. It
// never writes change rows, allocates cursors or reads another service's
// storage.
package crdt

import (
	"database/sql"
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
// Value holds the type-specific merged result: for a counter it decodes to the
// JSON integer sum of per-device maxima; for a gset it decodes to the sorted
// JSON array of every accepted element (an empty set is []); for a register it
// is the winning operation's JSON value, exactly as submitted; for an orset it
// decodes to the sorted JSON array of the elements with at least one live add
// (an empty set is []).
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
	Value      json.RawMessage // the merged value, exactly as GetState derives it
	Operations int64           // stored operations the merge is derived from
	Tombstones int64           // stored orset tombstones; 0 for counter, gset and register
}

// Gate is the only seam the CRDT service uses to reach the registration and
// permission layers. It is evaluated inside the write transaction, so a
// rejected submission is judged before any CRDT content is observed.
type Gate interface {
	// DeviceAuthorizedTx returns nil when the device is registered and
	// authorized, store.ErrDeviceNotFound for an unregistered device and
	// store.ErrPermissionDenied after a revoke.
	DeviceAuthorizedTx(tx *sql.Tx, documentID, deviceID string) error
}

// schema is created when the service is constructed. Table shapes are
// unchanged from the pre-refactor layout, so CRDT data already on disk stays
// readable after the split.
const schema = `
CREATE TABLE IF NOT EXISTS crdt_documents (
	document_id TEXT NOT NULL PRIMARY KEY,
	type        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS crdt_ops (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	value       BLOB,
	PRIMARY KEY (document_id, id)
);
CREATE TABLE IF NOT EXISTS crdt_counter_values (
	document_id TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	value       INTEGER NOT NULL,
	PRIMARY KEY (document_id, device_id)
);
CREATE TABLE IF NOT EXISTS crdt_set_elements (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	PRIMARY KEY (document_id, element)
);
CREATE TABLE IF NOT EXISTS crdt_register_ops (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	version     INTEGER NOT NULL,
	value       BLOB NOT NULL,
	PRIMARY KEY (document_id, id)
);
CREATE TABLE IF NOT EXISTS crdt_orset_tags (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	op_id       TEXT NOT NULL,
	PRIMARY KEY (document_id, element, op_id)
);
CREATE TABLE IF NOT EXISTS crdt_orset_tombstones (
	document_id TEXT NOT NULL,
	element     TEXT NOT NULL,
	op_id       TEXT NOT NULL,
	removed_by  TEXT NOT NULL,
	PRIMARY KEY (document_id, element, op_id)
);
-- op_identities keeps, for each operation id whose merge row compaction
-- trimmed away, the only information idempotency and conflict decisions still
-- need: the originating device and a digest of the type-specific comparison
-- content. Compaction never trims this table; it does not participate in any
-- merge and holds a fixed-size digest rather than the operation payload.
CREATE TABLE IF NOT EXISTS crdt_op_identities (
	document_id TEXT NOT NULL,
	id          TEXT NOT NULL,
	device_id   TEXT NOT NULL,
	digest      BLOB NOT NULL,
	PRIMARY KEY (document_id, id)
);
`
