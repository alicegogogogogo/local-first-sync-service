package store

import (
	"errors"
	"sync"
)

// crdtSubKey identifies the set of CRDT state subscriptions belonging to one
// device's view of one document: a state-changing commit fans out to every
// device set under the document, a permission revoke closes only the one
// (document, device) set.
type crdtSubKey struct {
	document string
	device   string
}

// CRDTSubscription is one in-memory, push-only subscription to a document's
// merged CRDT state. States that actually changed the merge are appended in
// transaction-commit order and handed to the connection in that order; the
// wake channel is merely a coalescing "drain the queue" signal. A permission
// revoke targeting the subscription's (document, device) pair is delivered as
// a separate, one-shot closed channel so it can neither be missed nor undone
// by a later grant on the same connection.
//
// Like change-log subscriptions, a CRDT subscription leaves no durable trace:
// nothing is written to the database, a disconnect simply unregisters, and
// closing the store wakes every subscription once (the server maps that to a
// going-away close). Pushing never writes an operation and never moves the
// merge; the subscription layer only observes committed states.
type CRDTSubscription struct {
	// mu guards queue.
	mu sync.Mutex
	// queue holds merged states in commit order that the connection has not
	// drained yet. It is unbounded: every committed state change must reach
	// the subscriber exactly once and in order, so a slow consumer is never
	// allowed to drop or coalesce a state.
	queue []CRDTState
	// wakes is a 1-buffered "drain now" signal; repeated notifications while
	// one is pending coalesce because Drain always empties the whole queue.
	wakes chan struct{}
	// revoked is closed once, the first time permission for the keyed pair is
	// revoked while this subscription is live. Closing is sticky: a later
	// grant never reopens it.
	revoked       chan struct{}
	revokedClosed bool
}

// Wake returns the coalescing drain signal.
func (x *CRDTSubscription) Wake() <-chan struct{} { return x.wakes }

// Revoked returns the one-shot revoke signal.
func (x *CRDTSubscription) Revoked() <-chan struct{} { return x.revoked }

// Drain removes and returns every queued merged state in commit order. It
// returns nil when the queue is empty.
func (x *CRDTSubscription) Drain() []CRDTState {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.queue) == 0 {
		return nil
	}
	out := x.queue
	x.queue = nil
	return out
}

// deliver appends a committed merged state and pokes the drain signal. It is
// non-blocking: a wake already pending covers this state because the consumer
// drains the whole queue each time it runs.
func (x *CRDTSubscription) deliver(state CRDTState) {
	x.mu.Lock()
	x.queue = append(x.queue, state)
	x.mu.Unlock()
	select {
	case x.wakes <- struct{}{}:
	default:
	}
}

// AddCRDTSubscription registers an in-memory subscription to documentID as
// seen by deviceID and returns it together with an idempotent unregister that
// a client disconnect, a permission close and a store close may all race to
// call.
//
// Registration precedes the subscriber's first state read, so a state-changing
// commit landing between registration and that read is queued and cannot be
// missed; the subscriber compares a queued state with the one it last sent and
// skips an identical one, which makes register-then-read ordering harmless.
//
// If the store is already closing the subscription's wake is signaled
// immediately and unregister is a no-op, so a late subscriber ends at once
// with the going-away close instead of parking forever.
func (s *Store) AddCRDTSubscription(documentID, deviceID string) (*CRDTSubscription, func()) {
	sub := &CRDTSubscription{
		wakes:   make(chan struct{}, 1),
		revoked: make(chan struct{}),
	}
	key := crdtSubKey{document: documentID, device: deviceID}

	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	if s.closed {
		close(sub.wakes)
		return sub, func() {}
	}
	s.subWG.Add(1)
	if s.crdtSubs == nil {
		s.crdtSubs = make(map[crdtSubKey]map[uint64]*CRDTSubscription)
	}
	s.nextCRDTSubID++
	id := s.nextCRDTSubID
	set := s.crdtSubs[key]
	if set == nil {
		set = make(map[uint64]*CRDTSubscription)
		s.crdtSubs[key] = set
	}
	set[id] = sub
	unregister := func() {
		s.pollMu.Lock()
		if set, ok := s.crdtSubs[key]; ok {
			if _, ok := set[id]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(s.crdtSubs, key)
				}
				s.pollMu.Unlock()
				s.subWG.Done()
				return
			}
		}
		s.pollMu.Unlock()
	}
	return sub, unregister
}

// OpenCRDTSubscription atomically registers a CRDT state subscription and
// reads the document's current merged state. It returns:
//
//   - state: the current merged state, or nil when no CRDT operation has ever
//     been committed for the document (the subscriber then waits silently);
//   - sub: the live subscription;
//   - unregister: idempotent removal.
//
// Registration and the read happen as one step relative to a SubmitCRDTOps
// commit — both hold crdtMu — so a state-changing commit lands either inside
// the returned initial state or in the subscription's queue, never in both and
// never neither; the subscriber therefore never misses the first state and
// never sends a queued state that is older than its initial one. When the
// store is already closing the wake is signaled immediately and unregister is
// a no-op, so the connection ends with a going-away close instead of parking.
func (s *Store) OpenCRDTSubscription(documentID, deviceID string) (state *CRDTState, sub *CRDTSubscription, unregister func(), err error) {
	s.crdtMu.Lock()
	defer s.crdtMu.Unlock()

	sub, unregister = s.AddCRDTSubscription(documentID, deviceID)
	current, getErr := s.GetCRDTState(documentID)
	switch {
	case errors.Is(getErr, ErrCRDTNotFound):
		return nil, sub, unregister, nil
	case getErr != nil:
		unregister()
		return nil, nil, func() {}, getErr
	}
	return &current, sub, unregister, nil
}

// notifyCRDTSubscribers publishes a newly merged state to every CRDT
// subscription open on documentID. It is called only after a state-changing
// CRDT transaction has committed, so every pushed state is durable and states
// arrive in transaction-commit order. Idempotent repeats, rejected batches and
// no-op additions never call it, so such commits push nothing.
func (s *Store) notifyCRDTSubscribers(documentID string, state CRDTState) {
	s.pollMu.Lock()
	var targets []*CRDTSubscription
	for key, set := range s.crdtSubs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	s.pollMu.Unlock()

	for _, sub := range targets {
		sub.deliver(state)
	}
}

// signalRevokedCRDTSubscribers closes the one-shot revoked channel of every
// CRDT subscription open on (documentID, deviceID), so the server ends those
// subscriptions with the permission-closed code even if a grant races in
// afterward. Other devices' and other documents' subscriptions are untouched.
func (s *Store) signalRevokedCRDTSubscribers(documentID, deviceID string) {
	s.pollMu.Lock()
	set := s.crdtSubs[crdtSubKey{document: documentID, device: deviceID}]
	targets := make([]*CRDTSubscription, 0, len(set))
	for _, sub := range set {
		if !sub.revokedClosed {
			sub.revokedClosed = true
			targets = append(targets, sub)
		}
	}
	s.pollMu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// wakeCRDTSubscribers gives every live CRDT subscription one coalescing wake
// during shutdown; the server re-checks the closed flag and finishes each
// connection with a going-away close.
func (s *Store) wakeCRDTSubscribers() {
	s.pollMu.Lock()
	var targets []*CRDTSubscription
	for _, set := range s.crdtSubs {
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	s.pollMu.Unlock()

	for _, sub := range targets {
		select {
		case sub.wakes <- struct{}{}:
		default:
		}
	}
}
