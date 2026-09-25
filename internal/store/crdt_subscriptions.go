// CRDT state subscriptions are the in-memory, push-only notification layer
// over the merged CRDT state. Unlike the change-log subscriptions — where a
// coalesced wakeup is enough because the consumer re-reads a durable log from
// its last cursor — a CRDT state has no cursor: every distinct merged value
// must reach the subscriber exactly once and in transaction commit order.
// Each subscription therefore carries an unbounded queue of merged states;
// the wake channel is only a "queue non-empty" hint and may coalesce.
//
// A subscription belongs to a (document, device) pair — the device owns the
// session that opened the connection. SubmitCRDTOps enqueues the new merged
// state after its commit, but only when the batch actually changed the merged
// value; idempotent repeats, rejected regressions and no-op additions enqueue
// nothing. A permission revoke closes the one-shot revoked channel of the
// matching pair, stickily. Subscriptions leave no durable trace: a disconnect
// unregisters, a store close broadcasts one final wakeup (mapped to a
// going-away close by the server), and nothing is written to the database.

package store

import (
	"errors"
	"sync"
)

// CRDTSubscription is one live CRDT state subscription. The queue is filled
// by SubmitCRDTOps (holding crdtMu) and drained by the serving handler.
type CRDTSubscription struct {
	// wakes is buffered (cap 1) and signaled whenever the queue transitions
	// to non-empty; signals coalesce freely because Drain takes everything.
	wakes chan struct{}
	// revoked is closed once, the first time permission for the keyed pair
	// is revoked while this subscription is live. Closing is sticky: a later
	// grant never reopens it, so a revoked subscription always ends.
	revoked chan struct{}

	// mu guards pending. The commit path appends; the consumer drains.
	mu      sync.Mutex
	pending []CRDTState

	revokedClosed bool // guarded by crdtMu

	store *Store
	key   subKey
	id    uint64
}

// Wakes returns the channel signaled (coalesceably) whenever new merged
// states have been queued, and once when the store starts closing.
func (sub *CRDTSubscription) Wakes() <-chan struct{} { return sub.wakes }

// Revoked returns the channel closed once if permission for the
// subscription's (document, device) pair is revoked while it is live.
func (sub *CRDTSubscription) Revoked() <-chan struct{} { return sub.revoked }

// Drain removes and returns every queued merged state in commit order. Each
// drained state is a distinct merged value: the commit path only enqueues
// when the merged value actually changed.
func (sub *CRDTSubscription) Drain() []CRDTState {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	out := sub.pending
	sub.pending = nil
	return out
}

// Unregister removes the subscription. It is idempotent: a client
// disconnect, a permission close and a store close may all race to call it.
func (sub *CRDTSubscription) Unregister() {
	s := sub.store
	s.crdtMu.Lock()
	if set, ok := s.crdtSubs[sub.key]; ok {
		if _, ok := set[sub.id]; ok {
			delete(set, sub.id)
			if len(set) == 0 {
				delete(s.crdtSubs, sub.key)
			}
			s.crdtMu.Unlock()
			s.subWG.Done()
			return
		}
	}
	s.crdtMu.Unlock()
}

// SubscribeCRDT registers a CRDT state subscription for documentID as seen by
// deviceID and atomically reads the current merged state: hasState is false
// when the document has no committed CRDT operations yet.
//
// Registration and the initial read run under crdtMu, which SubmitCRDTOps
// holds across its commit and fan-out, so every state-changing commit is
// reflected either in the returned initial state or in the subscription's
// queue — never both, never neither.
//
// If the store is already closing the subscription's wake channel is
// signaled immediately and the initial read reports no state, so a late
// subscriber ends with a going-away close instead of parking forever.
func (s *Store) SubscribeCRDT(documentID, deviceID string) (sub *CRDTSubscription, initial CRDTState, hasState bool, err error) {
	sub = &CRDTSubscription{
		wakes:   make(chan struct{}, 1),
		revoked: make(chan struct{}),
		store:   s,
		key:     subKey{document: documentID, device: deviceID},
	}

	s.crdtMu.Lock()
	defer s.crdtMu.Unlock()
	if s.crdtClosed {
		close(sub.wakes)
		return sub, CRDTState{}, false, nil
	}
	s.subWG.Add(1)
	s.nextCRDTSubID++
	sub.id = s.nextCRDTSubID
	set := s.crdtSubs[sub.key]
	if set == nil {
		set = make(map[uint64]*CRDTSubscription)
		s.crdtSubs[sub.key] = set
	}
	set[sub.id] = sub

	state, err := s.GetCRDTState(documentID)
	switch {
	case errors.Is(err, ErrCRDTNotFound):
		// No operations yet: the subscriber waits silently for the first
		// state to appear.
		return sub, CRDTState{}, false, nil
	case err != nil:
		delete(set, sub.id)
		if len(set) == 0 {
			delete(s.crdtSubs, sub.key)
		}
		s.subWG.Done()
		return nil, CRDTState{}, false, err
	}
	return sub, state, true, nil
}

// enqueueCRDTStateLocked appends state to the queue of every live
// subscription on documentID and signals their wake channels. The caller
// (SubmitCRDTOps) holds crdtMu and has just committed the transaction that
// produced state, so every subscriber observes the merged states in
// transaction commit order.
func (s *Store) enqueueCRDTStateLocked(documentID string, state CRDTState) {
	for key, set := range s.crdtSubs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			sub.mu.Lock()
			sub.pending = append(sub.pending, state)
			sub.mu.Unlock()
			select {
			case sub.wakes <- struct{}{}:
			default:
				// A pending wake already covers this state: Drain takes
				// the whole queue.
			}
		}
	}
}

// signalRevokedCRDTSubscribers closes the one-shot revoked channel of every
// CRDT subscription open on (documentID, deviceID), so the server ends those
// subscriptions with the permission-closed code even if a grant races in
// afterward. Other devices' and other documents' subscriptions are untouched.
func (s *Store) signalRevokedCRDTSubscribers(documentID, deviceID string) {
	s.crdtMu.Lock()
	set := s.crdtSubs[subKey{document: documentID, device: deviceID}]
	targets := make([]*CRDTSubscription, 0, len(set))
	for _, sub := range set {
		if !sub.revokedClosed {
			sub.revokedClosed = true
			targets = append(targets, sub)
		}
	}
	s.crdtMu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}
