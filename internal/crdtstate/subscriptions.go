package crdtstate

import (
	"errors"
	"sync"
)

// subKey identifies the set of subscriptions belonging to one device's view
// of one document: a state-changing commit fans out to every device set under
// the document, a revoke closes only the one (document, device) set.
type subKey struct {
	document string
	device   string
}

// Subscription is one in-memory, push-only subscription to a document's
// merged CRDT state. States that actually changed the merge are appended in
// transaction-commit order and handed to the connection in that order; the
// wake channel is merely a coalescing "drain the queue" signal. A revoke
// targeting the subscription's (document, device) pair is delivered as a
// separate, one-shot closed channel so it can neither be missed nor undone by
// a later grant on the same connection.
//
// A subscription leaves no durable trace: nothing is written to the database,
// a disconnect simply unregisters, and shutting the service down wakes every
// subscription once (the server maps that to a going-away close). Pushing
// never writes an operation and never moves the merge; the subscription layer
// only observes committed states.
type Subscription struct {
	// mu guards queue.
	mu sync.Mutex
	// queue holds merged states in commit order that the connection has not
	// drained yet. It is unbounded: every committed state change must reach
	// the subscriber exactly once and in order, so a slow consumer is never
	// allowed to drop or coalesce a state.
	queue []State
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
func (x *Subscription) Wake() <-chan struct{} { return x.wakes }

// Revoked returns the one-shot revoke signal.
func (x *Subscription) Revoked() <-chan struct{} { return x.revoked }

// Drain removes and returns every queued merged state in commit order. It
// returns nil when the queue is empty.
func (x *Subscription) Drain() []State {
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
func (x *Subscription) deliver(state State) {
	x.mu.Lock()
	x.queue = append(x.queue, state)
	x.mu.Unlock()
	select {
	case x.wakes <- struct{}{}:
	default:
	}
}

// subRegistry is the in-memory set of live CRDT subscriptions. It is
// deliberately separate from the service lock: notification after a commit
// happens while the service lock is held, but registry bookkeeping and a
// disconnect never need it.
type subRegistry struct {
	mu     sync.Mutex
	subs   map[subKey]map[uint64]*Subscription
	nextID uint64
	closed bool
	wg     sync.WaitGroup
}

func newSubRegistry() *subRegistry {
	return &subRegistry{subs: make(map[subKey]map[uint64]*Subscription)}
}

// add registers one subscription for documentID as seen by deviceID and
// returns it together with an idempotent unregister. When the registry is
// already shutting down the subscription's wake is signaled immediately and
// unregister is a no-op, so a late subscriber ends at once with the
// going-away close instead of parking forever.
func (r *subRegistry) add(documentID, deviceID string) (*Subscription, func()) {
	sub := &Subscription{
		wakes:   make(chan struct{}, 1),
		revoked: make(chan struct{}),
	}
	key := subKey{document: documentID, device: deviceID}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		close(sub.wakes)
		return sub, func() {}
	}
	r.wg.Add(1)
	r.nextID++
	id := r.nextID
	set := r.subs[key]
	if set == nil {
		set = make(map[uint64]*Subscription)
		r.subs[key] = set
	}
	set[id] = sub
	unregister := func() {
		r.mu.Lock()
		if set, ok := r.subs[key]; ok {
			if _, ok := set[id]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(r.subs, key)
				}
				r.mu.Unlock()
				r.wg.Done()
				return
			}
		}
		r.mu.Unlock()
	}
	return sub, unregister
}

// notify publishes a newly merged state to every subscription open on
// documentID. It is called only after a state-changing transaction has
// committed, so every pushed state is durable and states arrive in
// transaction-commit order. Idempotent repeats, rejected batches and no-op
// additions never call it, so such commits push nothing.
func (r *subRegistry) notify(documentID string, state State) {
	r.mu.Lock()
	var targets []*Subscription
	for key, set := range r.subs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	r.mu.Unlock()

	for _, sub := range targets {
		sub.deliver(state)
	}
}

// revoke closes the one-shot revoked channel of every subscription open on
// (documentID, deviceID), so the server ends those subscriptions with the
// permission-closed code even if a grant races in afterward. Other devices'
// and other documents' subscriptions are untouched.
func (r *subRegistry) revoke(documentID, deviceID string) {
	r.mu.Lock()
	set := r.subs[subKey{document: documentID, device: deviceID}]
	targets := make([]*Subscription, 0, len(set))
	for _, sub := range set {
		if !sub.revokedClosed {
			sub.revokedClosed = true
			targets = append(targets, sub)
		}
	}
	r.mu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// beginShutdown marks the registry closed (a later add signals immediately)
// and gives every live subscription one coalescing wake; the server re-checks
// the closed flag and finishes each connection with a going-away close.
func (r *subRegistry) beginShutdown() {
	r.mu.Lock()
	r.closed = true
	var targets []*Subscription
	for _, set := range r.subs {
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	r.mu.Unlock()

	for _, sub := range targets {
		select {
		case sub.wakes <- struct{}{}:
		default:
		}
	}
}

func (r *subRegistry) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

// AddSubscription registers an in-memory subscription to documentID as seen
// by deviceID and returns it together with an idempotent unregister that a
// client disconnect, a permission close and a service shutdown may all race
// to call. Prefer OpenSubscription from a connection handler: it pairs the
// registration with an atomic initial-state read.
func (s *Service) AddSubscription(documentID, deviceID string) (*Subscription, func()) {
	return s.subs.add(documentID, deviceID)
}

// OpenSubscription atomically registers a subscription and reads the
// document's current merged state. It returns:
//
//   - state: the current merged state, or nil when no CRDT operation has ever
//     been committed for the document (the subscriber then waits silently);
//   - sub: the live subscription;
//   - unregister: idempotent removal.
//
// Registration and the read happen as one step relative to a Submit commit —
// both hold the service lock — so a state-changing commit lands either inside
// the returned initial state or in the subscription's queue, never in both
// and never neither; the subscriber therefore never misses the first state
// and never sends a queued state older than its initial one. When the service
// is already shutting down the wake is signaled immediately and unregister
// is a no-op, so the connection ends with a going-away close instead of
// parking.
func (s *Service) OpenSubscription(documentID, deviceID string) (state *State, sub *Subscription, unregister func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, unregister = s.subs.add(documentID, deviceID)
	current, getErr := s.GetState(documentID)
	switch {
	case errors.Is(getErr, ErrNotFound):
		return nil, sub, unregister, nil
	case getErr != nil:
		unregister()
		return nil, nil, func() {}, getErr
	}
	return &current, sub, unregister, nil
}

// OnRevoke is the boundary through which the permission service tells the
// CRDT service that a (document, device) pair was revoked: live subscriptions
// on the pair end stickily with the permission-closed signal. The composition
// root registers this on the permission service's revoke hook.
func (s *Service) OnRevoke(documentID, deviceID string) {
	s.subs.revoke(documentID, deviceID)
}

// InterruptWaits wakes every live subscription so an orderly shutdown drains
// its connections immediately (they finish with a going-away close) instead
// of holding shutdown hostage. Committed state is untouched.
func (s *Service) InterruptWaits() {
	s.subs.beginShutdown()
}

// Close interrupts live subscriptions and waits for every handler to
// unregister. The shared database handle itself is closed by its owner (the
// composition root), never by one service.
func (s *Service) Close() {
	s.subs.beginShutdown()
	s.subs.wg.Wait()
}

// Closing reports whether the service has begun shutting down. A woken
// subscription uses it to distinguish a termination signal from a commit.
func (s *Service) Closing() bool {
	return s.subs.isClosed()
}
