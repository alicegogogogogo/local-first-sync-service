package crdt

import (
	"errors"
	"sync"
)

// subKey identifies the set of CRDT state subscriptions belonging to one
// device's view of one document: a state-changing commit fans out to every
// device set under the document, a permission revoke closes only the one
// (document, device) set.
type crdtSubKey struct {
	document string
	device   string
}

// Subscription is one in-memory, push-only subscription to a document's
// merged CRDT state. States that actually changed the merge are appended in
// transaction-commit order and handed to the connection in that order; the
// wake channel is merely a coalescing "drain the queue" signal. A permission
// revoke targeting the subscription's (document, device) pair is delivered as
// a separate, one-shot closed channel so it can neither be missed nor undone
// by a later grant on the same connection.
//
// A subscription leaves no durable trace: nothing is written, a disconnect
// simply unregisters, and stopping wakes every subscription once. Pushing
// never writes an operation and never moves the merge.
type Subscription struct {
	// mu guards queue.
	mu sync.Mutex
	// queue holds merged states in commit order the connection has not
	// drained yet. It is unbounded: every committed state change must reach
	// the subscriber exactly once and in order.
	queue []State
	// wakes is a 1-buffered "drain now" signal.
	wakes chan struct{}
	// revoked is closed once, the first time permission for the keyed pair is
	// revoked while this subscription is live. Closing is sticky.
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

// AddSubscription registers an in-memory subscription to documentID as seen by
// deviceID and returns it together with an idempotent unregister that a
// client disconnect, a permission close and a service stop may all race to
// call.
//
// Registration precedes the subscriber's first state read, so a
// state-changing commit landing between registration and that read is queued
// and cannot be missed. If the service is already stopping the wake is
// signaled immediately and unregister is a no-op.
func (s *Service) AddSubscription(documentID, deviceID string) (*Subscription, func()) {
	sub := &Subscription{
		wakes:   make(chan struct{}, 1),
		revoked: make(chan struct{}),
	}
	key := crdtSubKey{document: documentID, device: deviceID}

	s.subMu.Lock()
	defer s.subMu.Unlock()
	if s.closed {
		close(sub.wakes)
		return sub, func() {}
	}
	s.subWG.Add(1)
	s.nextSubID++
	id := s.nextSubID
	set := s.subs[key]
	if set == nil {
		set = make(map[uint64]*Subscription)
		s.subs[key] = set
	}
	set[id] = sub
	unregister := func() {
		s.subMu.Lock()
		if set, ok := s.subs[key]; ok {
			if _, ok := set[id]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(s.subs, key)
				}
				s.subMu.Unlock()
				s.subWG.Done()
				return
			}
		}
		s.subMu.Unlock()
	}
	return sub, unregister
}

// OpenSubscription atomically registers a state subscription and reads the
// document's current merged state. It returns:
//
//   - state: the current merged state, or nil when no CRDT operation has ever
//     been committed (the subscriber then waits silently);
//   - sub: the live subscription;
//   - unregister: idempotent removal.
//
// Registration and the read happen as one step relative to a SubmitOps commit
// — both hold the serialization mutex — so a state-changing commit lands
// either inside the returned initial state or in the subscription's queue,
// never both and never neither.
func (s *Service) OpenSubscription(documentID, deviceID string) (state *State, sub *Subscription, unregister func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub, unregister = s.AddSubscription(documentID, deviceID)
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

// notifySubscribers publishes a newly merged state to every subscription open
// on documentID. It is called only after a state-changing transaction has
// committed, so every pushed state is durable and states arrive in
// transaction-commit order.
func (s *Service) notifySubscribers(documentID string, state State) {
	s.subMu.Lock()
	var targets []*Subscription
	for key, set := range s.subs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	s.subMu.Unlock()

	for _, sub := range targets {
		sub.deliver(state)
	}
}

// PermissionRevoked closes the one-shot revoked channel of every CRDT
// subscription open on (documentID, deviceID), so the connection layer ends
// those subscriptions with the permission-closed code even if a grant races
// in afterward. It satisfies authz.RevokeSink; other devices' and documents'
// subscriptions are untouched.
func (s *Service) PermissionRevoked(documentID, deviceID string) {
	s.subMu.Lock()
	set := s.subs[crdtSubKey{document: documentID, device: deviceID}]
	targets := make([]*Subscription, 0, len(set))
	for _, sub := range set {
		if !sub.revokedClosed {
			sub.revokedClosed = true
			targets = append(targets, sub)
		}
	}
	s.subMu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// SignalDeviceDeregistered ends every live CRDT subscription belonging to
// deviceID across every document, after the device's deregistration
// transaction committed: the session that backed such a connection is already
// hard-deleted, so it closes immediately with the same permission-closed code
// a revoke uses, stickily. Other devices' subscriptions are untouched.
func (s *Service) SignalDeviceDeregistered(deviceID string) {
	s.subMu.Lock()
	var targets []*Subscription
	for key, set := range s.subs {
		if key.device != deviceID {
			continue
		}
		for _, sub := range set {
			if !sub.revokedClosed {
				sub.revokedClosed = true
				targets = append(targets, sub)
			}
		}
	}
	s.subMu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// Closing reports whether the service has begun stopping. A woken subscription
// uses it to distinguish a termination signal from a state change.
func (s *Service) Closing() bool {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	return s.closed
}

// InterruptWaits gives every live subscription one coalescing wake during
// shutdown and marks the service as stopping; the connection layer re-checks
// the flag and finishes each connection with a going-away close. Committed
// state is untouched.
func (s *Service) InterruptWaits() {
	s.subMu.Lock()
	s.closed = true
	var targets []*Subscription
	for _, set := range s.subs {
		for _, sub := range set {
			targets = append(targets, sub)
		}
	}
	s.subMu.Unlock()

	for _, sub := range targets {
		select {
		case sub.wakes <- struct{}{}:
		default:
		}
	}
}

// WaitDrained blocks until every subscription handler has unregistered.
func (s *Service) WaitDrained() {
	s.subWG.Wait()
}
