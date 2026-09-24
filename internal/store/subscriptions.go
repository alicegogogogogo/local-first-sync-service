package store

// subKey identifies the set of subscriptions belonging to one device's view
// of one document: change commits wake every device set under the document,
// a permission revoke closes only the one (document, device) set.
type subKey struct {
	document string
	device   string
}

// Subscriptions are the in-memory, push-only counterpart of long polling.
//
// A subscription belongs to a (document, device) pair — the device is the one
// owning the session that opened the connection. It receives one coalesceable
// signal per change-producing commit (PostChanges, ReplayChanges,
// MergeChange, RestoreSnapshot all signal through the same hook long polling
// uses). A permission revoke targeting the pair is delivered as a separate,
// one-shot closed channel so the event cannot be missed or undone by a later
// grant on the same connection.
//
// Like parked long polls, subscriptions leave no durable trace: nothing is
// written to the database, a disconnect simply unregisters, and closing the
// store broadcasts one final wakeup (the server maps it to a going-away
// close). Pushing never allocates a cursor or inserts a change row; the
// subscription layer only observes commits.
type subscription struct {
	// wakes receives one value on every change commit for the keyed
	// document. It is buffered (and signals coalesce) so a commit is never
	// dropped or blocked while the consumer is reading the log.
	wakes chan struct{}
	// revoked is closed once, the first time permission for the keyed pair
	// is revoked while this subscription is live. Closing is sticky: a later
	// grant never reopens it, so a subscription that was revoked always ends.
	revoked       chan struct{}
	revokedClosed bool
}

func (s *Store) initSubscriptionsLocked() {
	if s.subs == nil {
		s.subs = make(map[subKey]map[uint64]*subscription)
	}
}

// AddSubscription registers an in-memory subscription for documentID as seen
// by deviceID. It returns:
//
//   - wakes: signaled (coalesceably) after every change commit on documentID;
//   - revoked: closed once if/when permission for (documentID, deviceID) is
//     revoked while the subscription is live;
//   - unregister: idempotent removal; a client disconnect, a permission close
//     and a store close may all race to call it.
//
// If the store is already closing the wake channel is signaled immediately
// and unregister is a no-op, so a late subscriber parks on nothing.
func (s *Store) AddSubscription(documentID, deviceID string) (wakes <-chan struct{}, revoked <-chan struct{}, unregister func()) {
	ch := make(chan struct{}, 1)
	sub := &subscription{wakes: ch, revoked: make(chan struct{})}
	key := subKey{document: documentID, device: deviceID}

	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	if s.closed {
		close(ch)
		return ch, sub.revoked, func() {}
	}
	s.subWG.Add(1)
	s.initSubscriptionsLocked()
	s.nextSubID++
	id := s.nextSubID
	set := s.subs[key]
	if set == nil {
		set = make(map[uint64]*subscription)
		s.subs[key] = set
	}
	set[id] = sub
	remove := func() {
		s.pollMu.Lock()
		if set, ok := s.subs[key]; ok {
			if _, ok := set[id]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(s.subs, key)
				}
				s.pollMu.Unlock()
				s.subWG.Done()
				return
			}
		}
		s.pollMu.Unlock()
	}
	return ch, sub.revoked, remove
}

// Closing reports whether the store has begun shutting down. A woken
// subscription uses it to distinguish a termination signal from a commit.
func (s *Store) Closing() bool {
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	return s.closed
}

// signalSubscribers wakes every subscription open on documentID. It is called
// only after a change-bearing transaction has committed, so when a woken
// subscription re-reads the log the new rows are visible. Signals are
// non-blocking because the channels are buffered and the consumer always
// re-reads from its last observed cursor.
func (s *Store) signalSubscribers(documentID string) {
	s.pollMu.Lock()
	var targets []chan struct{}
	for key, set := range s.subs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			targets = append(targets, sub.wakes)
		}
	}
	s.pollMu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- struct{}{}:
		default:
			// A signal the consumer has not drained yet is already pending;
			// the pending wake covers this commit too because the consumer
			// re-reads from its last observed cursor.
		}
	}
}

// signalRevokedSubscribers closes the one-shot revoked channel of every
// subscription open on (documentID, deviceID), so the server ends those
// subscriptions with the permission-closed code even if a grant races in
// afterward. Other devices' and other documents' subscriptions are untouched.
func (s *Store) signalRevokedSubscribers(documentID, deviceID string) {
	s.pollMu.Lock()
	set := s.subs[subKey{document: documentID, device: deviceID}]
	targets := make([]*subscription, 0, len(set))
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
