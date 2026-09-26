package events

// subKey identifies the set of subscriptions belonging to one device's view
// of one document: change commits wake every device set under the document,
// a permission revoke closes only the one (document, device) set.
type subKey struct {
	document string
	device   string
}

// subscription is one in-memory, push-only observer of the change log.
//
// It belongs to a (document, device) pair — the device is the one owning the
// session that opened the connection. It receives one coalesceable signal per
// change-producing commit (PostChanges, ReplayChanges, MergeChange and
// RestoreSnapshot all signal through the same hook long polling uses). The
// permission service closes the separate one-shot revoked channel when it
// revokes the keyed pair; closing is sticky, so a later grant never reopens
// it.
//
// A subscription leaves no durable trace: nothing is written, a disconnect
// simply unregisters, and a stop broadcasts one final wakeup. Pushing never
// allocates a cursor or inserts a change row.
type subscription struct {
	// wakes receives one value on every change commit for the keyed document.
	// It is buffered (and signals coalesce) so a commit is never dropped or
	// blocked while the consumer is reading the log.
	wakes chan struct{}
	// revoked is closed once, the first time permission for the keyed pair is
	// revoked while this subscription is live. Closing is sticky.
	revoked       chan struct{}
	revokedClosed bool
}

// AddSubscription registers an in-memory subscription for documentID as seen
// by deviceID. It returns:
//
//   - wakes: signaled (coalesceably) after every change commit on documentID;
//   - revoked: closed once if/when permission for (documentID, deviceID) is
//     revoked while the subscription is live;
//   - unregister: idempotent removal; a client disconnect, a permission close
//     and a service stop may all race to call it.
//
// If the service is already stopping the wake channel is signaled immediately
// and unregister is a no-op, so a late subscriber parks on nothing.
func (s *Service) AddSubscription(documentID, deviceID string) (wakes <-chan struct{}, revoked <-chan struct{}, unregister func()) {
	ch := make(chan struct{}, 1)
	sub := &subscription{wakes: ch, revoked: make(chan struct{})}
	key := subKey{document: documentID, device: deviceID}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		close(ch)
		return ch, sub.revoked, func() {}
	}
	s.subWG.Add(1)
	s.nextSub++
	id := s.nextSub
	set := s.subs[key]
	if set == nil {
		set = make(map[uint64]*subscription)
		s.subs[key] = set
	}
	set[id] = sub
	remove := func() {
		s.mu.Lock()
		if set, ok := s.subs[key]; ok {
			if _, ok := set[id]; ok {
				delete(set, id)
				if len(set) == 0 {
					delete(s.subs, key)
				}
				s.mu.Unlock()
				s.subWG.Done()
				return
			}
		}
		s.mu.Unlock()
	}
	return ch, sub.revoked, remove
}

// signalSubscribers wakes every subscription open on documentID. It is called
// only after a change-bearing transaction has committed, so when a woken
// subscription re-reads the log the new rows are visible.
func (s *Service) signalSubscribers(documentID string) {
	s.mu.Lock()
	var targets []chan struct{}
	for key, set := range s.subs {
		if key.document != documentID {
			continue
		}
		for _, sub := range set {
			targets = append(targets, sub.wakes)
		}
	}
	s.mu.Unlock()

	for _, ch := range targets {
		select {
		case ch <- struct{}{}:
		default:
			// A signal the consumer has not drained yet is already pending; the
			// pending wake covers this commit too because the consumer re-reads
			// from its last observed cursor.
		}
	}
}

// SignalRevoked closes the one-shot revoked channel of every subscription open
// on (documentID, deviceID), so the connection layer ends those subscriptions
// with the permission-closed code even if a grant races in afterward. Other
// devices' and other documents' subscriptions are untouched. It is the
// permission service's revoke hook for the change event service.
func (s *Service) SignalRevoked(documentID, deviceID string) {
	s.mu.Lock()
	set := s.subs[subKey{document: documentID, device: deviceID}]
	targets := make([]*subscription, 0, len(set))
	for _, sub := range set {
		if !sub.revokedClosed {
			sub.revokedClosed = true
			targets = append(targets, sub)
		}
	}
	s.mu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// SignalDeviceGone closes the one-shot revoked channel of every subscription
// owned by deviceID across all documents, so a deregistered device's
// connections end immediately with the permission-closed close. It is the
// device-deregistration counterpart of SignalRevoked; other devices'
// subscriptions are untouched.
func (s *Service) SignalDeviceGone(deviceID string) {
	s.mu.Lock()
	targets := make([]*subscription, 0)
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
	s.mu.Unlock()

	for _, sub := range targets {
		close(sub.revoked)
	}
}

// Closing reports whether the service has begun stopping. A woken subscription
// uses it to distinguish a termination signal from a commit.
func (s *Service) Closing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// interruptWaits wakes every parked long poll without closing the database, so
// an orderly shutdown drains waiting connections immediately (they answer 503)
// instead of holding shutdown hostage until their wait deadline. Committed
// state is untouched. Live subscriptions get one last wakeup as well; the
// connection layer re-checks the closed flag and finishes with a going-away
// close.
func (s *Service) InterruptWaits() {
	s.mu.Lock()
	s.closed = true
	waits := s.waits
	s.waits = map[string]map[uint64]chan struct{}{}
	// Snapshot the live subscription channels without reaping the registry:
	// unregister remains valid while handlers drain, so the WaitGroup in
	// shutdown balances. New AddSubscription calls fail fast on closed.
	var subChans []chan struct{}
	for _, set := range s.subs {
		for _, sub := range set {
			subChans = append(subChans, sub.wakes)
		}
	}
	s.mu.Unlock()
	for _, set := range waits {
		for _, ch := range set {
			close(ch)
		}
	}
	for _, ch := range subChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// waitSubscriptions blocks until every subscription handler has unregistered.
func (s *Service) WaitDrained() {
	s.subWG.Wait()
}
