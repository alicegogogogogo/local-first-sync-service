// Package push holds the process-wide, in-memory registry of the live push
// connections — change subscriptions and CRDT state subscriptions alike.
//
// The registry is the management plane behind the device-scoped subscription
// list and cancel endpoints. It owns no durable state: a live entry exists
// exactly while its WebSocket connection does, so a client disconnect, an
// active cancel and a process restart all make the subscription vanish from
// every list immediately. Registration never writes a change, never advances
// a cursor and never writes a CRDT operation.
package push

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// Kind names one of the two push channels a subscription can ride.
type Kind string

const (
	// KindChanges is the change-log subscription: it replays committed
	// changes past a handshake cursor and fans out later commits.
	KindChanges Kind = "changes"
	// KindState is the CRDT state subscription: it pushes the current merged
	// state and every later merge that actually changes it; it carries no
	// cursor.
	KindState Kind = "state"
)

// Info is the immutable description of one live push connection. Cursor is the
// starting cursor requested at the handshake (0 for a state subscription,
// which has no cursor); EstablishedAt is the connection's establishment time
// as a Unix millisecond timestamp.
type Info struct {
	ID            string
	DeviceID      string
	DocumentID    string
	Kind          Kind
	Cursor        int64
	EstablishedAt int64
	seq           uint64
}

// Subscription is one live registry entry. Its one-shot Canceled channel is
// closed exactly when the device actively cancels this connection through the
// management endpoint; a revoke, a document deletion, a shutdown and a client
// disconnect never close it — those end the connection through their own
// service channels while CloseDocument/Unregister simply drop the entry.
type Subscription struct {
	info       Info
	cancel     chan struct{}
	cancelOnce sync.Once
}

// Info returns the subscription's immutable description.
func (s *Subscription) Info() Info { return s.info }

// Canceled returns the one-shot active-cancel signal.
func (s *Subscription) Canceled() <-chan struct{} { return s.cancel }

// Registry indexes every live push connection in the process. IDs are
// allocated here, so they are unique across both channels for the life of the
// process and are never reused after a connection leaves.
type Registry struct {
	mu   sync.Mutex
	next uint64
	live map[string]*Subscription
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{live: make(map[string]*Subscription)}
}

// Register records one freshly established push connection and allocates its
// process-unique subscription ID. cursor is the handshake starting cursor for
// a change subscription and must be 0 for a state subscription. The returned
// subscription must be removed with Unregister when the connection ends.
func (r *Registry) Register(deviceID, documentID string, kind Kind, cursor int64) *Subscription {
	r.mu.Lock()
	r.next++
	seq := r.next
	id := formatID(seq)
	sub := &Subscription{
		info: Info{
			ID:            id,
			DeviceID:      deviceID,
			DocumentID:    documentID,
			Kind:          kind,
			Cursor:        cursor,
			EstablishedAt: time.Now().UnixMilli(),
			seq:           seq,
		},
		cancel: make(chan struct{}),
	}
	r.live[id] = sub
	r.mu.Unlock()
	return sub
}

// Unregister removes a connection that has ended. It is idempotent: a client
// disconnect, an active cancel and a shutdown may all race to call it.
func (r *Registry) Unregister(id string) {
	r.mu.Lock()
	delete(r.live, id)
	r.mu.Unlock()
}

// List returns the descriptions of every live connection belonging to
// deviceID, in connection-establishment order (oldest first). Subscriptions
// established by the device directly at the document level and those
// established through sessions it owns all appear, because both flows
// register under the resolved owning device.
func (r *Registry) List(deviceID string) []Info {
	r.mu.Lock()
	out := make([]Info, 0)
	for _, sub := range r.live {
		if sub.info.DeviceID == deviceID {
			out = append(out, sub.info)
		}
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// Cancel actively ends the live connection identified by id, but only when it
// currently belongs to deviceID: it removes the entry at once and closes its
// cancel signal, after which the connection layer finishes it with the active
// cancel close code. It returns false without signaling anything when the
// subscription does not exist, has already ended, or belongs to another
// device — those cases are indistinguishable to the caller, which answers
// 404 in every one of them.
func (r *Registry) Cancel(deviceID, id string) bool {
	r.mu.Lock()
	sub, ok := r.live[id]
	if !ok || sub.info.DeviceID != deviceID {
		r.mu.Unlock()
		return false
	}
	// Leave no window in which an already-canceled subscription can still be
	// listed: remove before signaling.
	delete(r.live, id)
	r.mu.Unlock()

	sub.cancelOnce.Do(func() { close(sub.cancel) })
	return true
}

// CloseDocument drops every live entry — across both push channels and every
// owning device — subscribed to documentID, so those connections vanish from
// every subscription-management list at once. The connections themselves are
// ended through their own service channels (the document-deleted signal that
// carries the fixed close code), not through the active-cancel signal; this
// registry owns no durable state and holds no close code. Entries on other
// documents are untouched.
func (r *Registry) CloseDocument(documentID string) {
	r.mu.Lock()
	var ids []string
	for id, sub := range r.live {
		if sub.info.DocumentID == documentID {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		delete(r.live, id)
	}
	r.mu.Unlock()
}

// formatID renders the monotonic sequence as the subscription id. The prefix
// marks the token's origin in logs while the decimal stays unique within the
// process; ids are never reused.
func formatID(seq uint64) string {
	return "sub-" + strconv.FormatUint(seq, 10)
}
