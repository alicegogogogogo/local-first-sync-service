package app

import (
	"sort"
	"strconv"
	"sync"
	"time"
)

// Push channel kinds a live subscription belongs to. The values mirror the
// two subscribe path shapes: the change-log push (.../changes/subscribe) and
// the merged-CRDT-state push (.../crdt/state/subscribe).
const (
	PushChannelChanges = "changes"
	PushChannelState   = "state"
)

// PushSubscriptionInfo is the management view of one live push connection.
// It carries no durable state: an entry exists exactly while its connection is
// open and vanishes on a client disconnect, an active cancellation or a
// process restart.
type PushSubscriptionInfo struct {
	// ID is allocated by the server when the connection is established and is
	// never reused within the process.
	ID string
	// DeviceID owns the connection: the device a document-level subscription
	// declared, or the device that owns the session a session-level
	// subscription used.
	DeviceID string
	// DocumentID is the document the connection observes.
	DocumentID string
	// Channel is PushChannelChanges or PushChannelState.
	Channel string
	// Cursor is the starting cursor requested in the handshake. A state
	// subscription always carries 0.
	Cursor int64
	// EstablishedAt is when the connection was established.
	EstablishedAt time.Time

	// seq orders entries by connection establishment. It is the registry's
	// monotonic allocation counter and is not exposed.
	seq uint64
}

// livePushSub is one registry row: the public info plus the one-shot channel
// the active-cancel entry closes to end the connection with 4410.
type livePushSub struct {
	info   PushSubscriptionInfo
	cancel chan struct{}
}

// pushRegistry is the process-wide, in-memory registry of live push
// subscriptions shared by both push channels. It allocates one unbroken
// sequence of subscription ids, answers a device's live-subscription view and
// ends a single connection on an active cancellation. It never touches the
// database: nothing is persisted, no change cursor is consumed, and a restart
// starts with an empty registry.
type pushRegistry struct {
	mu   sync.Mutex
	seq  uint64
	live map[string]*livePushSub
}

func newPushRegistry() *pushRegistry {
	return &pushRegistry{live: make(map[string]*livePushSub)}
}

// register records one newly established connection owned by deviceID and
// returns its server-allocated id, its establishment timestamp, the one-shot
// channel closed on an active cancellation, and an idempotent unregister the
// connection pump calls on exit (client disconnect, revoke, shutdown or
// cancel).
func (r *pushRegistry) register(deviceID, documentID, channel string, cursor int64) (id string, establishedAt time.Time, canceled <-chan struct{}, unregister func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seq++
	seq := r.seq
	sub := &livePushSub{
		info: PushSubscriptionInfo{
			ID:            strconv.FormatUint(seq, 10),
			DeviceID:      deviceID,
			DocumentID:    documentID,
			Channel:       channel,
			Cursor:        cursor,
			EstablishedAt: time.Now(),
			seq:           seq,
		},
		cancel: make(chan struct{}),
	}
	r.live[sub.info.ID] = sub

	remove := func() {
		r.mu.Lock()
		// An active cancellation has already removed and signaled the row, so a
		// racing pump exit is a no-op.
		if current, ok := r.live[sub.info.ID]; ok && current == sub {
			delete(r.live, sub.info.ID)
		}
		r.mu.Unlock()
	}
	return sub.info.ID, sub.info.EstablishedAt, sub.cancel, remove
}

// list returns every live subscription owned by deviceID in connection
// establishment order, across both push channels.
func (r *pushRegistry) list(deviceID string) []PushSubscriptionInfo {
	r.mu.Lock()
	out := make([]PushSubscriptionInfo, 0)
	for _, sub := range r.live {
		if sub.info.DeviceID == deviceID {
			out = append(out, sub.info)
		}
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].seq < out[j].seq })
	return out
}

// cancel ends the one live subscription id owned by deviceID: its cancellation
// channel is closed exactly once (the connection pump answers 4410) and the
// row leaves the registry immediately. It reports false without leaking
// anything when the id does not exist, has already ended, or belongs to
// another device; a repeated cancellation therefore looks just like a missing
// subscription.
func (r *pushRegistry) cancel(deviceID, id string) bool {
	r.mu.Lock()
	sub, ok := r.live[id]
	if !ok || sub.info.DeviceID != deviceID {
		r.mu.Unlock()
		return false
	}
	delete(r.live, id)
	ch := sub.cancel
	r.mu.Unlock()

	close(ch)
	return true
}

// RegisterPushSubscription delegates to the shared live-subscription registry.
func (a *App) RegisterPushSubscription(deviceID, documentID, channel string, cursor int64) (id string, establishedAt time.Time, canceled <-chan struct{}, unregister func()) {
	return a.pushSubs.register(deviceID, documentID, channel, cursor)
}

// ListPushSubscriptions delegates to the shared live-subscription registry.
func (a *App) ListPushSubscriptions(deviceID string) []PushSubscriptionInfo {
	return a.pushSubs.list(deviceID)
}

// CancelPushSubscription delegates to the shared live-subscription registry.
func (a *App) CancelPushSubscription(deviceID, id string) bool {
	return a.pushSubs.cancel(deviceID, id)
}
