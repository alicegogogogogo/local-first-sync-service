package push

import (
	"testing"
	"time"
)

func TestRegisterAssignsUniqueIDsAndMetadata(t *testing.T) {
	r := NewRegistry()

	a := r.Register("dev-1", "doc-a", KindChanges, 7)
	b := r.Register("dev-1", "doc-b", KindState, 0)
	c := r.Register("dev-2", "doc-a", KindChanges, 0)

	if a.Info().ID == "" || a.Info().ID == b.Info().ID || b.Info().ID == c.Info().ID {
		t.Fatalf("ids must be unique and non-empty: %q %q %q", a.Info().ID, b.Info().ID, c.Info().ID)
	}
	if got := a.Info(); got.DeviceID != "dev-1" || got.DocumentID != "doc-a" || got.Kind != KindChanges || got.Cursor != 7 {
		t.Fatalf("unexpected metadata: %+v", got)
	}
	if got := b.Info(); got.Kind != KindState || got.Cursor != 0 {
		t.Fatalf("state subscription must record cursor 0: %+v", got)
	}
	now := time.Now().UnixMilli()
	if ts := a.Info().EstablishedAt; ts <= 0 || ts > now+1000 || ts < now-60_000 {
		t.Fatalf("establishedAt %d is not a current unix-milli timestamp", ts)
	}
}

func TestListIsFilteredAndInEstablishmentOrder(t *testing.T) {
	r := NewRegistry()

	first := r.Register("dev-1", "doc-a", KindChanges, 1)
	middle := r.Register("dev-2", "doc-a", KindState, 0) // another device, must be filtered
	third := r.Register("dev-1", "doc-b", KindState, 0)
	fourth := r.Register("dev-1", "doc-c", KindChanges, 9)

	got := r.List("dev-1")
	wantIDs := []string{first.Info().ID, third.Info().ID, fourth.Info().ID}
	if len(got) != len(wantIDs) {
		t.Fatalf("list = %d entries, want %d", len(got), len(wantIDs))
	}
	for i, want := range wantIDs {
		if got[i].ID != want {
			t.Fatalf("entry %d = %q, want %q (order: %v)", i, got[i].ID, want, wantIDs)
		}
	}
	if listed := r.List("dev-2"); len(listed) != 1 || listed[0].ID != middle.Info().ID {
		t.Fatalf("dev-2 list = %+v, want only %q", listed, middle.Info().ID)
	}
	if got := r.List("ghost"); len(got) != 0 {
		t.Fatalf("unknown device list = %+v, want empty", got)
	}
}

func TestUnregisterRemovesEntry(t *testing.T) {
	r := NewRegistry()
	sub := r.Register("dev-1", "doc", KindChanges, 0)

	if got := r.List("dev-1"); len(got) != 1 {
		t.Fatalf("live entry missing: %+v", got)
	}
	r.Unregister(sub.Info().ID)
	if got := r.List("dev-1"); len(got) != 0 {
		t.Fatalf("entry survived unregister: %+v", got)
	}
	// Idempotent: a racing teardown may remove the same connection twice.
	r.Unregister(sub.Info().ID)
}

func TestCancelSignalsAndRemovesOnce(t *testing.T) {
	r := NewRegistry()
	sub := r.Register("dev-1", "doc", KindChanges, 3)

	if !r.Cancel("dev-1", sub.Info().ID) {
		t.Fatal("first cancel must succeed")
	}
	select {
	case <-sub.Canceled():
	case <-time.After(time.Second):
		t.Fatal("cancel did not close the signal channel")
	}
	if got := r.List("dev-1"); len(got) != 0 {
		t.Fatalf("canceled subscription still listed: %+v", got)
	}
	if r.Cancel("dev-1", sub.Info().ID) {
		t.Fatal("repeated cancel must report not-found")
	}
}

func TestCancelRejectsUnknownAndOtherDevice(t *testing.T) {
	r := NewRegistry()
	sub := r.Register("dev-1", "doc", KindChanges, 0)

	if r.Cancel("dev-1", "sub-ghost") {
		t.Fatal("an unknown id must not cancel")
	}
	if r.Cancel("dev-2", sub.Info().ID) {
		t.Fatal("another device must not cancel a subscription it does not own")
	}
	// The rejected foreign cancel leaves the connection live and unsignaled.
	if got := r.List("dev-1"); len(got) != 1 || got[0].ID != sub.Info().ID {
		t.Fatalf("foreign cancel disturbed the entry: %+v", got)
	}
	select {
	case <-sub.Canceled():
		t.Fatal("foreign cancel must not signal the connection")
	default:
	}
}
