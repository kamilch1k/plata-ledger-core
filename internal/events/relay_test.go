package events

import (
	"context"
	"errors"
	"testing"
)

// fakeSource is an in-memory outbox for testing relay behaviour without a DB.
type fakeSource struct {
	events        []Event
	published     map[int64]bool
	failMarkOnce  bool
	markCallCount int
}

func newFakeSource(n int) *fakeSource {
	evs := make([]Event, n)
	for i := 0; i < n; i++ {
		evs[i] = Event{ID: int64(i + 1), AggregateID: "agg", Type: TransferPosted}
	}
	return &fakeSource{events: evs, published: map[int64]bool{}}
}

func (f *fakeSource) FetchUnpublished(_ context.Context, limit int) ([]Event, error) {
	var out []Event
	for _, e := range f.events {
		if !f.published[e.ID] {
			out = append(out, e)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeSource) MarkPublished(_ context.Context, ids []int64) error {
	f.markCallCount++
	if f.failMarkOnce {
		f.failMarkOnce = false
		return errors.New("simulated crash before commit")
	}
	for _, id := range ids {
		f.published[id] = true
	}
	return nil
}

// recordingPublisher records every event id it is asked to publish.
type recordingPublisher struct{ got []int64 }

func (p *recordingPublisher) Publish(_ context.Context, evs []Event) error {
	for _, e := range evs {
		p.got = append(p.got, e.ID)
	}
	return nil
}

func TestRelay_PublishesThenStops(t *testing.T) {
	src := newFakeSource(5)
	pub := &recordingPublisher{}
	relay := NewRelay(src, pub, 100)

	n, err := relay.PublishPending(context.Background())
	if err != nil || n != 5 {
		t.Fatalf("first drain: n=%d err=%v", n, err)
	}
	n, _ = relay.PublishPending(context.Background())
	if n != 0 {
		t.Fatalf("second drain should publish nothing, got %d", n)
	}
	if len(pub.got) != 5 {
		t.Fatalf("publisher saw %d events, want 5", len(pub.got))
	}
}

// A crash between publish and mark must re-publish (at-least-once), never drop.
func TestRelay_AtLeastOnce_OnCrashBeforeMark(t *testing.T) {
	src := newFakeSource(3)
	src.failMarkOnce = true
	pub := &recordingPublisher{}
	relay := NewRelay(src, pub, 100)

	// First attempt: publish succeeds, mark "crashes".
	if _, err := relay.PublishPending(context.Background()); err == nil {
		t.Fatal("expected mark to fail on the simulated crash")
	}
	// Restart: the same events are still unpublished and get re-published.
	n, err := relay.PublishPending(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("recovery drain: n=%d err=%v", n, err)
	}
	// At-least-once: the 3 events were delivered twice (consumers must dedupe).
	if len(pub.got) != 6 {
		t.Fatalf("publisher saw %d deliveries, want 6 (at-least-once)", len(pub.got))
	}
}
