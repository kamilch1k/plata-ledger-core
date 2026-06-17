package events

import "context"

// Publisher delivers events to a message bus. The production implementation is
// a Kafka producer (see internal/kafka); tests use an in-memory fake.
type Publisher interface {
	Publish(ctx context.Context, evs []Event) error
}

// EventSource is the relay's view of the outbox (satisfied by *Store).
type EventSource interface {
	FetchUnpublished(ctx context.Context, limit int) ([]Event, error)
	MarkPublished(ctx context.Context, ids []int64) error
}

// Relay drains the outbox to the Publisher.
type Relay struct {
	src   EventSource
	pub   Publisher
	batch int
}

func NewRelay(src EventSource, pub Publisher, batch int) *Relay {
	if batch <= 0 {
		batch = 100
	}
	return &Relay{src: src, pub: pub, batch: batch}
}

// PublishPending publishes one batch and returns how many were published.
//
// Delivery is at-least-once by design: events are published BEFORE being marked
// published, so a crash between the two re-publishes them on restart. That is
// why the bus is at-least-once and consumers must dedupe by event id — never
// mark-then-publish, which would silently drop events on a crash.
func (r *Relay) PublishPending(ctx context.Context) (int, error) {
	evs, err := r.src.FetchUnpublished(ctx, r.batch)
	if err != nil || len(evs) == 0 {
		return 0, err
	}
	if err := r.pub.Publish(ctx, evs); err != nil {
		return 0, err // not marked -> retried on the next tick
	}
	ids := make([]int64, len(evs))
	for i, e := range evs {
		ids[i] = e.ID
	}
	if err := r.src.MarkPublished(ctx, ids); err != nil {
		return 0, err // published but not marked -> re-published next tick (at-least-once)
	}
	return len(evs), nil
}
