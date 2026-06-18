// Package kafka adapts the events.Publisher interface and the AML consumer to a
// real Kafka broker (segmentio/kafka-go). It is wired in cmd/server only when
// KAFKA_BROKERS is set, and is verified manually against a live broker — it is
// excluded from CI coverage, like any adapter that needs external infrastructure.
package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"github.com/kamilch1k/ledgerline/internal/events"
)

// Publisher implements events.Publisher by writing events to a Kafka topic.
type Publisher struct {
	w *kafka.Writer
}

func NewPublisher(brokers []string, topic string) *Publisher {
	return &Publisher{w: &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}}
}

// Publish writes the batch, keyed by aggregate id so per-aggregate ordering is
// preserved on a partition.
func (p *Publisher) Publish(ctx context.Context, evs []events.Event) error {
	msgs := make([]kafka.Message, len(evs))
	for i, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		msgs[i] = kafka.Message{Key: []byte(e.AggregateID), Value: b}
	}
	return p.w.WriteMessages(ctx, msgs...)
}

func (p *Publisher) Close() error { return p.w.Close() }

// Consumer reads events from a Kafka topic and hands each to a handler. The
// handler is expected to dedupe (the AML consumer does), since delivery is
// at-least-once.
type Consumer struct {
	r *kafka.Reader
}

func NewConsumer(brokers []string, topic, group string) *Consumer {
	return &Consumer{r: kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: group,
	})}
}

// Run consumes until ctx is cancelled or a read error occurs.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, events.Event) error) error {
	for {
		m, err := c.r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		var e events.Event
		if err := json.Unmarshal(m.Value, &e); err != nil {
			continue // skip malformed messages rather than stalling the group
		}
		if err := handle(ctx, e); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error { return c.r.Close() }
