// Package events implements a transactional outbox: domain events are written
// to the events table in the same transaction as the state change that produced
// them, then a relay publishes them to a message bus (Kafka in production)
// at-least-once. Consumers dedupe by event id.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventType string

const (
	TransferPosted       EventType = "transfer.posted"
	ApplicationSubmitted EventType = "application.submitted"
	ApplicationKYCPassed EventType = "application.kyc_passed"
	ApplicationKYCFailed EventType = "application.kyc_failed"
	ApplicationScored    EventType = "application.scored"
	ApplicationApproved  EventType = "application.approved"
	ApplicationDeclined  EventType = "application.declined"
	ApplicationDisbursed EventType = "application.disbursed"
)

// Event is an immutable record of something that happened to an aggregate.
type Event struct {
	ID          int64           `json:"id"`
	AggregateID string          `json:"aggregate_id"`
	Type        EventType       `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
}

const schema = `
CREATE TABLE IF NOT EXISTS events (
	id           BIGSERIAL PRIMARY KEY,
	aggregate_id TEXT        NOT NULL,
	type         TEXT        NOT NULL,
	payload      JSONB       NOT NULL,
	created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
	published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events (aggregate_id, id);
CREATE INDEX IF NOT EXISTS idx_events_unpublished ON events (id) WHERE published_at IS NULL;
`

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}

// AppendTx writes an event inside an existing transaction. This is the heart of
// the transactional outbox: the event commits atomically with the state change.
func AppendTx(ctx context.Context, tx pgx.Tx, aggregateID string, t EventType, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO events (aggregate_id, type, payload) VALUES ($1, $2, $3)`,
		aggregateID, string(t), b)
	return err
}

// Store reads and marks events for the relay, and replays them for projections.
type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func scanEvents(rows pgx.Rows) ([]Event, error) {
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.Type, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// FetchUnpublished returns the oldest events not yet published, in order.
func (s *Store) FetchUnpublished(ctx context.Context, limit int) ([]Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, aggregate_id, type, payload, created_at
		 FROM events WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}

// MarkPublished marks events as published. Called only after a successful publish.
func (s *Store) MarkPublished(ctx context.Context, ids []int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE events SET published_at = now() WHERE id = ANY($1)`, ids)
	return err
}

// LoadByAggregate returns all events for an aggregate, in order — used to fold
// an event-sourced projection.
func (s *Store) LoadByAggregate(ctx context.Context, aggregateID string) ([]Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, aggregate_id, type, payload, created_at
		 FROM events WHERE aggregate_id = $1 ORDER BY id`, aggregateID)
	if err != nil {
		return nil, err
	}
	return scanEvents(rows)
}
