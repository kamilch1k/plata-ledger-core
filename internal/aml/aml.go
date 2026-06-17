// Package aml is a streaming transaction-monitoring consumer. It reads
// transfer events, applies fraud/AML rules, and raises alerts. Processing is
// idempotent: every event is deduped by id, so replaying the stream (which an
// at-least-once bus will do) never double-counts or double-alerts.
package aml

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kamilch1k/plata-ledger-core/internal/events"
)

const (
	// AmountThresholdMinor flags a single large transfer (e.g. $10,000).
	AmountThresholdMinor int64 = 1_000_000
	// VelocityThreshold flags an account once it reaches this many transfers.
	VelocityThreshold int = 5
)

type Alert struct {
	ID        int64           `json:"id"`
	EventID   int64           `json:"event_id"`
	Account   string          `json:"account"`
	Rule      string          `json:"rule"`
	Detail    json.RawMessage `json:"detail"`
	CreatedAt time.Time       `json:"created_at"`
}

type transferPayload struct {
	TransferID  string `json:"transfer_id"`
	FromAccount string `json:"from_account"`
	AmountMinor int64  `json:"amount_minor"`
}

type Consumer struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Consumer { return &Consumer{pool: pool} }

const schema = `
CREATE TABLE IF NOT EXISTS processed_events (
	event_id     BIGINT PRIMARY KEY,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS aml_account_counters (
	account        TEXT PRIMARY KEY,
	transfer_count INT  NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS aml_alerts (
	id         BIGSERIAL PRIMARY KEY,
	event_id   BIGINT NOT NULL,
	account    TEXT   NOT NULL,
	rule       TEXT   NOT NULL,
	detail     JSONB  NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func (c *Consumer) Migrate(ctx context.Context) error {
	_, err := c.pool.Exec(ctx, schema)
	return err
}

// Process applies the rules to a batch of events and returns the number of new
// alerts raised. Each event's dedup + effects happen in one transaction.
func (c *Consumer) Process(ctx context.Context, evs []events.Event) (int, error) {
	raised := 0
	for _, e := range evs {
		n, err := c.processOne(ctx, e)
		if err != nil {
			return raised, err
		}
		raised += n
	}
	return raised, nil
}

func (c *Consumer) processOne(ctx context.Context, e events.Event) (int, error) {
	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Dedup: claim the event id. If already processed, skip entirely.
	claim, err := tx.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, e.ID)
	if err != nil {
		return 0, err
	}
	if claim.RowsAffected() == 0 {
		return 0, nil
	}

	raised := 0
	if e.Type == events.TransferPosted {
		var p transferPayload
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return 0, err
		}

		if p.AmountMinor >= AmountThresholdMinor {
			if err := insertAlert(ctx, tx, e.ID, p.FromAccount, "large_transfer",
				map[string]any{"amount_minor": p.AmountMinor, "threshold_minor": AmountThresholdMinor}); err != nil {
				return 0, err
			}
			raised++
		}

		var count int
		if err := tx.QueryRow(ctx,
			`INSERT INTO aml_account_counters (account, transfer_count) VALUES ($1, 1)
			 ON CONFLICT (account) DO UPDATE SET transfer_count = aml_account_counters.transfer_count + 1
			 RETURNING transfer_count`, p.FromAccount).Scan(&count); err != nil {
			return 0, err
		}
		if count == VelocityThreshold {
			if err := insertAlert(ctx, tx, e.ID, p.FromAccount, "velocity",
				map[string]any{"transfer_count": count}); err != nil {
				return 0, err
			}
			raised++
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return raised, nil
}

func insertAlert(ctx context.Context, tx pgx.Tx, eventID int64, account, rule string, detail any) error {
	b, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx,
		`INSERT INTO aml_alerts (event_id, account, rule, detail) VALUES ($1, $2, $3, $4)`,
		eventID, account, rule, b)
	return err
}

// Alerts returns all raised alerts, oldest first (for inspection and tests).
func (c *Consumer) Alerts(ctx context.Context) ([]Alert, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT id, event_id, account, rule, detail, created_at FROM aml_alerts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Alert
	for rows.Next() {
		var a Alert
		if err := rows.Scan(&a.ID, &a.EventID, &a.Account, &a.Rule, &a.Detail, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
