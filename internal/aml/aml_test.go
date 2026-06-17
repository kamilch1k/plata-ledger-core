package aml

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/kamilch1k/plata-ledger-core/internal/events"
)

const testPort = 54331

var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(testPort).Logger(io.Discard),
	)
	if err := pg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", testPort)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		_ = pg.Stop()
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := New(pool).Migrate(ctx); err != nil {
		pool.Close()
		_ = pg.Stop()
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	_ = pg.Stop()
	os.Exit(code)
}

func freshConsumer(t *testing.T) *Consumer {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`TRUNCATE processed_events, aml_account_counters, aml_alerts RESTART IDENTITY`)
	require.NoError(t, err)
	return New(testPool)
}

func transferEvent(id int64, from string, amount int64) events.Event {
	payload, _ := json.Marshal(map[string]any{
		"transfer_id": fmt.Sprintf("txn_%d", id), "from_account": from, "amount_minor": amount,
	})
	return events.Event{ID: id, AggregateID: fmt.Sprintf("txn_%d", id), Type: events.TransferPosted, Payload: payload}
}

func TestAmountRule_FiresOnBoundary(t *testing.T) {
	ctx := context.Background()

	c := freshConsumer(t)
	n, err := c.Process(ctx, []events.Event{transferEvent(1, "A", AmountThresholdMinor)})
	require.NoError(t, err)
	require.Equal(t, 1, n, "amount exactly at the threshold should alert")

	c = freshConsumer(t)
	n, err = c.Process(ctx, []events.Event{transferEvent(1, "A", AmountThresholdMinor-1)})
	require.NoError(t, err)
	require.Equal(t, 0, n, "amount just below the threshold should not alert")
}

func TestVelocityRule_FiresAtThreshold(t *testing.T) {
	ctx := context.Background()
	c := freshConsumer(t)

	var batch []events.Event
	for i := int64(1); i <= int64(VelocityThreshold); i++ {
		batch = append(batch, transferEvent(i, "A", 100)) // small amounts, below the amount rule
	}
	n, err := c.Process(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, 1, n, "one velocity alert when the account reaches the threshold")

	alerts, _ := c.Alerts(ctx)
	require.Len(t, alerts, 1)
	require.Equal(t, "velocity", alerts[0].Rule)
}

func TestReplayDeterminism_DedupesByEventID(t *testing.T) {
	ctx := context.Background()
	c := freshConsumer(t)

	batch := []events.Event{
		transferEvent(1, "A", 100),
		transferEvent(2, "A", AmountThresholdMinor), // large
		transferEvent(3, "B", 200),
	}
	first, err := c.Process(ctx, batch)
	require.NoError(t, err)

	// Replaying the exact same events (as an at-least-once bus would) raises
	// nothing new and leaves counters unchanged.
	second, err := c.Process(ctx, batch)
	require.NoError(t, err)
	require.Equal(t, 0, second, "replayed events must not raise new alerts")

	alerts, _ := c.Alerts(ctx)
	require.Len(t, alerts, first, "alert set is stable across replays")

	var countA int
	require.NoError(t, testPool.QueryRow(ctx,
		`SELECT transfer_count FROM aml_account_counters WHERE account = 'A'`).Scan(&countA))
	require.Equal(t, 2, countA, "A's counter reflects 2 unique transfers, not 4")
}
