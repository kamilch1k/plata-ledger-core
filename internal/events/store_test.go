package events

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testPort = 54332

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
	pool, err := pgxpool.New(ctx, fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", testPort))
	if err != nil {
		_ = pg.Stop()
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	if err := Migrate(ctx, pool); err != nil {
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

func appendEvent(t *testing.T, aggregateID string, typ EventType) {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, AppendTx(ctx, tx, aggregateID, typ, map[string]any{"k": "v"}))
	require.NoError(t, tx.Commit(ctx))
}

func TestStore_AppendFetchMarkPublished(t *testing.T) {
	ctx := context.Background()
	_, err := testPool.Exec(ctx, `TRUNCATE events RESTART IDENTITY`)
	require.NoError(t, err)
	s := NewStore(testPool)

	appendEvent(t, "agg1", TransferPosted)
	appendEvent(t, "agg1", ApplicationSubmitted)

	pending, err := s.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Equal(t, TransferPosted, pending[0].Type) // oldest first

	ids := []int64{pending[0].ID, pending[1].ID}
	require.NoError(t, s.MarkPublished(ctx, ids))

	after, err := s.FetchUnpublished(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, after, "published events are no longer pending")
}

func TestStore_LoadByAggregate(t *testing.T) {
	ctx := context.Background()
	_, err := testPool.Exec(ctx, `TRUNCATE events RESTART IDENTITY`)
	require.NoError(t, err)
	s := NewStore(testPool)

	appendEvent(t, "appA", ApplicationSubmitted)
	appendEvent(t, "appB", ApplicationSubmitted)
	appendEvent(t, "appA", ApplicationApproved)

	evs, err := s.LoadByAggregate(ctx, "appA")
	require.NoError(t, err)
	require.Len(t, evs, 2)
	require.Equal(t, ApplicationSubmitted, evs[0].Type)
	require.Equal(t, ApplicationApproved, evs[1].Type)
}
