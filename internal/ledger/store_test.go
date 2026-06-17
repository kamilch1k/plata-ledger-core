package ledger

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testPort = 54329

var testPool *pgxpool.Pool

// TestMain boots a real, ephemeral Postgres (no Docker required) once for the
// whole package, so the concurrency tests exercise genuine Postgres locking.
func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(testPort).Logger(io.Discard),
	)
	if err := pg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres start:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable&pool_max_conns=20", testPort)
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

// freshStore truncates all tables so each test starts from a clean slate.
func freshStore(t *testing.T) *Store {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`TRUNCATE ledger_entries, transfers, accounts RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return New(testPool)
}

func TestTransfer_AppliesDoubleEntry(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	a, err := s.CreateAccount(ctx, "Alice", "USD", 10_000)
	require.NoError(t, err)
	b, err := s.CreateAccount(ctx, "Bob", "USD", 0)
	require.NoError(t, err)

	_, replay, err := s.Transfer(ctx, TransferParams{
		IdempotencyKey: "k1", FromAccount: a.ID, ToAccount: b.ID, AmountMinor: 3_000,
	})
	require.NoError(t, err)
	require.False(t, replay)

	gotA, _ := s.GetAccount(ctx, a.ID)
	gotB, _ := s.GetAccount(ctx, b.ID)
	require.EqualValues(t, 7_000, gotA.BalanceMinor)
	require.EqualValues(t, 3_000, gotB.BalanceMinor)

	sum, err := s.SumLedgerEntries(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 0, sum, "double-entry ledger must sum to zero")
}

// The headline test: many concurrent transfers from the same account can never
// overdraw it, and exactly the affordable number succeed.
func TestTransfer_ConcurrentDoubleSpend_NoOverdraft(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	a, _ := s.CreateAccount(ctx, "Alice", "USD", 10_000)
	b, _ := s.CreateAccount(ctx, "Bob", "USD", 0)

	const workers = 50
	const amount = 3_000 // only 3 of these fit in 10_000

	var succeeded int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.Transfer(ctx, TransferParams{
				IdempotencyKey: fmt.Sprintf("spend-%d", i),
				FromAccount:    a.ID, ToAccount: b.ID, AmountMinor: amount,
			})
			if err == nil {
				atomic.AddInt64(&succeeded, 1)
			} else {
				require.ErrorIs(t, err, ErrInsufficientFunds)
			}
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 3, succeeded)
	gotA, _ := s.GetAccount(ctx, a.ID)
	gotB, _ := s.GetAccount(ctx, b.ID)
	require.EqualValues(t, 1_000, gotA.BalanceMinor)
	require.EqualValues(t, 9_000, gotB.BalanceMinor)
	require.GreaterOrEqual(t, gotA.BalanceMinor, int64(0), "balance must never go negative")

	sum, _ := s.SumLedgerEntries(ctx)
	require.EqualValues(t, 0, sum)
}

// The same idempotency key delivered concurrently applies exactly once.
func TestTransfer_Idempotent_Concurrent(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	a, _ := s.CreateAccount(ctx, "Alice", "USD", 10_000)
	b, _ := s.CreateAccount(ctx, "Bob", "USD", 0)

	const workers = 20
	var applied, replayed int64
	ids := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tr, replay, err := s.Transfer(ctx, TransferParams{
				IdempotencyKey: "same-key", FromAccount: a.ID, ToAccount: b.ID, AmountMinor: 2_500,
			})
			require.NoError(t, err)
			ids[i] = tr.ID
			if replay {
				atomic.AddInt64(&replayed, 1)
			} else {
				atomic.AddInt64(&applied, 1)
			}
		}(i)
	}
	wg.Wait()

	require.EqualValues(t, 1, applied, "exactly one delivery applies the transfer")
	require.EqualValues(t, workers-1, replayed)
	for _, id := range ids {
		require.Equal(t, ids[0], id, "every delivery returns the same transfer")
	}

	gotA, _ := s.GetAccount(ctx, a.ID)
	require.EqualValues(t, 7_500, gotA.BalanceMinor, "balance moved exactly once")

	var count int64
	require.NoError(t, testPool.QueryRow(ctx, `SELECT COUNT(*) FROM transfers`).Scan(&count))
	require.EqualValues(t, 1, count)
}

func TestTransfer_Errors(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	usd, _ := s.CreateAccount(ctx, "USD acct", "USD", 100)
	usd2, _ := s.CreateAccount(ctx, "USD acct 2", "USD", 0)
	eur, _ := s.CreateAccount(ctx, "EUR acct", "EUR", 0)

	cases := []struct {
		name string
		p    TransferParams
		want error
	}{
		{"insufficient funds", TransferParams{IdempotencyKey: "e1", FromAccount: usd.ID, ToAccount: usd2.ID, AmountMinor: 101}, ErrInsufficientFunds},
		{"currency mismatch", TransferParams{IdempotencyKey: "e2", FromAccount: usd.ID, ToAccount: eur.ID, AmountMinor: 10}, ErrCurrencyMismatch},
		{"zero amount", TransferParams{IdempotencyKey: "e3", FromAccount: usd.ID, ToAccount: usd2.ID, AmountMinor: 0}, ErrInvalidAmount},
		{"same account", TransferParams{IdempotencyKey: "e4", FromAccount: usd.ID, ToAccount: usd.ID, AmountMinor: 10}, ErrSameAccount},
		{"missing key", TransferParams{FromAccount: usd.ID, ToAccount: usd2.ID, AmountMinor: 10}, ErrMissingIdempotencyKey},
		{"unknown account", TransferParams{IdempotencyKey: "e5", FromAccount: usd.ID, ToAccount: "acc_nope", AmountMinor: 10}, ErrAccountNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := s.Transfer(ctx, tc.p)
			require.ErrorIs(t, err, tc.want)
		})
	}

	// No failed attempt moved money.
	got, _ := s.GetAccount(ctx, usd.ID)
	require.EqualValues(t, 100, got.BalanceMinor)
}

func TestCreateAccount_Validation(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	_, err := s.CreateAccount(ctx, "x", "usd", 0) // lower-case currency
	require.ErrorIs(t, err, ErrInvalidCurrency)
	_, err = s.CreateAccount(ctx, "x", "USD", -1)
	require.ErrorIs(t, err, ErrInvalidAmount)
}
