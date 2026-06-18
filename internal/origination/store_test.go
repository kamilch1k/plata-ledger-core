package origination

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"

	"github.com/kamilch1k/ledgerline/internal/events"
	"github.com/kamilch1k/ledgerline/internal/ledger"
)

const testPort = 54330

var (
	testPool    *pgxpool.Pool
	ledgerStore *ledger.Store
)

func TestMain(m *testing.M) {
	pg := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(testPort).Logger(io.Discard),
	)
	if err := pg.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
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
	ledgerStore = ledger.New(pool)
	for _, mig := range []func(context.Context) error{
		ledgerStore.Migrate,
		func(c context.Context) error { return events.Migrate(c, pool) },
		New(pool, ledgerStore).Migrate,
	} {
		if err := mig(ctx); err != nil {
			pool.Close()
			_ = pg.Stop()
			fmt.Fprintln(os.Stderr, "migrate:", err)
			os.Exit(1)
		}
	}
	testPool = pool

	code := m.Run()
	pool.Close()
	_ = pg.Stop()
	os.Exit(code)
}

func freshStore(t *testing.T) *Store {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`TRUNCATE applications, ledger_entries, transfers, accounts, events RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return New(testPool, ledgerStore)
}

func TestLifecycle_HappyPath_Disburses(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)

	funding, err := ledgerStore.CreateAccount(ctx, "Funding", "USD", 10_000_000)
	require.NoError(t, err)
	borrower, err := ledgerStore.CreateAccount(ctx, "Borrower", "USD", 0)
	require.NoError(t, err)

	app, err := s.CreateApplication(ctx, CreateApplicationInput{
		BorrowerAccount: borrower.ID, AmountMinor: 120_000, TermMonths: 12, MonthlyIncomeMinor: 500_000,
	})
	require.NoError(t, err)
	require.Equal(t, Submitted, app.State)

	_, err = s.RunKYC(ctx, app.ID, true)
	require.NoError(t, err)
	scored, err := s.Score(ctx, app.ID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, scored.RiskScore, ApprovalThreshold)
	decided, err := s.Decide(ctx, app.ID)
	require.NoError(t, err)
	require.Equal(t, Approved, decided.State)

	disbursed, err := s.Disburse(ctx, app.ID, funding.ID)
	require.NoError(t, err)
	require.Equal(t, Disbursed, disbursed.State)
	require.NotEmpty(t, disbursed.DisbursedTransferID)

	gotBorrower, _ := ledgerStore.GetAccount(ctx, borrower.ID)
	require.EqualValues(t, 120_000, gotBorrower.BalanceMinor)

	// The event log records the full lifecycle in order.
	evs, err := events.NewStore(testPool).LoadByAggregate(ctx, app.ID)
	require.NoError(t, err)
	types := make([]events.EventType, len(evs))
	for i, e := range evs {
		types[i] = e.Type
	}
	require.Equal(t, []events.EventType{
		events.ApplicationSubmitted,
		events.ApplicationKYCPassed,
		events.ApplicationScored,
		events.ApplicationApproved,
		events.ApplicationDisbursed,
	}, types)
}

func TestDecide_LowScore_Declines(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	borrower, _ := ledgerStore.CreateAccount(ctx, "B", "USD", 0)

	app, _ := s.CreateApplication(ctx, CreateApplicationInput{
		BorrowerAccount: borrower.ID, AmountMinor: 900_000, TermMonths: 3, MonthlyIncomeMinor: 400_000,
	})
	_, _ = s.RunKYC(ctx, app.ID, true)
	_, _ = s.Score(ctx, app.ID)
	decided, err := s.Decide(ctx, app.ID)
	require.NoError(t, err)
	require.Equal(t, Declined, decided.State)

	// A declined application cannot be disbursed.
	_, err = s.Disburse(ctx, app.ID, "acc_whatever")
	require.ErrorIs(t, err, ErrIllegalTransition)
}

func TestIllegalTransitions(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	borrower, _ := ledgerStore.CreateAccount(ctx, "B", "USD", 0)
	app, _ := s.CreateApplication(ctx, CreateApplicationInput{
		BorrowerAccount: borrower.ID, AmountMinor: 1000, TermMonths: 12, MonthlyIncomeMinor: 500_000,
	})

	// Cannot score before KYC.
	_, err := s.Score(ctx, app.ID)
	require.ErrorIs(t, err, ErrIllegalTransition)

	// KYC twice is illegal the second time.
	_, err = s.RunKYC(ctx, app.ID, true)
	require.NoError(t, err)
	_, err = s.RunKYC(ctx, app.ID, true)
	require.ErrorIs(t, err, ErrIllegalTransition)

	// Unknown application.
	_, err = s.Score(ctx, "app_missing")
	require.ErrorIs(t, err, ErrApplicationNotFound)
}

func TestDisburse_Idempotent(t *testing.T) {
	ctx := context.Background()
	s := freshStore(t)
	funding, _ := ledgerStore.CreateAccount(ctx, "Funding", "USD", 10_000_000)
	borrower, _ := ledgerStore.CreateAccount(ctx, "Borrower", "USD", 0)
	app, _ := s.CreateApplication(ctx, CreateApplicationInput{
		BorrowerAccount: borrower.ID, AmountMinor: 50_000, TermMonths: 24, MonthlyIncomeMinor: 800_000,
	})
	_, _ = s.RunKYC(ctx, app.ID, true)
	_, _ = s.Score(ctx, app.ID)
	_, _ = s.Decide(ctx, app.ID)

	_, err := s.Disburse(ctx, app.ID, funding.ID)
	require.NoError(t, err)
	again, err := s.Disburse(ctx, app.ID, funding.ID) // no-op
	require.NoError(t, err)
	require.Equal(t, Disbursed, again.State)

	gotBorrower, _ := ledgerStore.GetAccount(ctx, borrower.ID)
	require.EqualValues(t, 50_000, gotBorrower.BalanceMinor, "disbursed exactly once")
}

// Property: folding the event log always equals the stored projection state.
func TestProperty_FoldEqualsProjection(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 40
	properties := gopter.NewProperties(params)

	properties.Property("fold(events) == projection.state for any lifecycle",
		prop.ForAll(func(seed int64) bool {
			s := freshStore(t)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(seed))

			funding, err := ledgerStore.CreateAccount(ctx, "F", "USD", 1_000_000_000)
			if err != nil {
				return false
			}
			borrower, err := ledgerStore.CreateAccount(ctx, "B", "USD", 0)
			if err != nil {
				return false
			}
			app, err := s.CreateApplication(ctx, CreateApplicationInput{
				BorrowerAccount:    borrower.ID,
				AmountMinor:        int64(rng.Intn(2_000_000) + 1_000),
				TermMonths:         rng.Intn(60) + 1,
				MonthlyIncomeMinor: int64(rng.Intn(1_000_000)),
			})
			if err != nil {
				return false
			}

			kycPass := rng.Intn(2) == 0
			if _, err := s.RunKYC(ctx, app.ID, kycPass); err != nil {
				return false
			}
			if kycPass {
				if _, err := s.Score(ctx, app.ID); err != nil {
					return false
				}
				decided, err := s.Decide(ctx, app.ID)
				if err != nil {
					return false
				}
				if decided.State == Approved {
					if _, err := s.Disburse(ctx, app.ID, funding.ID); err != nil {
						return false
					}
				}
			}

			folded, err := s.FoldState(ctx, app.ID)
			if err != nil {
				return false
			}
			got, err := s.Get(ctx, app.ID)
			if err != nil {
				return false
			}
			return folded == got.State
		}, gen.Int64()))

	properties.TestingRun(t)
}
