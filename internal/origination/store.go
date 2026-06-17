package origination

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kamilch1k/plata-ledger-core/internal/events"
	"github.com/kamilch1k/plata-ledger-core/internal/ledger"
)

// Disburser moves the loan principal to the borrower. Satisfied by *ledger.Store.
type Disburser interface {
	Transfer(ctx context.Context, p ledger.TransferParams) (ledger.Transfer, bool, error)
}

type Store struct {
	pool      *pgxpool.Pool
	events    *events.Store
	disburser Disburser
}

func New(pool *pgxpool.Pool, d Disburser) *Store {
	return &Store{pool: pool, events: events.NewStore(pool), disburser: d}
}

const schema = `
CREATE TABLE IF NOT EXISTS applications (
	id                    TEXT PRIMARY KEY,
	borrower_account      TEXT   NOT NULL,
	amount_minor          BIGINT NOT NULL CHECK (amount_minor > 0),
	term_months           INT    NOT NULL CHECK (term_months > 0),
	monthly_income_minor  BIGINT NOT NULL CHECK (monthly_income_minor >= 0),
	state                 TEXT   NOT NULL,
	risk_score            INT    NOT NULL DEFAULT 0,
	disbursed_transfer_id TEXT,
	created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

type CreateApplicationInput struct {
	BorrowerAccount    string
	AmountMinor        int64
	TermMonths         int
	MonthlyIncomeMinor int64
}

func (s *Store) CreateApplication(ctx context.Context, in CreateApplicationInput) (Application, error) {
	if in.AmountMinor <= 0 || in.TermMonths <= 0 || in.MonthlyIncomeMinor < 0 || in.BorrowerAccount == "" {
		return Application{}, ErrInvalidApplication
	}
	app := Application{
		ID:                 "app_" + uuid.NewString(),
		BorrowerAccount:    in.BorrowerAccount,
		AmountMinor:        in.AmountMinor,
		TermMonths:         in.TermMonths,
		MonthlyIncomeMinor: in.MonthlyIncomeMinor,
		State:              Submitted,
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Application{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO applications (id, borrower_account, amount_minor, term_months, monthly_income_minor, state)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		app.ID, app.BorrowerAccount, app.AmountMinor, app.TermMonths, app.MonthlyIncomeMinor, string(Submitted)); err != nil {
		return Application{}, err
	}
	if err := events.AppendTx(ctx, tx, app.ID, events.ApplicationSubmitted, map[string]any{
		"amount_minor": app.AmountMinor,
		"term_months":  app.TermMonths,
	}); err != nil {
		return Application{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Application{}, err
	}
	return s.Get(ctx, app.ID)
}

// advance loads and locks the application, lets fn compute the target state (and
// apply any extra column updates), validates the transition, updates the state,
// and appends the event — all in one transaction.
func (s *Store) advance(
	ctx context.Context,
	appID string,
	fn func(ctx context.Context, tx pgx.Tx, app *Application) (State, events.EventType, any, error),
) (Application, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Application{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	app, err := loadForUpdate(ctx, tx, appID)
	if err != nil {
		return Application{}, err
	}
	to, et, payload, err := fn(ctx, tx, &app)
	if err != nil {
		return Application{}, err
	}
	if !CanTransition(app.State, to) {
		return Application{}, fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, app.State, to)
	}
	if _, err := tx.Exec(ctx,
		`UPDATE applications SET state = $1, updated_at = now() WHERE id = $2`, string(to), appID); err != nil {
		return Application{}, err
	}
	if err := events.AppendTx(ctx, tx, appID, et, payload); err != nil {
		return Application{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Application{}, err
	}
	return s.Get(ctx, appID)
}

// RunKYC moves a submitted application to kyc_passed or kyc_failed.
func (s *Store) RunKYC(ctx context.Context, appID string, pass bool) (Application, error) {
	return s.advance(ctx, appID, func(_ context.Context, _ pgx.Tx, _ *Application) (State, events.EventType, any, error) {
		if pass {
			return KYCPassed, events.ApplicationKYCPassed, map[string]any{"result": "passed"}, nil
		}
		return KYCFailed, events.ApplicationKYCFailed, map[string]any{"result": "failed"}, nil
	})
}

// Score computes and stores the risk score, moving kyc_passed -> scored.
func (s *Store) Score(ctx context.Context, appID string) (Application, error) {
	return s.advance(ctx, appID, func(ctx context.Context, tx pgx.Tx, app *Application) (State, events.EventType, any, error) {
		score := RiskScore(*app)
		if _, err := tx.Exec(ctx, `UPDATE applications SET risk_score = $1 WHERE id = $2`, score, app.ID); err != nil {
			return "", "", nil, err
		}
		return Scored, events.ApplicationScored, map[string]any{"risk_score": score}, nil
	})
}

// Decide approves or declines a scored application against ApprovalThreshold.
func (s *Store) Decide(ctx context.Context, appID string) (Application, error) {
	return s.advance(ctx, appID, func(_ context.Context, _ pgx.Tx, app *Application) (State, events.EventType, any, error) {
		if app.RiskScore >= ApprovalThreshold {
			return Approved, events.ApplicationApproved, map[string]any{"risk_score": app.RiskScore}, nil
		}
		return Declined, events.ApplicationDeclined, map[string]any{"risk_score": app.RiskScore}, nil
	})
}

// Disburse moves the principal to the borrower (idempotently, via the ledger),
// then records the disbursement. Safe to call twice: the ledger dedupes the
// money movement and a second call on an already-disbursed application is a no-op.
func (s *Store) Disburse(ctx context.Context, appID, fundingAccount string) (Application, error) {
	app, err := s.Get(ctx, appID)
	if err != nil {
		return Application{}, err
	}
	if app.State == Disbursed {
		return app, nil
	}
	if app.State != Approved {
		return Application{}, fmt.Errorf("%w: cannot disburse from %s", ErrIllegalTransition, app.State)
	}

	tr, _, err := s.disburser.Transfer(ctx, ledger.TransferParams{
		IdempotencyKey: "disburse-" + appID,
		FromAccount:    fundingAccount,
		ToAccount:      app.BorrowerAccount,
		AmountMinor:    app.AmountMinor,
	})
	if err != nil {
		return Application{}, err
	}

	return s.advance(ctx, appID, func(ctx context.Context, tx pgx.Tx, _ *Application) (State, events.EventType, any, error) {
		if _, err := tx.Exec(ctx, `UPDATE applications SET disbursed_transfer_id = $1 WHERE id = $2`, tr.ID, appID); err != nil {
			return "", "", nil, err
		}
		return Disbursed, events.ApplicationDisbursed, map[string]any{"transfer_id": tr.ID, "amount_minor": app.AmountMinor}, nil
	})
}

func (s *Store) Get(ctx context.Context, appID string) (Application, error) {
	return scanApp(s.pool.QueryRow(ctx, selectApp+` WHERE id = $1`, appID))
}

// FoldState rebuilds the state purely from the event log — the event-sourcing
// invariant: folding the events must equal the stored projection.
func (s *Store) FoldState(ctx context.Context, appID string) (State, error) {
	evs, err := s.events.LoadByAggregate(ctx, appID)
	if err != nil {
		return "", err
	}
	var st State
	for _, e := range evs {
		st = applyEvent(st, e.Type)
	}
	return st, nil
}

func applyEvent(st State, t events.EventType) State {
	switch t {
	case events.ApplicationSubmitted:
		return Submitted
	case events.ApplicationKYCPassed:
		return KYCPassed
	case events.ApplicationKYCFailed:
		return KYCFailed
	case events.ApplicationScored:
		return Scored
	case events.ApplicationApproved:
		return Approved
	case events.ApplicationDeclined:
		return Declined
	case events.ApplicationDisbursed:
		return Disbursed
	default:
		return st
	}
}

const selectApp = `SELECT id, borrower_account, amount_minor, term_months, monthly_income_minor,
	state, risk_score, COALESCE(disbursed_transfer_id, ''), created_at, updated_at FROM applications`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanApp(row rowScanner) (Application, error) {
	var a Application
	err := row.Scan(&a.ID, &a.BorrowerAccount, &a.AmountMinor, &a.TermMonths, &a.MonthlyIncomeMinor,
		&a.State, &a.RiskScore, &a.DisbursedTransferID, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Application{}, ErrApplicationNotFound
	}
	return a, err
}

func loadForUpdate(ctx context.Context, tx pgx.Tx, appID string) (Application, error) {
	return scanApp(tx.QueryRow(ctx, selectApp+` WHERE id = $1 FOR UPDATE`, appID))
}
