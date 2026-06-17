package ledger

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kamilch1k/plata-ledger-core/internal/events"
)

// Account is a balance held in a single currency. The balance is stored in
// minor units and guarded by a CHECK (balance_minor >= 0) constraint as a
// backstop against overdraft, in addition to the application-level check.
type Account struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Currency     string    `json:"currency"`
	BalanceMinor int64     `json:"balance_minor"`
	CreatedAt    time.Time `json:"created_at"`
}

// Transfer is a posted money movement between two accounts.
type Transfer struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	FromAccount    string    `json:"from_account"`
	ToAccount      string    `json:"to_account"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	CreatedAt      time.Time `json:"created_at"`
}

// Entry is one side of a double-entry posting. Debits are negative, credits
// positive; the sum of all entries for a transfer (and across the whole
// ledger) is always zero.
type Entry struct {
	ID          int64     `json:"id"`
	TransferID  string    `json:"transfer_id"`
	AccountID   string    `json:"account_id"`
	AmountMinor int64     `json:"amount_minor"`
	CreatedAt   time.Time `json:"created_at"`
}

// TransferParams is the input to Store.Transfer.
type TransferParams struct {
	IdempotencyKey string
	FromAccount    string
	ToAccount      string
	AmountMinor    int64
}

// Store is the data access layer over Postgres.
type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

const schema = `
CREATE TABLE IF NOT EXISTS accounts (
	id            TEXT PRIMARY KEY,
	name          TEXT NOT NULL,
	currency      TEXT NOT NULL,
	balance_minor BIGINT NOT NULL DEFAULT 0 CHECK (balance_minor >= 0),
	created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transfers (
	id              TEXT PRIMARY KEY,
	idempotency_key TEXT NOT NULL UNIQUE,
	from_account    TEXT NOT NULL REFERENCES accounts(id),
	to_account      TEXT NOT NULL REFERENCES accounts(id),
	amount_minor    BIGINT NOT NULL CHECK (amount_minor > 0),
	currency        TEXT NOT NULL,
	created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
	id           BIGSERIAL PRIMARY KEY,
	transfer_id  TEXT NOT NULL REFERENCES transfers(id),
	account_id   TEXT NOT NULL REFERENCES accounts(id),
	amount_minor BIGINT NOT NULL,
	created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entries_account ON ledger_entries (account_id, id);
`

// Migrate creates the schema if it does not exist.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

// CreateAccount opens a new account with an optional starting balance.
func (s *Store) CreateAccount(ctx context.Context, name, currency string, initialMinor int64) (Account, error) {
	if !validCurrency(currency) {
		return Account{}, ErrInvalidCurrency
	}
	if initialMinor < 0 {
		return Account{}, ErrInvalidAmount
	}
	a := Account{
		ID:           "acc_" + uuid.NewString(),
		Name:         name,
		Currency:     currency,
		BalanceMinor: initialMinor,
	}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO accounts (id, name, currency, balance_minor)
		 VALUES ($1, $2, $3, $4) RETURNING created_at`,
		a.ID, a.Name, a.Currency, a.BalanceMinor).Scan(&a.CreatedAt)
	if err != nil {
		return Account{}, err
	}
	return a, nil
}

// GetAccount returns the current state of an account.
func (s *Store) GetAccount(ctx context.Context, id string) (Account, error) {
	var a Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, currency, balance_minor, created_at FROM accounts WHERE id = $1`, id).
		Scan(&a.ID, &a.Name, &a.Currency, &a.BalanceMinor, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	return a, err
}

// Transfer moves money atomically and idempotently. The returned bool is true
// when the call was an idempotent replay (a transfer with the same key already
// existed) — in that case no balances are touched.
//
// Ordering is deliberate to be both deadlock-free and correct under concurrency:
//  1. Lock both account rows FOR UPDATE in a deterministic (sorted) order.
//     Taking the strongest lock first means the weaker FK row-share locks that
//     the later transfers INSERT acquires need no upgrade — which is what would
//     otherwise deadlock concurrent transfers on the same accounts.
//  2. Claim the idempotency key BEFORE the balance check, so a replay of an
//     already-applied transfer is recognised even if the balance has since
//     changed.
//  3. Enforce no overdraft, then post the double entry.
func (s *Store) Transfer(ctx context.Context, p TransferParams) (Transfer, bool, error) {
	if p.IdempotencyKey == "" {
		return Transfer{}, false, ErrMissingIdempotencyKey
	}
	if p.AmountMinor <= 0 {
		return Transfer{}, false, ErrInvalidAmount
	}
	if p.FromAccount == p.ToAccount {
		return Transfer{}, false, ErrSameAccount
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Transfer{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// 1. Lock both account rows FOR UPDATE in sorted order.
	ids := []string{p.FromAccount, p.ToAccount}
	sort.Strings(ids)
	type acct struct {
		currency string
		balance  int64
	}
	locked := make(map[string]acct, 2)
	rows, err := tx.Query(ctx,
		`SELECT id, currency, balance_minor FROM accounts WHERE id = ANY($1) ORDER BY id FOR UPDATE`, ids)
	if err != nil {
		return Transfer{}, false, err
	}
	for rows.Next() {
		var id string
		var a acct
		if err := rows.Scan(&id, &a.currency, &a.balance); err != nil {
			rows.Close()
			return Transfer{}, false, err
		}
		locked[id] = a
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return Transfer{}, false, err
	}

	fromAcct, ok := locked[p.FromAccount]
	if !ok {
		return Transfer{}, false, ErrAccountNotFound
	}
	toAcct, ok := locked[p.ToAccount]
	if !ok {
		return Transfer{}, false, ErrAccountNotFound
	}
	if fromAcct.currency != toAcct.currency {
		return Transfer{}, false, ErrCurrencyMismatch
	}

	// 2. Claim the idempotency key before checking the balance.
	t := Transfer{
		ID:             "txn_" + uuid.NewString(),
		IdempotencyKey: p.IdempotencyKey,
		FromAccount:    p.FromAccount,
		ToAccount:      p.ToAccount,
		AmountMinor:    p.AmountMinor,
		Currency:       fromAcct.currency,
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO transfers (id, idempotency_key, from_account, to_account, amount_minor, currency)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (idempotency_key) DO NOTHING
		 RETURNING created_at`,
		t.ID, t.IdempotencyKey, t.FromAccount, t.ToAccount, t.AmountMinor, t.Currency).Scan(&t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// Duplicate key: the original transfer has committed. Return it unchanged.
		_ = tx.Rollback(ctx)
		existing, getErr := s.transferByKey(ctx, p.IdempotencyKey)
		return existing, true, getErr
	}
	if err != nil {
		return Transfer{}, false, err
	}

	// 3. Enforce no overdraft.
	if fromAcct.balance < p.AmountMinor {
		return Transfer{}, false, ErrInsufficientFunds
	}

	// 4. Apply the double-entry movement.
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_minor = balance_minor - $1 WHERE id = $2`,
		p.AmountMinor, p.FromAccount); err != nil {
		return Transfer{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_minor = balance_minor + $1 WHERE id = $2`,
		p.AmountMinor, p.ToAccount); err != nil {
		return Transfer{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ledger_entries (transfer_id, account_id, amount_minor)
		 VALUES ($1, $2, $3), ($1, $4, $5)`,
		t.ID, p.FromAccount, -p.AmountMinor, p.ToAccount, p.AmountMinor); err != nil {
		return Transfer{}, false, err
	}

	// Transactional outbox: the event commits atomically with the movement.
	if err := events.AppendTx(ctx, tx, t.ID, events.TransferPosted, map[string]any{
		"transfer_id":  t.ID,
		"from_account": p.FromAccount,
		"to_account":   p.ToAccount,
		"amount_minor": p.AmountMinor,
		"currency":     t.Currency,
	}); err != nil {
		return Transfer{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Transfer{}, false, err
	}
	return t, false, nil
}

func (s *Store) transferByKey(ctx context.Context, key string) (Transfer, error) {
	var t Transfer
	err := s.pool.QueryRow(ctx,
		`SELECT id, idempotency_key, from_account, to_account, amount_minor, currency, created_at
		 FROM transfers WHERE idempotency_key = $1`, key).
		Scan(&t.ID, &t.IdempotencyKey, &t.FromAccount, &t.ToAccount, &t.AmountMinor, &t.Currency, &t.CreatedAt)
	return t, err
}

// Statement returns an account's most recent ledger entries, newest first.
func (s *Store) Statement(ctx context.Context, accountID string, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, transfer_id, account_id, amount_minor, created_at
		 FROM ledger_entries WHERE account_id = $1 ORDER BY id DESC LIMIT $2`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.TransferID, &e.AccountID, &e.AmountMinor, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// SumLedgerEntries returns the sum of all entry amounts; it must always be zero
// in a correct double-entry ledger (used by reconciliation and tests).
func (s *Store) SumLedgerEntries(ctx context.Context) (int64, error) {
	var sum int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(amount_minor), 0) FROM ledger_entries`).Scan(&sum)
	return sum, err
}

// SumBalances returns the total of all account balances.
func (s *Store) SumBalances(ctx context.Context) (int64, error) {
	var sum int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(balance_minor), 0) FROM accounts`).Scan(&sum)
	return sum, err
}
