# Design notes

A ledger's whole job is to be correct about money under concurrency and
retries. These are the decisions that make `plata-ledger-core` correct, and why.

## Money is int64 minor units, never floats

Every amount is an integer count of the currency's smallest unit (e.g. cents).
Floating point can't represent `0.10` exactly, and rounding errors in money are
unacceptable. `BIGINT` columns and `int64` in Go carry values far larger than
any realistic balance, and all arithmetic is exact.

## Double-entry, and the zero-sum invariant

Every transfer writes two `ledger_entries`: a debit (negative) on the sender and
a credit (positive) on the receiver. Therefore the sum of all entries is always
zero, and the sum of balances is conserved across any number of transfers. Both
are asserted by tests (`SumLedgerEntries == 0`, balance conservation), and
`SumLedgerEntries` is also a cheap production reconciliation check.

## Idempotency: `Idempotency-Key` + `ON CONFLICT`

Clients and networks retry. A transfer carries a caller-supplied idempotency
key, stored `UNIQUE`. The transfer is claimed with
`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING ...`:

- **First delivery** inserts the row and applies the movement.
- **A replay** gets no row back, so it returns the already-committed transfer
  **without touching balances**.

Crucially, the key is claimed **before** the balance check. If we checked the
balance first, a replay of an already-applied transfer could spuriously fail
with "insufficient funds" because the balance had legitimately moved on. Claim
first → a replay is always recognised as a replay.

## Concurrency: `SELECT ... FOR UPDATE`, and the deadlock I had to design out

Concurrent transfers from the same account must not overspend it. Each transfer
locks **both** account rows with `SELECT ... FOR UPDATE` before reading the
balance, so the check-and-debit is a race-free critical section. The first run
of the concurrent test (50 goroutines spending from one account) immediately
surfaced a real bug:

```
ERROR: deadlock detected (SQLSTATE 40P01)
```

The cause: inserting a `transfers` row takes a `FOR KEY SHARE` lock on the
referenced account rows (the foreign keys). My first version inserted the
transfer **before** the `FOR UPDATE`, so each transaction held a *share* lock and
then tried to *upgrade* to `FOR UPDATE` — two transactions each waiting for the
other to release the share lock is a classic lock-upgrade deadlock.

Two fixes, both applied:

1. **Take the strongest lock first.** Lock the account rows `FOR UPDATE` *before*
   inserting the transfer, so the later FK share lock needs no upgrade.
2. **Lock in a deterministic order** (account ids sorted) so transfers in
   opposite directions can never form a lock cycle.

After this, the 50-goroutine test passes deterministically: exactly the
affordable number of transfers succeed, the balance never goes negative, and the
ledger still sums to zero.

## Defence in depth: a `CHECK` constraint

`balance_minor BIGINT NOT NULL CHECK (balance_minor >= 0)` means that even if the
application logic were wrong, the database itself would reject an overdraft. The
application check returns a clean `ErrInsufficientFunds`; the constraint is the
backstop.

## Isolation level

The default `READ COMMITTED` plus explicit `FOR UPDATE` row locks is sufficient
here: the row locks serialise the only operation that mutates a balance.
`SERIALIZABLE` would also work but adds serialization-failure retries for no
benefit given the explicit locking, so it isn't used.

## Why embedded Postgres for tests (not testcontainers/SQLite)

Tests boot a real, ephemeral Postgres via `fergusstrange/embedded-postgres` — so
they exercise genuine Postgres locking (`FOR UPDATE`, FK locks, the deadlock
detector) **without requiring Docker**. `git clone && go test ./...` works with
nothing but the Go toolchain. SQLite was rejected because its concurrency model
differs from Postgres and wouldn't prove the property that matters.

## Testability seam

The HTTP layer depends on a `LedgerService` interface, so handler behaviour
(status codes, `Idempotency-Key` handling, replay → 200 vs new → 201) is tested
with an in-memory fake and no database, while the store's concurrency guarantees
are tested against real Postgres.

## Roadmap (intentionally out of scope for v1)

v1 is the correct, tested core.

- **v1.1 (shipped)** — a gRPC/Protobuf surface (`proto/ledger.proto`) over the
  same service layer; a property-based test (`gopter`) asserting the ledger
  invariants on random transfer sequences; and a throughput benchmark
  (`go test -bench`). *Still planned: an Allure report published to GitHub
  Pages.*
- **v2** — a loan-origination state machine with an append-only event audit
  trail; Kafka event streaming via a transactional outbox with consumer-side
  dedup; a streaming AML/fraud consumer; and a dbt + SQL analytics layer modelling
  DPD buckets and default flags.
