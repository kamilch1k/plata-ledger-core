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

## Event sourcing and the outbox (v2)

**Transactional outbox.** A state change and the event announcing it must be
all-or-nothing. Writing to a separate broker inside the DB transaction is
impossible (the broker isn't transactional with Postgres), and publishing after
commit can be lost on a crash. So the event is written to the `events` table in
the *same* transaction (`events.AppendTx`), and a separate relay publishes it
afterwards. The DB is the single source of truth; the bus eventually catches up.

**At-least-once, never at-most-once.** The relay publishes *then* marks
published. A crash between the two re-publishes on restart — never drops. The
inverse (mark then publish) would silently lose events on a crash. Because the
bus is therefore at-least-once, every consumer must dedupe; the AML consumer
claims each event id in a `processed_events` table (`INSERT ... ON CONFLICT DO
NOTHING`) before acting, so replays are no-ops. Both properties are tested with
fakes (`relay_test.go`) and against Postgres (`aml` replay-determinism test).

**Event-sourced FSM.** The loan-origination projection is derived from its event
log; a property test folds the events and asserts the result equals the stored
projection for any random lifecycle. Illegal transitions are rejected by a single
`transitions` table, and disbursement reuses the ledger's idempotency so the
two-step "move money, then record it" is safe to retry.

## Roadmap

- **v1.1 (shipped)** — gRPC/Protobuf surface, a `gopter` property test, and a
  throughput benchmark.
- **v2 (shipped, except analytics)** — event-sourced loan-origination FSM,
  transactional outbox + at-least-once relay, streaming AML consumer with
  event-id dedup, and a Kafka producer/consumer adapter behind the `Publisher`
  interface.
- **v2c (shipped)** — a dbt + DuckDB analytics layer (`analytics/`) modelling DPD
  buckets, a latching default flag, vintage-cohort default rates, and PAR30, with
  generic + singular reconciliation tests; runs serverless via `dbt build`. The
  origination FSM is also now exposed over HTTP. *(Still planned: an Allure report
  on GitHub Pages.)*
