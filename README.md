# plata-ledger-core

[![CI](https://github.com/kamilch1k/plata-ledger-core/actions/workflows/ci.yml/badge.svg)](https://github.com/kamilch1k/plata-ledger-core/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Go](https://img.shields.io/badge/go-1.26-00ADD8?logo=go&logoColor=white)

**An idempotent, concurrency-safe double-entry ledger in Go.** It does the one
thing a payments backend cannot get wrong: move money **exactly once**, **never
overdraw an account under concurrency**, and keep a **balanced double-entry**
record — proven by tests against real Postgres.

This is the core (v1) of a staged fintech platform; the roadmap (gRPC, Kafka
events, loan origination, risk analytics) is in [DESIGN.md](DESIGN.md).

## What it guarantees (and proves)

| Guarantee | Mechanism | Proof |
| --------- | --------- | ----- |
| **No overdraft under concurrency** | `SELECT ... FOR UPDATE` on account rows, locked in a deterministic order; `CHECK (balance_minor >= 0)` backstop | `TestTransfer_ConcurrentDoubleSpend_NoOverdraft` — 50 goroutines spend from one account; exactly the affordable number succeed, balance never negative |
| **Exactly-once on retries** | `Idempotency-Key` + `INSERT ... ON CONFLICT DO NOTHING`, key claimed before the balance check | `TestTransfer_Idempotent_Concurrent` — 20 concurrent deliveries of one key apply **once**, 19 are replays |
| **Balanced books** | double-entry: every transfer writes a debit and a credit | sum of all ledger entries is always `0`; balance is conserved across transfers |
| **Exact money** | `int64` minor units everywhere, never floats | — |

Money correctness is subtle, so the reasoning — including a real
**deadlock I had to design out** — is written up in [DESIGN.md](DESIGN.md).

Beyond the example-based tests above, a **property-based test** (`gopter`,
`TestProperty_LedgerInvariants`) replays dozens of *random* transfer sequences
and asserts those invariants hold for every generated case, and a **benchmark**
measures throughput: **~620 transfers/sec single-threaded** (1.6 ms/transfer)
against embedded Postgres on a laptop — reproducible with
`go test -bench=BenchmarkTransfer ./internal/ledger`.

## Run the tests (no Docker, no setup)

The suite boots its own ephemeral Postgres (`embedded-postgres`), so all you
need is Go:

```bash
git clone https://github.com/kamilch1k/plata-ledger-core
cd plata-ledger-core
go test -p 1 ./...   # ~84% coverage; -p 1 keeps the embedded-Postgres instances from racing
```

## Run the server

The server needs a Postgres. Point `DATABASE_URL` at one and run:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/ledger?sslmode=disable"
go run ./cmd/server         # HTTP on :8080, gRPC on :9090, runs migrations on start
```

The same service layer is exposed over **gRPC** (`proto/ledger.proto`):
`CreateAccount`, `GetAccount`, and `Transfer` (with the idempotency key in the
request message). Domain errors map to gRPC codes (`NotFound`,
`FailedPrecondition`, `InvalidArgument`).

### API

```bash
# open two accounts
curl -s localhost:8080/accounts -d '{"name":"Alice","currency":"USD","initial_balance_minor":10000}'
curl -s localhost:8080/accounts -d '{"name":"Bob","currency":"USD","initial_balance_minor":0}'

# transfer $30.00 — the Idempotency-Key makes retries safe
curl -s localhost:8080/transfers \
  -H 'Idempotency-Key: 7f3c-otp-1' \
  -d '{"from_account":"acc_...","to_account":"acc_...","amount_minor":3000}'

curl -s localhost:8080/accounts/acc_...            # balance
curl -s localhost:8080/accounts/acc_.../statement  # ledger entries
```

A replayed `Idempotency-Key` returns `200` with the original transfer; a new one
returns `201`. Overdraft returns `422`.

## Event-driven layer (v2)

State changes emit domain events through a **transactional outbox**: each event
is written to the `events` table in the *same* transaction as the change that
produced it, so an event is never lost or double-written relative to its state. A
**relay** drains the outbox to the bus (Kafka in production; a logging stub
locally) **at-least-once** — it publishes *before* marking, so a crash
re-publishes rather than drops.

Built on that:

- **Loan origination** (`internal/origination`) — an event-sourced application
  FSM (`submitted → kyc → scored → approved/declined → disbursed`) with a
  transparent rule-based risk score. Illegal transitions are rejected, approved
  loans disburse through the ledger idempotently, and a property test asserts the
  event-sourcing invariant: **folding the events always equals the projected state**.
- **AML monitoring** (`internal/aml`) — a streaming consumer applying
  amount-threshold and velocity rules, **deduping by event id** so replays from an
  at-least-once bus never double-count or double-alert.

The Kafka producer/consumer (`internal/kafka`, `segmentio/kafka-go`) sits behind
the `Publisher` / handler interfaces, so every correctness property is tested
against embedded Postgres with in-memory fakes — no Docker, no broker required.

## Layout

```
cmd/server           entrypoint: migrate, serve HTTP + gRPC, run the outbox relay
internal/ledger      the core: accounts, transfers, double-entry, concurrency
internal/api         HTTP layer over a LedgerService interface (DB-free tests)
internal/grpcapi     gRPC layer over the same interface
internal/ledgerpb    generated protobuf/gRPC code
internal/events      transactional outbox + at-least-once relay
internal/origination event-sourced loan-application FSM + risk scoring
internal/aml         streaming AML consumer (dedupe + rules)
internal/kafka       Kafka producer/consumer adapter (live-verified, not in CI)
proto/               ledger.proto service definition
```

## Tech

Go 1.26 · Postgres (`pgx/v5`) · gRPC + Protobuf · Kafka (`segmentio/kafka-go`) ·
transactional outbox · stdlib `net/http` routing · `embedded-postgres` for
hermetic tests · `gopter` property tests · GitHub Actions (`go vet`, `gofmt`,
`-race`, coverage gate).

## License

[MIT](LICENSE) © Kamil Gilfanov
