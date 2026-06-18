# plata-risk — dbt analytics (v2c)

A self-contained **dbt** project modelling credit-risk metrics on **DuckDB**, so
it runs with no database server and no Docker: `dbt build` loads the seed,
builds the models, and runs the tests against a local DuckDB file.

## Models

| Model | What it computes |
| ----- | ---------------- |
| `stg_loan_status` | typed view over the raw loan-month snapshots |
| `mart_loan_month_dpd` | DPD bucket (`current`/`1-30`/`31-60`/`61-90`/`90+`) + a **latching** default flag (90+ DPD, stays defaulted) |
| `mart_vintage_cohorts` | default rate by origination vintage |
| `mart_portfolio_at_risk` | **PAR30** (share of outstanding principal >30 DPD) by month |

## Tests

Generic (`schema.yml`): `not_null`, `unique`, and `accepted_values` on the DPD
bucket. Singular (`tests/`): DPD uniqueness per loan-month, PAR30 and vintage
default-rate within `[0,1]`, and that the default flag **latches** (never
regresses).

## Run

```bash
pip install dbt-duckdb
cd analytics
dbt build --profiles-dir .      # seed -> run -> test
dbt docs generate --profiles-dir .   # optional: lineage docs
```

Seeds are synthetic; in production these models would run on the loan-month
panel derived from the ledger's transfers and the origination event stream.
