-- Default must latch: once a loan is in default, it stays in default in every
-- later month. This finds any month where the flag regressed to false despite
-- an earlier default.
with f as (
    select
        loan_id,
        as_of_month,
        is_default,
        max(case when is_default then 1 else 0 end) over (
            partition by loan_id
            order by as_of_month
            rows between unbounded preceding and current row
        ) as ever_default
    from {{ ref('mart_loan_month_dpd') }}
)
select loan_id, as_of_month
from f
where ever_default = 1 and is_default = false
